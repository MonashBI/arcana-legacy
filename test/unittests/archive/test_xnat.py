import os.path
import shutil
import tempfile
import logging
from unittest import TestCase
import xnat
from nianalysis.testing import BaseTestCase, test_data_dir
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from nianalysis.archive.xnat import XNATArchive, download_all_datasets
from nianalysis.data_formats import (
    nifti_gz_format, dicom_format)
from nianalysis.dataset import Dataset, DatasetSpec
from nianalysis.utils import split_extension
from nianalysis.data_formats import data_formats_by_ext
from nianalysis.utils import INPUT_SUFFIX, OUTPUT_SUFFIX
from nianalysis.exceptions import NiAnalysisError
import sys
# Import TestExistingPrereqs study to test it on XNAT
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'study'))
from test_study import TestExistingPrereqs  # @UnresolvedImport @IgnorePep8
sys.path.pop(0)

logger = logging.getLogger('NiAnalysis')


def dummy_pipeline():
    pass


class TestXnatArchive(BaseTestCase):

    PROJECT = 'TEST002'
    SUBJECT = 'TEST002_001'
    SESSION = 'MR01'
    STUDY_NAME = 'astudy'
    SUMMARY_STUDY_NAME = 'asummary'

    @property
    def session_id(self):
        return '_'.join((self.SUBJECT, self.SESSION))

    def setUp(self):
        self.reset_dirs()
        shutil.rmtree(self.archive_cache_dir, ignore_errors=True)
        os.makedirs(self.archive_cache_dir)
        self._delete_test_subjects()
        download_all_datasets(
            self.cache_dir, self.SERVER,
            '{}_{}'.format(self.XNAT_TEST_PROJECT, self.name),
            overwrite=False)
        with self._connect() as mbi_xnat:
            project = mbi_xnat.projects[self.PROJECT]
            subject = mbi_xnat.classes.SubjectData(
                label=self.SUBJECT,
                parent=project)
            session = mbi_xnat.classes.MrSessionData(
                label=self.session_id,
                parent=subject)
            for fname in os.listdir(self.cache_dir):
                name, ext = split_extension(fname)
                dataset = mbi_xnat.classes.MrScanData(type=name,
                                                      parent=session)
                resource = dataset.create_resource(
                    data_formats_by_ext[ext].name.upper())
                resource.upload(fname, fname)

    def tearDown(self):
        # Clean up working dirs
        shutil.rmtree(self.archive_cache_dir, ignore_errors=True)
        # Clean up session created for unit-test
        self._delete_test_subjects()

    @property
    def archive_cache_dir(self):
        return self.cache_dir + '.archive'

    def _delete_test_subjects(self):
        with self._connect() as mbi_xnat:
            project = mbi_xnat.projects[self.PROJECT]
            if self.SUBJECT in project.subjects:
                project.subjects[self.SUBJECT].delete()
            project_summary_name = (self.PROJECT + '_' +
                                    XNATArchive.SUMMARY_NAME)
            if project_summary_name in project.subjects:
                project.subjects[project_summary_name].delete()

    def _connect(self):
        return xnat.connect(self.SERVER)

    def test_archive_roundtrip(self):

        # Create working dirs
        # Create DarisSource node
        archive = XNATArchive(
            server=self.SERVER, cache_dir=self.archive_cache_dir)
        source_files = [Dataset('source1', nifti_gz_format),
                        Dataset('source2', nifti_gz_format),
                        Dataset('source3', nifti_gz_format),
                        Dataset('source4', nifti_gz_format)]
        # Sink datasets need to be considered to be processed so we set their
        # 'pipeline' attribute to be not None. May need to update this if
        # checks on valid pipelines are included in Dataset __init__ method
        sink_files = [DatasetSpec('sink1', nifti_gz_format,
                                  pipeline=dummy_pipeline),
                      DatasetSpec('sink3', nifti_gz_format,
                                  pipeline=dummy_pipeline),
                      DatasetSpec('sink4', nifti_gz_format,
                                  pipeline=dummy_pipeline)]
        inputnode = pe.Node(IdentityInterface(['subject_id', 'visit_id']),
                            'inputnode')
        inputnode.inputs.subject_id = str(self.SUBJECT)
        inputnode.inputs.visit_id = str(self.SESSION)
        source = archive.source(self.PROJECT, source_files,
                                study_name=self.STUDY_NAME)
        sink = archive.sink(self.PROJECT, sink_files,
                                study_name=self.STUDY_NAME)
        sink.inputs.name = 'archive-roundtrip-unittest'
        sink.inputs.description = (
            "A test session created by archive roundtrip unittest")
        # Create workflow connecting them together
        workflow = pe.Workflow('source-sink-unit-test',
                               base_dir=self.work_dir)
        workflow.add_nodes((source, sink))
        workflow.connect(inputnode, 'subject_id', source, 'subject_id')
        workflow.connect(inputnode, 'visit_id', source, 'visit_id')
        workflow.connect(inputnode, 'subject_id', sink, 'subject_id')
        workflow.connect(inputnode, 'visit_id', sink, 'visit_id')
        for source_file in source_files:
            if source_file.name != 'source2':
                sink_name = source_file.name.replace('source', 'sink')
                workflow.connect(
                    source, source_file.name + OUTPUT_SUFFIX,
                    sink, sink_name + INPUT_SUFFIX)
        workflow.run()
        # Check cache was created properly
        source_cache_dir = os.path.join(
            self.archive_cache_dir, str(self.PROJECT),
            str(self.SUBJECT), str(self.SESSION))
        sink_cache_dir = os.path.join(
            self.archive_cache_dir, str(self.PROJECT),
            str(self.SUBJECT),
            str(self.SESSION) + XNATArchive.PROCESSED_SUFFIX)
        self.assertEqual(sorted(os.listdir(source_cache_dir)),
                         ['source1.nii.gz', 'source2.nii.gz',
                          'source3.nii.gz', 'source4.nii.gz'])
        expected_sink_datasets = [self.STUDY_NAME + '_sink1',
                                  self.STUDY_NAME + '_sink3',
                                  self.STUDY_NAME + '_sink4']
        self.assertEqual(sorted(os.listdir(sink_cache_dir)),
                         [d + nifti_gz_format.extension
                          for d in expected_sink_datasets])
        with self._connect() as mbi_xnat:
            dataset_names = mbi_xnat.experiments[
                self.session_id +
                XNATArchive.PROCESSED_SUFFIX].scans.keys()
        self.assertEqual(sorted(dataset_names), expected_sink_datasets)

    def test_summary(self):
        # Create working dirs
        # Create XNATSource node
        archive = XNATArchive(
            server=self.SERVER, cache_dir=self.archive_cache_dir)
        # TODO: Should test out other file formats as well.
        source_files = [Dataset('source1', nifti_gz_format),
                        Dataset('source2', nifti_gz_format),
                        Dataset('source3', nifti_gz_format)]
        inputnode = pe.Node(IdentityInterface(['subject_id', 'visit_id']),
                            'inputnode')
        inputnode.inputs.subject_id = self.SUBJECT
        inputnode.inputs.visit_id = self.SESSION
        source = archive.source(self.PROJECT, source_files)
        subject_sink_files = [DatasetSpec('sink1', nifti_gz_format,
                                          multiplicity='per_subject',
                                          pipeline=dummy_pipeline)]
        subject_sink = archive.sink(self.PROJECT,
                                    subject_sink_files,
                                    multiplicity='per_subject',
                                    study_name=self.SUMMARY_STUDY_NAME)
        subject_sink.inputs.name = 'subject_summary'
        subject_sink.inputs.description = (
            "Tests the sinking of subject-wide datasets")
        # Test visit sink
        visit_sink_files = [DatasetSpec('sink2', nifti_gz_format,
                                        multiplicity='per_visit',
                                        pipeline=dummy_pipeline)]
        visit_sink = archive.sink(self.PROJECT,
                                      visit_sink_files,
                                      multiplicity='per_visit',
                                      study_name=self.SUMMARY_STUDY_NAME)
        visit_sink.inputs.name = 'visit_summary'
        visit_sink.inputs.description = (
            "Tests the sinking of visit-wide datasets")
        # Test project sink
        project_sink_files = [DatasetSpec('sink3', nifti_gz_format,
                                          multiplicity='per_project',
                                          ipeline=dummy_pipeline)]
        project_sink = archive.sink(self.PROJECT,
                                    project_sink_files,
                                    multiplicity='per_project',
                                    study_name=self.SUMMARY_STUDY_NAME)

        project_sink.inputs.name = 'project_summary'
        project_sink.inputs.description = (
            "Tests the sinking of project-wide datasets")
        # Create workflow connecting them together
        workflow = pe.Workflow('summary_unittest',
                               base_dir=self.work_dir)
        workflow.add_nodes((source, subject_sink, visit_sink,
                            project_sink))
        workflow.connect(inputnode, 'subject_id', source, 'subject_id')
        workflow.connect(inputnode, 'visit_id', source, 'visit_id')
        workflow.connect(inputnode, 'subject_id', subject_sink, 'subject_id')
        workflow.connect(inputnode, 'visit_id', visit_sink, 'visit_id')
        workflow.connect(
            source, 'source1' + OUTPUT_SUFFIX,
            subject_sink, 'sink1' + INPUT_SUFFIX)
        workflow.connect(
            source, 'source2' + OUTPUT_SUFFIX,
            visit_sink, 'sink2' + INPUT_SUFFIX)
        workflow.connect(
            source, 'source3' + OUTPUT_SUFFIX,
            project_sink, 'sink3' + INPUT_SUFFIX)
        workflow.run()
        with self._connect() as mbi_xnat:
            # Check subject summary directories were created properly in cache
            expected_subj_datasets = [self.SUMMARY_STUDY_NAME + '_sink1']
            subject_dir = os.path.join(
                self.archive_cache_dir, self.PROJECT, self.SUBJECT,
                self.SUBJECT + '_' + XNATArchive.SUMMARY_NAME)
            self.assertEqual(sorted(os.listdir(subject_dir)),
                             [d + nifti_gz_format.extension
                              for d in expected_subj_datasets])
            # and on XNAT
            subject_dataset_names = mbi_xnat.projects[
                self.PROJECT].experiments[
                    '{}_{}'.format(self.SUBJECT,
                                   XNATArchive.SUMMARY_NAME)].scans.keys()
            self.assertEqual(expected_subj_datasets, subject_dataset_names)
            # Check visit summary directories were created properly in
            # cache
            expected_visit_datasets = [self.SUMMARY_STUDY_NAME + '_sink2']
            visit_dir = os.path.join(
                self.archive_cache_dir, self.PROJECT,
                self.PROJECT + '_' + XNATArchive.SUMMARY_NAME,
                (self.PROJECT + '_' + XNATArchive.SUMMARY_NAME +
                 '_' + self.SESSION))
            self.assertEqual(sorted(os.listdir(visit_dir)),
                             [d + nifti_gz_format.extension
                              for d in expected_visit_datasets])
            # and on XNAT
            visit_dataset_names = mbi_xnat.projects[
                self.PROJECT].experiments[
                    '{}_{}_{}'.format(
                        self.PROJECT, XNATArchive.SUMMARY_NAME,
                        self.SESSION)].scans.keys()
            self.assertEqual(expected_visit_datasets, visit_dataset_names)
            # Check project summary directories were created properly in cache
            expected_proj_datasets = [self.SUMMARY_STUDY_NAME + '_sink3']
            project_dir = os.path.join(
                self.archive_cache_dir, self.PROJECT,
                self.PROJECT + '_' + XNATArchive.SUMMARY_NAME,
                self.PROJECT + '_' + XNATArchive.SUMMARY_NAME + '_' +
                XNATArchive.SUMMARY_NAME)
            self.assertEqual(sorted(os.listdir(project_dir)),
                             [d + nifti_gz_format.extension
                              for d in expected_proj_datasets])
            # and on XNAT
            project_dataset_names = mbi_xnat.projects[
                self.PROJECT].experiments[
                    '{}_{sum}_{sum}'.format(
                        self.PROJECT,
                        sum=XNATArchive.SUMMARY_NAME)].scans.keys()
            self.assertEqual(expected_proj_datasets, project_dataset_names)
        # Reload the data from the summary directories
        reloadinputnode = pe.Node(IdentityInterface(['subject_id',
                                                     'visit_id']),
                                  'reload_inputnode')
        reloadinputnode.inputs.subject_id = self.SUBJECT
        reloadinputnode.inputs.visit_id = self.SESSION
        reloadsource = archive.source(
            self.PROJECT,
            (source_files + subject_sink_files + visit_sink_files +
             project_sink_files),
            name='reload_source',
            study_name=self.SUMMARY_STUDY_NAME)
        reloadsink = archive.sink(self.PROJECT,
                                  [DatasetSpec('resink1', nifti_gz_format,
                                               pipeline=dummy_pipeline),
                                   DatasetSpec('resink2', nifti_gz_format,
                                               pipeline=dummy_pipeline),
                                   DatasetSpec('resink3', nifti_gz_format,
                                               pipeline=dummy_pipeline)],
                                  study_name=self.SUMMARY_STUDY_NAME)
        reloadsink.inputs.name = 'reload_summary'
        reloadsink.inputs.description = (
            "Tests the reloading of subject and project summary datasets")
        reloadworkflow = pe.Workflow('reload_summary_unittest',
                                     base_dir=self.work_dir)
        reloadworkflow.connect(reloadinputnode, 'subject_id',
                               reloadsource, 'subject_id')
        reloadworkflow.connect(reloadinputnode, 'visit_id',
                               reloadsource, 'visit_id')
        reloadworkflow.connect(reloadinputnode, 'subject_id',
                               reloadsink, 'subject_id')
        reloadworkflow.connect(reloadinputnode, 'visit_id',
                               reloadsink, 'visit_id')
        reloadworkflow.connect(reloadsource, 'sink1' + OUTPUT_SUFFIX,
                               reloadsink, 'resink1' + INPUT_SUFFIX)
        reloadworkflow.connect(reloadsource, 'sink2' + OUTPUT_SUFFIX,
                               reloadsink, 'resink2' + INPUT_SUFFIX)
        reloadworkflow.connect(reloadsource, 'sink3' + OUTPUT_SUFFIX,
                               reloadsink, 'resink3' + INPUT_SUFFIX)
        reloadworkflow.run()
        # Check that the datasets
        session_dir = os.path.join(
            self.archive_cache_dir, self.PROJECT, self.SUBJECT,
            self.SESSION + XNATArchive.PROCESSED_SUFFIX)
        self.assertEqual(sorted(os.listdir(session_dir)),
                         [self.SUMMARY_STUDY_NAME + '_resink1.nii.gz',
                          self.SUMMARY_STUDY_NAME + '_resink2.nii.gz',
                          self.SUMMARY_STUDY_NAME + '_resink3.nii.gz'])
        # and on XNAT
        with self._connect() as mbi_xnat:
            resinked_dataset_names = mbi_xnat.projects[
                self.PROJECT].experiments[
                    self.session_id +
                    XNATArchive.PROCESSED_SUFFIX].scans.keys()
            self.assertEqual(sorted(resinked_dataset_names),
                             [self.SUMMARY_STUDY_NAME + '_resink1',
                              self.SUMMARY_STUDY_NAME + '_resink2',
                              self.SUMMARY_STUDY_NAME + '_resink3'])

    def test_project_info(self):
        archive = XNATArchive(
            server=self.SERVER, cache_dir=self.archive_cache_dir)
        project_info = archive.project(self.PROJECT)
        self.assertEqual(sorted(s.id for s in project_info.subjects),
                         [self.SUBJECT])
        subject = list(project_info.subjects)[0]
        self.assertEqual([s.id for s in subject.sessions],
                         [self.SESSION])
        session = list(subject.sessions)[0]
        self.assertEqual(
            sorted(d.name for d in sorted(session.datasets)),
            ['source1', 'source2', 'source3', 'source4'])


class TestXnatArchiveSpecialCharInScanName(TestCase):

    PROJECT = 'MRH033'
    SUBJECT = 'MRH033_001'
    VISIT = 'MR01'
    SERVER = 'https://mbi-xnat.erc.monash.edu.au'
    TEST_NAME = 'special_char_in_scan_name'
    DATASETS = ['localizer 3 PLANES (Left)',
                'PosDisp: [3] cv_t1rho_3D_2_TR450 (Left)']
    WORK_PATH = os.path.join(test_data_dir, 'work', TEST_NAME)

    def test_special_char_in_scan_name(self):
        """
        Tests whether XNAT source can download files with spaces in their names
        """
        cache_dir = tempfile.mkdtemp()
        archive = XNATArchive(
            server=self.SERVER, cache_dir=cache_dir)
        source = archive.source(
            self.PROJECT, [Dataset(d, dicom_format) for d in self.DATASETS])
        source.inputs.subject_id = self.SUBJECT
        source.inputs.visit_id = self.VISIT
        workflow = pe.Workflow(self.TEST_NAME, base_dir=self.WORK_PATH)
        workflow.add_nodes([source])
        graph = workflow.run()
        result = next(n.result for n in graph.nodes() if n.name == source.name)
        for dname in self.DATASETS:
            path = getattr(result.outputs, dname + OUTPUT_SUFFIX)
            self.assertEqual(os.path.basename(path), dname)
            self.assertTrue(os.path.exists(path))


class TestOnXnatMixin(object):

    def setUp(self):
        self._clean_up()
        cache_dir = os.path.join(self.CACHE_BASE_PATH,
                                 self._get_name(self.base_class))
        self.base_class.setUp(self, cache_dir=cache_dir)
        with xnat.connect(self.SERVER) as mbi_xnat:
            # Copy local archive to XNAT
            xproject = mbi_xnat.projects[self.PROJECT]
            for subj in os.listdir(self.project_dir):
                subj_dir = os.path.join(self.project_dir, subj)
                subj_id = self.PROJECT + '_' + subj
                xsubject = mbi_xnat.classes.SubjectData(label=subj_id,
                                                        parent=xproject)
                for visit in os.listdir(subj_dir):
                    sess_dir = os.path.join(subj_dir, visit)
                    for scan_fname in os.listdir(sess_dir):
                        scan_name, ext = split_extension(scan_fname)
                        sess_id = subj_id + '_' + visit
                        if '_' in scan_name:
                            sess_id += XNATArchive.PROCESSED_SUFFIX
                        xsession = mbi_xnat.classes.MrSessionData(
                            label=sess_id,
                            parent=xsubject)
                        dataset = mbi_xnat.classes.MrScanData(type=scan_name,
                                                              parent=xsession)
                        resource = dataset.create_resource(
                            data_formats_by_ext[ext].name.upper())
                        resource.upload(os.path.join(sess_dir, scan_fname),
                                        scan_fname)
        self._output_cache_dir = tempfile.mkdtemp()

    def tearDown(self):
        self._clean_up()

    def _clean_up(self):
        # Clean up working dirs
        shutil.rmtree(self.cache_dir, ignore_errors=True)
        # Clean up session created for unit-test
        with xnat.connect(self.SERVER) as mbi_xnat:
            xproject = mbi_xnat.projects[self.PROJECT]
            for xsubject in list(xproject.subjects.itervalues()):
                xsubject.delete()

    @property
    def archive(self):
        return XNATArchive(server=self.SERVER, cache_dir=self.cache_dir)

    @property
    def xnat_session_name(self):
        return '{}_{}'.format(self.XNAT_TEST_PROJECT,
                              self._get_name(self.base_class))

    @property
    def project_dir(self):
        return os.path.join(self.ARCHIVE_PATH, self._get_name(self.base_class))

    @property
    def output_cache_dir(self):
        return self._output_cache_dir

    @property
    def base_class(self):
        return type(self).__mro__[2]

    @property
    def project_id(self):
        return self.PROJECT

    def _full_subject_id(self, subject):
        return self.PROJECT + '_' + subject

    def _proc_sess_id(self, session):
        return session + XNATArchive.PROCESSED_SUFFIX

    def get_session_dir(self, subject=None, session=None,
                        multiplicity='per_session', processed=False):
        if subject is None and multiplicity in ('per_session', 'per_subject'):
            subject = self.SUBJECT
        if session is None and multiplicity in ('per_session', 'per_visit'):
            session = self.SESSION
        if multiplicity == 'per_session':
            assert subject is not None
            assert session is not None
            parts = [self.PROJECT, subject, session]
        elif multiplicity == 'per_subject':
            assert subject is not None
            assert session is None
            parts = [self.PROJECT, subject, XNATArchive.SUMMARY_NAME]
        elif multiplicity == 'per_visit':
            assert session is not None
            assert subject is None
            parts = [self.PROJECT, XNATArchive.SUMMARY_NAME, session]
        elif multiplicity == 'per_project':
            assert subject is None
            assert session is None
            parts = [self.PROJECT, XNATArchive.SUMMARY_NAME,
                     XNATArchive.SUMMARY_NAME]
        else:
            assert False
        session_id = '_'.join(parts)
        if processed:
            session_id += XNATArchive.PROCESSED_SUFFIX
        session_path = os.path.join(self.output_cache_dir, session_id)
        if not os.path.exists(session_path):
            download_all_datasets(session_path, self.SERVER, session_id)
        return session_path

    def output_file_path(self, fname, study_name, subject=None, session=None,
                         multiplicity='per_session'):
        try:
            acq_path = self.base_class.output_file_path(
                self, fname, study_name, subject=subject, session=session,
                multiplicity=multiplicity, processed=False)
        except KeyError:
            acq_path = None
        try:
            proc_path = self.base_class.output_file_path(
                self, fname, study_name, subject=subject, session=session,
                multiplicity=multiplicity, processed=True)
        except KeyError:
            proc_path = None
        if acq_path is not None and os.path.exists(acq_path):
            if os.path.exists(proc_path):
                raise NiAnalysisError(
                    "Both acquired and processed paths were found for "
                    "'{}_{}' ({} and {})".format(study_name, fname, acq_path,
                                                 proc_path))
            path = acq_path
        else:
            path = proc_path
        return path


class TestExistingPrereqsOnXnat(TestOnXnatMixin, TestExistingPrereqs):

    PROJECT = 'TEST007'

    def test_per_session_prereqs(self):
        super(TestExistingPrereqsOnXnat, self).test_per_session_prereqs()
