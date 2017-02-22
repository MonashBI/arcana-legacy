import os.path
import shutil
import xnat
from nianalysis.testing import BaseTestCase
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from nianalysis.archive.xnat import XNATArchive
from nianalysis.data_formats import nifti_gz_format
from nianalysis.dataset import Dataset
import logging
from nianalysis.utils import split_extension
from nianalysis.data_formats import data_formats_by_ext
from nianalysis.utils import INPUT_SUFFIX, OUTPUT_SUFFIX
from nianalysis.archive.xnat import download_all_datasets

logger = logging.getLogger('NiAnalysis')


class TestXnatArchive(BaseTestCase):

    PROJECT = 'TEST002'
    SUBJECT = 'TEST002_001'
    SESSION = 'MR01'
    STUDY_NAME = 'astudy'
    SUMMARY_STUDY_NAME = 'asummary'

    @property
    def full_session_id(self):
        return '_'.join((self.SUBJECT, self.SESSION))

    def setUp(self):
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
                label=self.full_session_id,
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
        return xnat.connect(self.XNAT_URL, user=self.XNAT_LOGIN,
                            password=self.XNAT_PASSWORD)

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
        sink_files = [Dataset('sink1', nifti_gz_format, processed=True),
                      Dataset('sink3', nifti_gz_format, processed=True),
                      Dataset('sink4', nifti_gz_format, processed=True)]
        inputnode = pe.Node(IdentityInterface(['subject_id', 'session_id']),
                            'inputnode')
        inputnode.inputs.subject_id = str(self.SUBJECT)
        inputnode.inputs.session_id = str(self.SESSION)
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
        workflow.connect(inputnode, 'session_id', source, 'session_id')
        workflow.connect(inputnode, 'subject_id', sink, 'subject_id')
        workflow.connect(inputnode, 'session_id', sink, 'session_id')
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
                self.full_session_id +
                XNATArchive.PROCESSED_SUFFIX].scans.keys()
        self.assertEqual(sorted(dataset_names), expected_sink_datasets)

    def test_summary(self):
        # Create working dirs
        # Create XNATSource node
        archive = XNATArchive(
            server=self.XNAT_URL, cache_dir=self.archive_cache_dir,
            user=self.XNAT_LOGIN, password=self.XNAT_PASSWORD)
        # TODO: Should test out other file formats as well.
        source_files = [Dataset('source1', nifti_gz_format),
                        Dataset('source2', nifti_gz_format)]
        inputnode = pe.Node(IdentityInterface(['subject_id', 'session_id']),
                            'inputnode')
        inputnode.inputs.subject_id = self.SUBJECT
        inputnode.inputs.session_id = self.SESSION
        source = archive.source(self.PROJECT, source_files)
        subject_sink_files = [Dataset('sink1', nifti_gz_format,
                                      multiplicity='per_subject',
                                      processed=True)]
        subject_sink = archive.sink(self.PROJECT,
                                    subject_sink_files,
                                    multiplicity='per_subject',
                                    study_name=self.SUMMARY_STUDY_NAME)
        subject_sink.inputs.name = 'subject_summary'
        subject_sink.inputs.description = (
            "Tests the sinking of subject-wide datasets")
        project_sink_files = [Dataset('sink2', nifti_gz_format,
                                      multiplicity='per_project',
                                      processed=True)]
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
        workflow.add_nodes((source, subject_sink, project_sink))
        workflow.connect(inputnode, 'subject_id', source, 'subject_id')
        workflow.connect(inputnode, 'session_id', source, 'session_id')
        workflow.connect(inputnode, 'subject_id', subject_sink, 'subject_id')
        workflow.connect(
            source, 'source1' + OUTPUT_SUFFIX,
            subject_sink, 'sink1' + INPUT_SUFFIX)
        workflow.connect(
            source, 'source2' + OUTPUT_SUFFIX,
            project_sink, 'sink2' + INPUT_SUFFIX)
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
            # Check project summary directories were created properly in cache
            expected_proj_datasets = [self.SUMMARY_STUDY_NAME + '_sink2']
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
                                                     'session_id']),
                                  'reload_inputnode')
        reloadinputnode.inputs.subject_id = self.SUBJECT
        reloadinputnode.inputs.session_id = self.SESSION
        reloadsource = archive.source(
            self.PROJECT,
            source_files + subject_sink_files + project_sink_files,
            name='reload_source',
            study_name=self.SUMMARY_STUDY_NAME)
        reloadsink = archive.sink(self.PROJECT,
                                  [Dataset('resink1', nifti_gz_format,
                                           processed=True),
                                   Dataset('resink2', nifti_gz_format,
                                           processed=True)],
                                  study_name=self.SUMMARY_STUDY_NAME)
        reloadsink.inputs.name = 'reload_summary'
        reloadsink.inputs.description = (
            "Tests the reloading of subject and project summary datasets")
        reloadworkflow = pe.Workflow('reload_summary_unittest',
                                     base_dir=self.work_dir)
        reloadworkflow.connect(reloadinputnode, 'subject_id',
                               reloadsource, 'subject_id')
        reloadworkflow.connect(reloadinputnode, 'session_id',
                               reloadsource, 'session_id')
        reloadworkflow.connect(reloadinputnode, 'subject_id',
                               reloadsink, 'subject_id')
        reloadworkflow.connect(reloadinputnode, 'session_id',
                               reloadsink, 'session_id')
        reloadworkflow.connect(reloadsource, 'sink1' + OUTPUT_SUFFIX,
                               reloadsink, 'resink1' + INPUT_SUFFIX)
        reloadworkflow.connect(reloadsource, 'sink2' + OUTPUT_SUFFIX,
                               reloadsink, 'resink2' + INPUT_SUFFIX)
        reloadworkflow.run()
        # Check that the datasets
        session_dir = os.path.join(
            self.archive_cache_dir, self.PROJECT, self.SUBJECT,
            self.SESSION + XNATArchive.PROCESSED_SUFFIX)
        self.assertEqual(sorted(os.listdir(session_dir)),
                         [self.SUMMARY_STUDY_NAME + '_resink1.nii.gz',
                          self.SUMMARY_STUDY_NAME + '_resink2.nii.gz'])
        # and on XNAT
        with self._connect() as mbi_xnat:
            resinked_dataset_names = mbi_xnat.projects[
                self.PROJECT].experiments[
                    self.full_session_id +
                    XNATArchive.PROCESSED_SUFFIX].scans.keys()
            self.assertEqual(sorted(resinked_dataset_names),
                             [self.SUMMARY_STUDY_NAME + '_resink1',
                              self.SUMMARY_STUDY_NAME + '_resink2'])

    def test_project_info(self):
        archive = XNATArchive(
            server=self.XNAT_URL, cache_dir=self.archive_cache_dir,
            user=self.XNAT_LOGIN, password=self.XNAT_PASSWORD)
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
