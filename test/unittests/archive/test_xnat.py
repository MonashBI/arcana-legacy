import os.path
import shutil
import xnat
from unittest import TestCase
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from nianalysis.archive.xnat import (
    XNATArchive, XNATSource, XNATSink)
from nianalysis.data_formats import nifti_gz_format
from nianalysis.dataset import Dataset
from nianalysis.testing import test_data_dir
import logging
from nianalysis.utils import split_extension
from nianalysis.data_formats import data_formats_by_ext

logger = logging.getLogger('NiAnalysis')


class TestXnatArchive(TestCase):

    XNAT_URL = 'https://mbi-xnat.erc.monash.edu.au'
    XNAT_LOGIN = 'unittest'
    XNAT_PASSWORD = 'Test123!'

    PROJECT_ID = 'TEST000'
    SUBJECT_ID = 'TEST000_001'
    SESSION_ID = 'TEST000_001_MR01'
    STUDY_NAME = 'astudy'
    SUMMARY_STUDY_NAME = 'asummary'
    TEST_IMAGE = os.path.abspath(os.path.join(test_data_dir,
                                              'test_image.nii.gz'))
    TEST_DIR = os.path.abspath(os.path.join(test_data_dir, 'xnat'))
    CACHE_DIR = os.path.abspath(os.path.join(TEST_DIR, 'base_dir'))
    WORKFLOW_DIR = os.path.abspath(os.path.join(TEST_DIR, 'workflow_dir'))

    def setUp(self):
        # Create test data on DaRIS
        self._session_id = None
        # Make cache and working dirs
        shutil.rmtree(self.TEST_DIR, ignore_errors=True)
        shutil.rmtree(self.CACHE_DIR, ignore_errors=True)
        self._delete_test_subjects()
        os.makedirs(self.WORKFLOW_DIR)
        os.makedirs(self.CACHE_DIR)
        with self._connect() as mbi_xnat:
            project = mbi_xnat.projects[self.PROJECT_ID]
            subject = mbi_xnat.classes.SubjectData(
                label=self.SUBJECT_ID, parent=project)
            session = mbi_xnat.classes.MrSessionData(
                label=self.SESSION_ID, parent=subject)
            for fname in ('source1.nii.gz', 'source2.nii.gz', 'source3.nii.gz',
                          'source4.nii.gz'):
                name, ext = split_extension(fname)
                dataset = mbi_xnat.classes.MrScanData(type=name,
                                                      parent=session)
                resource = dataset.create_resource(
                    data_formats_by_ext[ext].name)
                resource.upload(self.TEST_IMAGE, fname)

    def tearDown(self):
        # Clean up working dirs
        shutil.rmtree(self.TEST_DIR, ignore_errors=True)
        shutil.rmtree(self.CACHE_DIR, ignore_errors=True)
        # Clean up session created for unit-test
        self._delete_test_subjects()

    def _delete_test_subjects(self):
        with self._connect() as mbi_xnat:
            project = mbi_xnat.projects[self.PROJECT_ID]
            if self.SUBJECT_ID in project.subjects:
                project.subjects[self.SUBJECT_ID].delete()
            project_summary_name = (self.PROJECT_ID + '_' +
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
            server=self.XNAT_URL, cache_dir=self.CACHE_DIR,
            user=self.XNAT_LOGIN, password=self.XNAT_PASSWORD)
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
        inputnode.inputs.subject_id = str(self.SUBJECT_ID)
        inputnode.inputs.session_id = str(self.SESSION_ID)
        source = archive.source(self.PROJECT_ID, source_files,
                                study_name=self.STUDY_NAME)
        sink = archive.sink(self.PROJECT_ID, sink_files,
                                study_name=self.STUDY_NAME)
        sink.inputs.name = 'archive-roundtrip-unittest'
        sink.inputs.description = (
            "A test session created by archive roundtrip unittest")
        # Create workflow connecting them together
        workflow = pe.Workflow('source-sink-unit-test',
                               base_dir=self.WORKFLOW_DIR)
        workflow.add_nodes((source, sink))
        workflow.connect(inputnode, 'subject_id', source, 'subject_id')
        workflow.connect(inputnode, 'session_id', source, 'session_id')
        workflow.connect(inputnode, 'subject_id', sink, 'subject_id')
        workflow.connect(inputnode, 'session_id', sink, 'session_id')
        for source_file in source_files:
            if source_file.name != 'source2':
                sink_name = source_file.name.replace('source', 'sink')
                workflow.connect(
                    source, source_file.name + XNATSource.OUTPUT_SUFFIX,
                    sink, sink_name + XNATSink.INPUT_SUFFIX)
        workflow.run()
        # Check cache was created properly
        source_cache_dir = os.path.join(
            self.CACHE_DIR, str(self.PROJECT_ID),
            str(self.SUBJECT_ID), str(self.SESSION_ID))
        sink_cache_dir = os.path.join(
            self.CACHE_DIR, str(self.PROJECT_ID),
            str(self.SUBJECT_ID),
            str(self.SESSION_ID) + XNATArchive.PROCESSED_SUFFIX)
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
                self.SESSION_ID + XNATArchive.PROCESSED_SUFFIX].scans.keys()
        self.assertEqual(sorted(dataset_names), expected_sink_datasets)

    def test_summary(self):
        # Create working dirs
        # Create XNATSource node
        archive = XNATArchive(
            server=self.XNAT_URL, cache_dir=self.CACHE_DIR,
            user=self.XNAT_LOGIN, password=self.XNAT_PASSWORD)
        # TODO: Should test out other file formats as well.
        source_files = [Dataset('source1', nifti_gz_format),
                        Dataset('source2', nifti_gz_format)]
        inputnode = pe.Node(IdentityInterface(['subject_id', 'session_id']),
                            'inputnode')
        inputnode.inputs.subject_id = self.SUBJECT_ID
        inputnode.inputs.session_id = self.SESSION_ID
        source = archive.source(self.PROJECT_ID, source_files)
        subject_sink_files = [Dataset('sink1', nifti_gz_format,
                                      multiplicity='per_subject',
                                      processed=True)]
        subject_sink = archive.sink(self.PROJECT_ID,
                                    subject_sink_files,
                                    multiplicity='per_subject',
                                    study_name=self.SUMMARY_STUDY_NAME)
        subject_sink.inputs.name = 'subject_summary'
        subject_sink.inputs.description = (
            "Tests the sinking of subject-wide datasets")
        project_sink_files = [Dataset('sink2', nifti_gz_format,
                                      multiplicity='per_project',
                                      processed=True)]
        project_sink = archive.sink(self.PROJECT_ID,
                                    project_sink_files,
                                    multiplicity='per_project',
                                    study_name=self.SUMMARY_STUDY_NAME)

        project_sink.inputs.name = 'project_summary'
        project_sink.inputs.description = (
            "Tests the sinking of project-wide datasets")
        # Create workflow connecting them together
        workflow = pe.Workflow('summary_unittest',
                               base_dir=self.WORKFLOW_DIR)
        workflow.add_nodes((source, subject_sink, project_sink))
        workflow.connect(inputnode, 'subject_id', source, 'subject_id')
        workflow.connect(inputnode, 'session_id', source, 'session_id')
        workflow.connect(inputnode, 'subject_id', subject_sink, 'subject_id')
        workflow.connect(
            source, 'source1' + XNATSource.OUTPUT_SUFFIX,
            subject_sink, 'sink1' + XNATSink.INPUT_SUFFIX)
        workflow.connect(
            source, 'source2' + XNATSource.OUTPUT_SUFFIX,
            project_sink, 'sink2' + XNATSink.INPUT_SUFFIX)
        workflow.run()
        with self._connect() as mbi_xnat:
            # Check subject summary directories were created properly in cache
            expected_subj_datasets = [self.SUMMARY_STUDY_NAME + '_sink1']
            subject_dir = os.path.join(
                self.CACHE_DIR, self.PROJECT_ID, self.SUBJECT_ID,
                self.SUBJECT_ID + '_' + XNATArchive.SUMMARY_NAME)
            self.assertEqual(sorted(os.listdir(subject_dir)),
                             [d + nifti_gz_format.extension
                              for d in expected_subj_datasets])
            # and on XNAT
            subject_dataset_names = mbi_xnat.projects[
                self.PROJECT_ID].experiments[
                    '{}_{}'.format(self.SUBJECT_ID,
                                   XNATArchive.SUMMARY_NAME)].scans.keys()
            self.assertEqual(expected_subj_datasets, subject_dataset_names)
            # Check project summary directories were created properly in cache
            expected_proj_datasets = [self.SUMMARY_STUDY_NAME + '_sink2']
            project_dir = os.path.join(
                self.CACHE_DIR, self.PROJECT_ID,
                self.PROJECT_ID + '_' + XNATArchive.SUMMARY_NAME,
                self.PROJECT_ID + '_' + XNATArchive.SUMMARY_NAME + '_' +
                XNATArchive.SUMMARY_NAME)
            self.assertEqual(sorted(os.listdir(project_dir)),
                             [d + nifti_gz_format.extension
                              for d in expected_proj_datasets])
            # and on XNAT
            project_dataset_names = mbi_xnat.projects[
                self.PROJECT_ID].experiments[
                    '{}_{sum}_{sum}'.format(
                        self.PROJECT_ID,
                        sum=XNATArchive.SUMMARY_NAME)].scans.keys()
            self.assertEqual(expected_proj_datasets, project_dataset_names)
        # Reload the data from the summary directories
        reloadinputnode = pe.Node(IdentityInterface(['subject_id',
                                                     'session_id']),
                                  'reload_inputnode')
        reloadinputnode.inputs.subject_id = self.SUBJECT_ID
        reloadinputnode.inputs.session_id = self.SESSION_ID
        reloadsource = archive.source(
            self.PROJECT_ID,
            source_files + subject_sink_files + project_sink_files,
            name='reload_source',
            study_name=self.SUMMARY_STUDY_NAME)
        reloadsink = archive.sink(self.PROJECT_ID,
                                  [Dataset('resink1', nifti_gz_format,
                                           processed=True),
                                   Dataset('resink2', nifti_gz_format,
                                           processed=True)],
                                  study_name=self.SUMMARY_STUDY_NAME)
        reloadsink.inputs.name = 'reload_summary'
        reloadsink.inputs.description = (
            "Tests the reloading of subject and project summary datasets")
        reloadworkflow = pe.Workflow('reload_summary_unittest',
                                     base_dir=self.WORKFLOW_DIR)
        reloadworkflow.connect(reloadinputnode, 'subject_id',
                               reloadsource, 'subject_id')
        reloadworkflow.connect(reloadinputnode, 'session_id',
                               reloadsource, 'session_id')
        reloadworkflow.connect(reloadinputnode, 'subject_id',
                               reloadsink, 'subject_id')
        reloadworkflow.connect(reloadinputnode, 'session_id',
                               reloadsink, 'session_id')
        reloadworkflow.connect(reloadsource,
                               'sink1' + XNATSource.OUTPUT_SUFFIX,
                               reloadsink,
                               'resink1' + XNATSink.INPUT_SUFFIX)
        reloadworkflow.connect(reloadsource,
                               'sink2' + XNATSource.OUTPUT_SUFFIX,
                               reloadsink,
                               'resink2' + XNATSink.INPUT_SUFFIX)
        reloadworkflow.run()
        # Check that the datasets
        session_dir = os.path.join(
            self.CACHE_DIR, self.PROJECT_ID, self.SUBJECT_ID,
            self.SESSION_ID + XNATArchive.PROCESSED_SUFFIX)
        self.assertEqual(sorted(os.listdir(session_dir)),
                         [self.SUMMARY_STUDY_NAME + '_resink1.nii.gz',
                          self.SUMMARY_STUDY_NAME + '_resink2.nii.gz'])
        # and on XNAT
        with self._connect() as mbi_xnat:
            resinked_dataset_names = mbi_xnat.projects[
                self.PROJECT_ID].experiments[
                    self.SESSION_ID +
                    XNATArchive.PROCESSED_SUFFIX].scans.keys()
            self.assertEqual(sorted(resinked_dataset_names),
                             [self.SUMMARY_STUDY_NAME + '_resink1',
                              self.SUMMARY_STUDY_NAME + '_resink2'])
