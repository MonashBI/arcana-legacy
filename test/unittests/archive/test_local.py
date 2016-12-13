import os.path
import shutil
from unittest import TestCase
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from nianalysis.archive.local import (
    LocalArchive, LocalSource, LocalSink, SUBJECT_SUMMARY_NAME,
    PROJECT_SUMMARY_NAME)
from nianalysis.data_formats import nifti_gz_format
from nianalysis.dataset import Dataset
from nianalysis.testing import test_data_dir
import logging

logger = logging.getLogger('NiAnalysis')


class TestLocalArchive(TestCase):

    PROJECT_ID = 'DUMMYPROJECTID'
    SUBJECT_ID = 'DUMMYSUBJECTID'
    SESSION_ID = 'DUMMYSESSIONID'
    STUDY_NAME = 'ASTUDY'
    SUMMARY_STUDY_NAME = 'ASUMMARYSTUDY'
    TEST_IMAGE = os.path.abspath(os.path.join(test_data_dir,
                                              'test_image.nii.gz'))
    TEST_DIR = os.path.abspath(os.path.join(test_data_dir, 'local'))
    BASE_DIR = os.path.abspath(os.path.join(TEST_DIR, 'base_dir'))
    WORKFLOW_DIR = os.path.abspath(os.path.join(TEST_DIR, 'workflow_dir'))

    def setUp(self):
        # Create test data on DaRIS
        self._session_id = None
        # Make cache and working dirs
        shutil.rmtree(self.TEST_DIR, ignore_errors=True)
        os.makedirs(self.WORKFLOW_DIR)
        session_path = os.path.join(
            self.BASE_DIR, self.PROJECT_ID, self.SUBJECT_ID, self.SESSION_ID)
        os.makedirs(session_path)
        for i in xrange(1, 5):
            shutil.copy(self.TEST_IMAGE,
                        os.path.join(session_path,
                                     'source{}.nii.gz'.format(i)))

    def tearDown(self):
        # Clean up working dirs
        shutil.rmtree(self.TEST_DIR, ignore_errors=True)

    def test_archive_roundtrip(self):
        # Create working dirs
        # Create LocalSource node
        archive = LocalArchive(base_dir=self.BASE_DIR)
        # TODO: Should test out other file formats as well.
        source_files = [Dataset('source1', nifti_gz_format),
                        Dataset('source2', nifti_gz_format),
                        Dataset('source3', nifti_gz_format),
                        Dataset('source4', nifti_gz_format)]
        sink_files = [Dataset('sink1', nifti_gz_format, processed=True),
                      Dataset('sink3', nifti_gz_format, processed=True),
                      Dataset('sink4', nifti_gz_format, processed=True)]
        inputnode = pe.Node(IdentityInterface(['subject_id', 'session_id']),
                            'inputnode')
        inputnode.inputs.subject_id = self.SUBJECT_ID
        inputnode.inputs.session_id = self.SESSION_ID
        source = archive.source(self.PROJECT_ID, source_files,
                                study_name=self.STUDY_NAME)
        sink = archive.sink(self.PROJECT_ID, sink_files,
                                study_name=self.STUDY_NAME)
        sink.inputs.name = 'archive_sink'
        sink.inputs.description = (
            "A test session created by archive roundtrip unittest")
        # Create workflow connecting them together
        workflow = pe.Workflow('source_sink_unit_test',
                               base_dir=self.WORKFLOW_DIR)
        workflow.add_nodes((source, sink))
        workflow.connect(inputnode, 'subject_id', source, 'subject_id')
        workflow.connect(inputnode, 'session_id', source, 'session_id')
        workflow.connect(inputnode, 'subject_id', sink, 'subject_id')
        workflow.connect(inputnode, 'session_id', sink, 'session_id')
        for source_file in source_files:
            if not source_file.name.endswith('2'):
                source_name = source_file.name
                sink_name = source_name.replace('source', 'sink')
                workflow.connect(
                    source, source_name + LocalSource.OUTPUT_SUFFIX,
                    sink, sink_name + LocalSink.INPUT_SUFFIX)
        workflow.run()
        # Check local directory was created properly
        session_dir = os.path.join(
            self.BASE_DIR, str(self.PROJECT_ID), str(self.SUBJECT_ID),
            str(self.SESSION_ID))
        self.assertEqual(sorted(os.listdir(session_dir)),
                         ['sink1.nii.gz', 'sink3.nii.gz', 'sink4.nii.gz',
                          'source1.nii.gz', 'source2.nii.gz',
                          'source3.nii.gz', 'source4.nii.gz'])

    def test_summary(self):
        # Create working dirs
        # Create LocalSource node
        archive = LocalArchive(base_dir=self.BASE_DIR)
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
            source, 'source1' + LocalSource.OUTPUT_SUFFIX,
            subject_sink, 'sink1' + LocalSink.INPUT_SUFFIX)
        workflow.connect(
            source, 'source2' + LocalSource.OUTPUT_SUFFIX,
            project_sink, 'sink2' + LocalSink.INPUT_SUFFIX)
        workflow.run()
        # Check local summary directories were created properly
        subject_dir = os.path.join(
            self.BASE_DIR, str(self.PROJECT_ID), str(self.SUBJECT_ID),
            SUBJECT_SUMMARY_NAME)
        self.assertEqual(sorted(os.listdir(subject_dir)),
                         ['sink1.nii.gz'])
        project_dir = os.path.join(
            self.BASE_DIR, str(self.PROJECT_ID), PROJECT_SUMMARY_NAME)
        self.assertEqual(sorted(os.listdir(project_dir)),
                         ['sink2.nii.gz'])
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
                               'sink1' + LocalSource.OUTPUT_SUFFIX,
                               reloadsink,
                               'resink1' + LocalSink.INPUT_SUFFIX)
        reloadworkflow.connect(reloadsource,
                               'sink2' + LocalSource.OUTPUT_SUFFIX,
                               reloadsink,
                               'resink2' + LocalSink.INPUT_SUFFIX)
        reloadworkflow.run()
        session_dir = os.path.join(
            self.BASE_DIR, str(self.PROJECT_ID), str(self.SUBJECT_ID),
            str(self.SESSION_ID))
        self.assertEqual(sorted(os.listdir(session_dir)),
                         ['resink1.nii.gz', 'resink2.nii.gz',
                          'source1.nii.gz', 'source2.nii.gz',
                          'source3.nii.gz', 'source4.nii.gz'])
