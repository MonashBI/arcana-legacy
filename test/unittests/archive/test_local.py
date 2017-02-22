import os.path
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from nianalysis.archive.local import (
    LocalArchive, SUBJECT_SUMMARY_NAME, PROJECT_SUMMARY_NAME)
from nianalysis.data_formats import nifti_gz_format
from nianalysis.dataset import Dataset
import logging
from nianalysis.utils import INPUT_SUFFIX, OUTPUT_SUFFIX
from nianalysis.testing import PipelineTestCase

logger = logging.getLogger('NiAnalysis')


class TestLocalArchive(PipelineTestCase):

    STUDY_NAME = 'astudy'
    SUMMARY_STUDY_NAME = 'asummary'

    def test_archive_roundtrip(self):
        # Create working dirs
        # Create LocalSource node
        archive = LocalArchive(base_dir=self.ARCHIVE_PATH)
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
        inputnode.inputs.subject_id = self.SUBJECT
        inputnode.inputs.session_id = self.SESSION
        source = archive.source(self.name, source_files,
                                study_name=self.STUDY_NAME)
        sink = archive.sink(self.name, sink_files, study_name=self.STUDY_NAME)
        sink.inputs.name = 'archive_sink'
        sink.inputs.description = (
            "A test session created by archive roundtrip unittest")
        # Create workflow connecting them together
        workflow = pe.Workflow('source_sink_unit_test', base_dir=self.work_dir)
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
                    source, source_name + OUTPUT_SUFFIX,
                    sink, sink_name + INPUT_SUFFIX)
        workflow.run()
        # Check local directory was created properly
        self.assertEqual(sorted(os.listdir(self.session_dir)),
                         [self.STUDY_NAME + '_sink1.nii.gz',
                          self.STUDY_NAME + '_sink3.nii.gz',
                          self.STUDY_NAME + '_sink4.nii.gz',
                          'source1.nii.gz', 'source2.nii.gz',
                          'source3.nii.gz', 'source4.nii.gz'])

    def test_summary(self):
        # Create working dirs
        # Create LocalSource node
        archive = LocalArchive(base_dir=self.ARCHIVE_PATH)
        # TODO: Should test out other file formats as well.
        source_files = [Dataset('source1', nifti_gz_format),
                        Dataset('source2', nifti_gz_format)]
        inputnode = pe.Node(IdentityInterface(['subject_id', 'session_id']),
                            'inputnode')
        inputnode.inputs.subject_id = self.SUBJECT
        inputnode.inputs.session_id = self.SESSION
        source = archive.source(self.name, source_files)
        subject_sink_files = [Dataset('sink1', nifti_gz_format,
                                      multiplicity='per_subject',
                                      processed=True)]
        subject_sink = archive.sink(self.name,
                                    subject_sink_files,
                                    multiplicity='per_subject',
                                    study_name=self.SUMMARY_STUDY_NAME)
        subject_sink.inputs.name = 'subject_summary'
        subject_sink.inputs.description = (
            "Tests the sinking of subject-wide datasets")
        project_sink_files = [Dataset('sink2', nifti_gz_format,
                                      multiplicity='per_project',
                                      processed=True)]
        project_sink = archive.sink(self.name,
                                    project_sink_files,
                                    multiplicity='per_project',
                                    study_name=self.SUMMARY_STUDY_NAME)

        project_sink.inputs.name = 'project_summary'
        project_sink.inputs.description = (
            "Tests the sinking of project-wide datasets")
        # Create workflow connecting them together
        workflow = pe.Workflow('summary_unittest', base_dir=self.work_dir)
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
        # Check local summary directories were created properly
        subject_dir = self.get_session_dir(multiplicity='per_subject')
        self.assertEqual(sorted(os.listdir(subject_dir)),
                         [self.SUMMARY_STUDY_NAME + '_sink1.nii.gz'])
        project_dir = self.get_session_dir(multiplicity='per_project')
        self.assertEqual(sorted(os.listdir(project_dir)),
                         [self.SUMMARY_STUDY_NAME + '_sink2.nii.gz'])
        # Reload the data from the summary directories
        reloadinputnode = pe.Node(IdentityInterface(['subject_id',
                                                     'session_id']),
                                  'reload_inputnode')
        reloadinputnode.inputs.subject_id = self.SUBJECT
        reloadinputnode.inputs.session_id = self.SESSION
        reloadsource = archive.source(
            self.name,
            source_files + subject_sink_files + project_sink_files,
            name='reload_source',
            study_name=self.SUMMARY_STUDY_NAME)
        reloadsink = archive.sink(self.name,
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
        reloadworkflow.connect(reloadsource,
                               'sink1' + OUTPUT_SUFFIX,
                               reloadsink,
                               'resink1' + INPUT_SUFFIX)
        reloadworkflow.connect(reloadsource,
                               'sink2' + OUTPUT_SUFFIX,
                               reloadsink,
                               'resink2' + INPUT_SUFFIX)
        reloadworkflow.run()
        self.assertEqual(sorted(os.listdir(self.session_dir)),
                         [self.SUMMARY_STUDY_NAME + '_resink1.nii.gz',
                          self.SUMMARY_STUDY_NAME + '_resink2.nii.gz',
                          'source1.nii.gz', 'source2.nii.gz',
                          'source3.nii.gz', 'source4.nii.gz'])
