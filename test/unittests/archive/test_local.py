import os
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from nianalysis.archive.local import LocalArchive
from nianalysis.data_formats import nifti_gz_format
from nianalysis.dataset import Dataset, DatasetSpec, Field, FieldSpec
import logging
from nianalysis.utils import PATH_SUFFIX
from nianalysis.testing import BaseTestCase

logger = logging.getLogger('NiAnalysis')


def dummy_pipeline():
    pass


class TestLocalArchive(BaseTestCase):

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
        sink_files = [DatasetSpec('sink1', nifti_gz_format,
                                  pipeline=dummy_pipeline),
                      DatasetSpec('sink3', nifti_gz_format,
                                  pipeline=dummy_pipeline),
                      DatasetSpec('sink4', nifti_gz_format,
                                  pipeline=dummy_pipeline)]
        inputnode = pe.Node(IdentityInterface(['subject_id', 'visit_id']),
                            'inputnode')
        inputnode.inputs.subject_id = self.SUBJECT
        inputnode.inputs.visit_id = self.VISIT
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
        workflow.connect(inputnode, 'visit_id', source, 'visit_id')
        workflow.connect(inputnode, 'subject_id', sink, 'subject_id')
        workflow.connect(inputnode, 'visit_id', sink, 'visit_id')
        for source_file in source_files:
            if not source_file.name.endswith('2'):
                source_name = source_file.name
                sink_name = source_name.replace('source', 'sink')
                workflow.connect(
                    source, source_name + PATH_SUFFIX,
                    sink, sink_name + PATH_SUFFIX)
        workflow.run()
        # Check local directory was created properly
        self.assertEqual(sorted(os.listdir(self.session_dir)),
                         [self.STUDY_NAME + '_sink1.nii.gz',
                          self.STUDY_NAME + '_sink3.nii.gz',
                          self.STUDY_NAME + '_sink4.nii.gz',
                          'source1.nii.gz', 'source2.nii.gz',
                          'source3.nii.gz', 'source4.nii.gz'])

    def test_fields_roundtrip(self):
        archive = LocalArchive(base_dir=self.ARCHIVE_PATH)
        sink = archive.sink(self.name,
                            output_fields=[
                                Field('field1', int, processed=True),
                                Field('field2', float, processed=True),
                                Field('field3', str, processed=True)],
                            name='fields_sink',
                            study_name='test')
        sink.inputs.field1_field = field1 = 1
        sink.inputs.field2_field = field2 = 2.0
        sink.inputs.field3_field = field3 = '3'
        sink.inputs.subject_id = self.SUBJECT
        sink.inputs.visit_id = self.VISIT
        sink.inputs.description = "Test sink of fields"
        sink.inputs.name = 'test_sink'
        sink.run()
        source = archive.source(
            self.name,
            input_fields=[
                FieldSpec('field1', int, pipeline=dummy_pipeline),
                FieldSpec('field2', float, pipeline=dummy_pipeline),
                FieldSpec('field3', str, pipeline=dummy_pipeline)],
            name='fields_source',
            study_name='test')
        source.inputs.visit_id = self.VISIT
        source.inputs.subject_id = self.SUBJECT
        source.inputs.description = "Test source of fields"
        source.inputs.name = 'test_source'
        results = source.run()
        self.assertEqual(results.outputs.field1_field, field1)
        self.assertEqual(results.outputs.field2_field, field2)
        self.assertEqual(results.outputs.field3_field, field3)

    def test_summary(self):
        # Create working dirs
        # Create LocalSource node
        archive = LocalArchive(base_dir=self.ARCHIVE_PATH)
        # TODO: Should test out other file formats as well.
        source_files = [Dataset('source1', nifti_gz_format),
                        Dataset('source2', nifti_gz_format),
                        Dataset('source3', nifti_gz_format)]
        inputnode = pe.Node(IdentityInterface(['subject_id', 'visit_id']),
                            'inputnode')
        inputnode.inputs.subject_id = self.SUBJECT
        inputnode.inputs.visit_id = self.VISIT
        source = archive.source(self.name, source_files)
        # Test subject sink
        subject_sink_files = [DatasetSpec('sink1', nifti_gz_format,
                                          multiplicity='per_subject',
                                          pipeline=dummy_pipeline)]
        subject_sink = archive.sink(self.name,
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
        visit_sink = archive.sink(self.name,
                                      visit_sink_files,
                                      multiplicity='per_visit',
                                      study_name=self.SUMMARY_STUDY_NAME)
        visit_sink.inputs.name = 'visit_summary'
        visit_sink.inputs.description = (
            "Tests the sinking of visit-wide datasets")
        # Test project sink
        project_sink_files = [DatasetSpec('sink3', nifti_gz_format,
                                          multiplicity='per_project',
                                          pipeline=dummy_pipeline)]
        project_sink = archive.sink(self.name,
                                    project_sink_files,
                                    multiplicity='per_project',
                                    study_name=self.SUMMARY_STUDY_NAME)

        project_sink.inputs.name = 'project_summary'
        project_sink.inputs.description = (
            "Tests the sinking of project-wide datasets")
        # Create workflow connecting them together
        workflow = pe.Workflow('summary_unittest', base_dir=self.work_dir)
        workflow.add_nodes((source, subject_sink, visit_sink,
                            project_sink))
        workflow.connect(inputnode, 'subject_id', source, 'subject_id')
        workflow.connect(inputnode, 'visit_id', source, 'visit_id')
        workflow.connect(inputnode, 'subject_id', subject_sink, 'subject_id')
        workflow.connect(inputnode, 'visit_id', visit_sink, 'visit_id')
        workflow.connect(
            source, 'source1' + PATH_SUFFIX,
            subject_sink, 'sink1' + PATH_SUFFIX)
        workflow.connect(
            source, 'source2' + PATH_SUFFIX,
            visit_sink, 'sink2' + PATH_SUFFIX)
        workflow.connect(
            source, 'source3' + PATH_SUFFIX,
            project_sink, 'sink3' + PATH_SUFFIX)
        workflow.run()
        # Check local summary directories were created properly
        subject_dir = self.get_session_dir(multiplicity='per_subject')
        self.assertEqual(sorted(os.listdir(subject_dir)),
                         [self.SUMMARY_STUDY_NAME + '_sink1.nii.gz'])
        visit_dir = self.get_session_dir(multiplicity='per_visit')
        self.assertEqual(sorted(os.listdir(visit_dir)),
                         [self.SUMMARY_STUDY_NAME + '_sink2.nii.gz'])
        project_dir = self.get_session_dir(multiplicity='per_project')
        self.assertEqual(sorted(os.listdir(project_dir)),
                         [self.SUMMARY_STUDY_NAME + '_sink3.nii.gz'])
        # Reload the data from the summary directories
        reloadinputnode = pe.Node(IdentityInterface(['subject_id',
                                                     'visit_id']),
                                  'reload_inputnode')
        reloadinputnode.inputs.subject_id = self.SUBJECT
        reloadinputnode.inputs.visit_id = self.VISIT
        reloadsource = archive.source(
            self.name,
            (source_files + subject_sink_files + visit_sink_files +
             project_sink_files),
            name='reload_source',
            study_name=self.SUMMARY_STUDY_NAME)
        reloadsink = archive.sink(self.name,
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
        reloadworkflow.connect(reloadsource,
                               'sink1' + PATH_SUFFIX,
                               reloadsink,
                               'resink1' + PATH_SUFFIX)
        reloadworkflow.connect(reloadsource,
                               'sink2' + PATH_SUFFIX,
                               reloadsink,
                               'resink2' + PATH_SUFFIX)
        reloadworkflow.connect(reloadsource,
                               'sink3' + PATH_SUFFIX,
                               reloadsink,
                               'resink3' + PATH_SUFFIX)
        reloadworkflow.run()
        self.assertEqual(sorted(os.listdir(self.session_dir)),
                         [self.SUMMARY_STUDY_NAME + '_resink1.nii.gz',
                          self.SUMMARY_STUDY_NAME + '_resink2.nii.gz',
                          self.SUMMARY_STUDY_NAME + '_resink3.nii.gz',
                          'source1.nii.gz', 'source2.nii.gz',
                          'source3.nii.gz', 'source4.nii.gz'])
