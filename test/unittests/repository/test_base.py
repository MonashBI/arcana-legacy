from future import standard_library
standard_library.install_aliases()
import os  # @IgnorePep8
from nipype.pipeline import engine as pe  # @IgnorePep8
from nipype.interfaces.utility import IdentityInterface  # @IgnorePep8
from arcana.repository.local import LocalRepository  # @IgnorePep8
from arcana.dataset.file_format.standard import text_format  # @IgnorePep8
from arcana.runner import LinearRunner  # @IgnorePep8
from arcana.dataset import (  # @IgnorePep8
    DatasetMatch, FieldSpec)  # @IgnorePep8
from arcana.utils import PATH_SUFFIX  # @IgnorePep8
from future.utils import with_metaclass  # @IgnorePep8
from arcana.testing import BaseTestCase  # @IgnorePep8
from arcana.dataset import DatasetSpec  # @IgnorePep8
from arcana.study import Study, StudyMetaClass  # @IgnorePep8
from arcana.repository.local import FIELDS_FNAME  # @IgnorePep8


class DummyStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        DatasetSpec('source1', text_format, optional=True),
        DatasetSpec('source2', text_format, optional=True),
        DatasetSpec('source3', text_format, optional=True),
        DatasetSpec('source4', text_format, optional=True),
        DatasetSpec('sink1', text_format, 'dummy_pipeline'),
        DatasetSpec('sink3', text_format, 'dummy_pipeline'),
        DatasetSpec('sink4', text_format, 'dummy_pipeline'),
        DatasetSpec('subject_sink', text_format, 'dummy_pipeline',
                    frequency='per_subject'),
        DatasetSpec('visit_sink', text_format, 'dummy_pipeline',
                    frequency='per_visit'),
        DatasetSpec('project_sink', text_format, 'dummy_pipeline',
                    frequency='per_project'),
        DatasetSpec('resink1', text_format, 'dummy_pipeline'),
        DatasetSpec('resink2', text_format, 'dummy_pipeline'),
        DatasetSpec('resink3', text_format, 'dummy_pipeline'),
        FieldSpec('field1', int, 'dummy_pipeline'),
        FieldSpec('field2', float, 'dummy_pipeline'),
        FieldSpec('field3', str, 'dummy_pipeline')]

    def dummy_pipeline(self):
        pass


class TestSinkAndSource(BaseTestCase):

    STUDY_NAME = 'astudy'
    SUMMARY_STUDY_NAME = 'asummary'
    INPUT_DATASETS = {'source1': '1',
                      'source2': '2',
                      'source3': '3',
                      'source4': '4'}

    def test_repository_roundtrip(self):
        study = DummyStudy(
            self.STUDY_NAME, self.repository, runner=LinearRunner('a_dir'),
            inputs=[DatasetMatch('source1', text_format, 'source1'),
                    DatasetMatch('source2', text_format, 'source2'),
                    DatasetMatch('source3', text_format, 'source3'),
                    DatasetMatch('source4', text_format, 'source4')])
        # TODO: Should test out other file formats as well.
        source_files = [study.input(n)
                        for n in ('source1', 'source2', 'source3',
                                  'source4')]
        sink_files = [study.spec(n)
                      for n in ('sink1', 'sink3', 'sink4')]
        inputnode = pe.Node(IdentityInterface(['subject_id', 'visit_id']),
                            'inputnode')
        inputnode.inputs.subject_id = self.SUBJECT
        inputnode.inputs.visit_id = self.VISIT
        source = self.repository.source(source_files)
        sink = self.repository.sink(sink_files)
        sink.inputs.name = 'repository_sink'
        sink.inputs.desc = (
            "A test session created by repository roundtrip unittest")
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
        outputs = [
            f for f in sorted(os.listdir(self.session_dir))
            if f != FIELDS_FNAME]
        self.assertEqual(outputs,
                         ['sink1.txt', 'sink3.txt', 'sink4.txt',
                          'source1.txt', 'source2.txt',
                          'source3.txt', 'source4.txt'])

    def test_fields_roundtrip(self):
        STUDY_NAME = 'fields_roundtrip'
        study = DummyStudy(
            STUDY_NAME, self.repository,
            runner=LinearRunner('a_dir'),
            inputs=[])
        sink = self.repository.sink(
            outputs=[
                study.spec('field1'),
                study.spec('field2'),
                study.spec('field3')],
            name='fields_sink')
        sink.inputs.field1_field = field1 = 1
        sink.inputs.field2_field = field2 = 2.0
        sink.inputs.field3_field = field3 = '3'
        sink.inputs.subject_id = self.SUBJECT
        sink.inputs.visit_id = self.VISIT
        sink.inputs.desc = "Test sink of fields"
        sink.inputs.name = 'test_sink'
        sink.run()
        source = self.repository.source(
            inputs=[
                study.spec('field1'),
                study.spec('field2'),
                study.spec('field3')],
            name='fields_source')
        source.inputs.visit_id = self.VISIT
        source.inputs.subject_id = self.SUBJECT
        source.inputs.desc = "Test source of fields"
        source.inputs.name = 'test_source'
        results = source.run()
        self.assertEqual(results.outputs.field1_field, field1)
        self.assertEqual(results.outputs.field2_field, field2)
        self.assertEqual(results.outputs.field3_field, field3)

    def test_summary(self):
        study = DummyStudy(
            self.SUMMARY_STUDY_NAME, self.repository, LinearRunner('ad'),
            inputs=[DatasetMatch('source1', text_format, 'source1'),
                    DatasetMatch('source2', text_format, 'source2'),
                    DatasetMatch('source3', text_format, 'source3')])
        # TODO: Should test out other file formats as well.
        source_files = [study.input(n)
                        for n in ('source1', 'source2', 'source3')]
        inputnode = pe.Node(
            IdentityInterface(['subject_id', 'visit_id']), 'inputnode')
        inputnode.inputs.subject_id = self.SUBJECT
        inputnode.inputs.visit_id = self.VISIT
        source = self.repository.source(source_files)
        # Test subject sink
        subject_sink_files = [
            study.spec('subject_sink')]
        subject_sink = self.repository.sink(
            subject_sink_files, frequency='per_subject')
        subject_sink.inputs.name = 'subject_summary'
        subject_sink.inputs.desc = (
            "Tests the sinking of subject-wide datasets")
        # Test visit sink
        visit_sink_files = [study.spec('visit_sink')]
        visit_sink = self.repository.sink(visit_sink_files,
                                          frequency='per_visit')
        visit_sink.inputs.name = 'visit_summary'
        visit_sink.inputs.desc = (
            "Tests the sinking of visit-wide datasets")
        # Test project sink
        project_sink_files = [
            study.spec('project_sink')]
        project_sink = self.repository.sink(project_sink_files,
                                            frequency='per_project')

        project_sink.inputs.name = 'project_summary'
        project_sink.inputs.desc = (
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
            subject_sink, 'subject_sink' + PATH_SUFFIX)
        workflow.connect(
            source, 'source2' + PATH_SUFFIX,
            visit_sink, 'visit_sink' + PATH_SUFFIX)
        workflow.connect(
            source, 'source3' + PATH_SUFFIX,
            project_sink, 'project_sink' + PATH_SUFFIX)
        workflow.run()
        # Check local summary directories were created properly
        subject_dir = self.get_session_dir(frequency='per_subject')
        self.assertEqual(sorted(os.listdir(subject_dir)),
                         [self.SUMMARY_STUDY_NAME + '_subject_sink.txt'])
        visit_dir = self.get_session_dir(frequency='per_visit')
        self.assertEqual(sorted(os.listdir(visit_dir)),
                         [self.SUMMARY_STUDY_NAME + '_visit_sink.txt'])
        project_dir = self.get_session_dir(frequency='per_project')
        self.assertEqual(sorted(os.listdir(project_dir)),
                         [self.SUMMARY_STUDY_NAME + '_project_sink.txt'])
        # Reload the data from the summary directories
        reloadinputnode = pe.Node(IdentityInterface(['subject_id',
                                                     'visit_id']),
                                  'reload_inputnode')
        reloadinputnode.inputs.subject_id = self.SUBJECT
        reloadinputnode.inputs.visit_id = self.VISIT
        reloadsource = self.repository.source(
            (source_files + subject_sink_files + visit_sink_files +
             project_sink_files),
            name='reload_source',
            study_name=self.SUMMARY_STUDY_NAME)
        reloadsink = self.repository.sink(
            [study.spec(n)
             for n in ('resink1', 'resink2', 'resink3')],
            study_name=self.SUMMARY_STUDY_NAME)
        reloadsink.inputs.name = 'reload_summary'
        reloadsink.inputs.desc = (
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
                               'subject_sink' + PATH_SUFFIX,
                               reloadsink,
                               'resink1' + PATH_SUFFIX)
        reloadworkflow.connect(reloadsource,
                               'visit_sink' + PATH_SUFFIX,
                               reloadsink,
                               'resink2' + PATH_SUFFIX)
        reloadworkflow.connect(reloadsource,
                               'project_sink' + PATH_SUFFIX,
                               reloadsink,
                               'resink3' + PATH_SUFFIX)
        reloadworkflow.run()
        outputs = [
            f for f in sorted(os.listdir(self.session_dir))
            if f != FIELDS_FNAME]
        self.assertEqual(outputs,
                         [self.SUMMARY_STUDY_NAME + '_resink1.txt',
                          self.SUMMARY_STUDY_NAME + '_resink2.txt',
                          self.SUMMARY_STUDY_NAME + '_resink3.txt',
                          'source1.txt', 'source2.txt',
                          'source3.txt', 'source4.txt'])
