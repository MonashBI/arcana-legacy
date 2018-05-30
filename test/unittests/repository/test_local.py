from future import standard_library
standard_library.install_aliases()
import os
from unittest import TestCase
import tempfile
import os.path as op
import shutil
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from arcana.repository.local import (
    LocalSource, LocalRepository, FIELDS_FNAME)
from arcana.data_format import text_format
from arcana.study import Study, StudyMetaClass
from arcana.runner import LinearRunner
from arcana.dataset import (
    DatasetMatch, Dataset, DatasetSpec, Field, FieldSpec)
from arcana.utils import PATH_SUFFIX
from arcana.testing import BaseTestCase, BaseMultiSubjectTestCase
from arcana.repository import Project, Subject, Session, Visit
from future.utils import PY2
from future.utils import with_metaclass
if PY2:
    import pickle as pkl  # @UnusedImport
else:
    import pickle as pkl  # @Reimport


class DummyStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        DatasetSpec('source1', text_format),
        DatasetSpec('source2', text_format),
        DatasetSpec('source3', text_format),
        DatasetSpec('source4', text_format,
                    optional=True),
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
        DatasetSpec('resink3', text_format, 'dummy_pipeline')]

    def dummy_pipeline(self):
        pass


class TestLocalRepository(BaseTestCase):

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
        source = self.repository.source(source_files,
                                     study_name=self.STUDY_NAME)
        sink = self.repository.sink(sink_files, study_name=self.STUDY_NAME)
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
                         [self.STUDY_NAME + '_sink1.txt',
                          self.STUDY_NAME + '_sink3.txt',
                          self.STUDY_NAME + '_sink4.txt',
                          'source1.txt', 'source2.txt',
                          'source3.txt', 'source4.txt'])

    def test_fields_roundtrip(self):
        repository = LocalRepository(base_dir=self.project_dir)
        sink = repository.sink(
            outputs=[
                FieldSpec('field1', int, 'pipeline'),
                FieldSpec('field2', float, 'pipeline'),
                FieldSpec('field3', str, 'pipeline')],
            name='fields_sink',
            study_name='test')
        sink.inputs.field1_field = field1 = 1
        sink.inputs.field2_field = field2 = 2.0
        sink.inputs.field3_field = field3 = '3'
        sink.inputs.subject_id = self.SUBJECT
        sink.inputs.visit_id = self.VISIT
        sink.inputs.desc = "Test sink of fields"
        sink.inputs.name = 'test_sink'
        sink.run()
        source = repository.source(
            inputs=[
                FieldSpec('field1', int, 'dummy_pipeline'),
                FieldSpec('field2', float, 'dummy_pipeline'),
                FieldSpec('field3', str, 'dummy_pipeline')],
            name='fields_source',
            study_name='test')
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
        subject_sink = self.repository.sink(subject_sink_files,
                                         frequency='per_subject',
                                         study_name=self.SUMMARY_STUDY_NAME)
        subject_sink.inputs.name = 'subject_summary'
        subject_sink.inputs.desc = (
            "Tests the sinking of subject-wide datasets")
        # Test visit sink
        visit_sink_files = [study.spec('visit_sink')]
        visit_sink = self.repository.sink(visit_sink_files,
                                       frequency='per_visit',
                                       study_name=self.SUMMARY_STUDY_NAME)
        visit_sink.inputs.name = 'visit_summary'
        visit_sink.inputs.desc = (
            "Tests the sinking of visit-wide datasets")
        # Test project sink
        project_sink_files = [
            study.spec('project_sink')]
        project_sink = self.repository.sink(project_sink_files,
                                         frequency='per_project',
                                         study_name=self.SUMMARY_STUDY_NAME)

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


class TestProjectInfo(BaseMultiSubjectTestCase):
    """
    This unittest tests out that extracting the existing scans and
    fields in a project returned in a Project object.
    """

    DATASET_CONTENTS = {'ones': 1, 'tens': 10, 'hundreds': 100,
                        'thousands': 1000}

    def get_tree(self, repository, set_ids=False):
        sessions = [
            Session(
                'subject1', 'visit1', datasets=[
                    Dataset('hundreds', text_format,
                            subject_id='subject1', visit_id='visit1',
                            repository=repository),
                    Dataset('ones', text_format,
                            subject_id='subject1', visit_id='visit1',
                            repository=repository),
                    Dataset('tens', text_format,
                            subject_id='subject1', visit_id='visit1',
                            repository=repository)],
                fields=[
                    Field('a', value=1,
                          subject_id='subject1', visit_id='visit1',
                          repository=repository),
                    Field('b', value=10,
                          subject_id='subject1', visit_id='visit1',
                          repository=repository),
                    Field('d', value=42.42,
                          subject_id='subject1', visit_id='visit1',
                          repository=repository)]),
            Session(
                'subject1', 'visit2', datasets=[
                    Dataset('ones', text_format,
                            subject_id='subject1', visit_id='visit2',
                            repository=repository),
                    Dataset('tens', text_format,
                            subject_id='subject1', visit_id='visit2',
                            repository=repository)],
                fields=[
                    Field('a', value=2,
                          subject_id='subject1', visit_id='visit2',
                          repository=repository),
                    Field('c', value='van',
                          subject_id='subject1', visit_id='visit2',
                          repository=repository)]),
            Session(
                'subject2', 'visit1', datasets=[
                    Dataset('ones', text_format,
                            subject_id='subject2', visit_id='visit1',
                            repository=repository),
                    Dataset('tens', text_format,
                            subject_id='subject2', visit_id='visit1',
                            repository=repository)],
                fields=[]),
            Session(
                'subject2', 'visit2', datasets=[
                    Dataset('ones', text_format,
                            subject_id='subject2', visit_id='visit2',
                            repository=repository),
                    Dataset('tens', text_format,
                            subject_id='subject2', visit_id='visit2',
                            repository=repository)],
                fields=[
                    Field('a', value=22,
                          subject_id='subject2', visit_id='visit2',
                          repository=repository),
                    Field('b', value=220,
                          subject_id='subject2', visit_id='visit2',
                          repository=repository),
                    Field('c', value='buggy',
                          subject_id='subject2', visit_id='visit2',
                          repository=repository)])]
        project = Project(
            subjects=[
                Subject(
                    'subject1', sessions=[s for s in sessions
                                          if s.subject_id == 'subject1'],
                    datasets=[
                        Dataset('ones', text_format,
                                frequency='per_subject',
                                subject_id='subject1',
                                repository=repository),
                        Dataset('tens', text_format,
                                frequency='per_subject',
                                subject_id='subject1',
                                repository=repository)],
                    fields=[
                        Field('e', value=4.44444,
                              frequency='per_subject',
                              subject_id='subject1',
                              repository=repository)]),
                Subject(
                    'subject2', sessions=[s for s in sessions
                                          if s.subject_id == 'subject2'],
                    datasets=[
                        Dataset('ones', text_format,
                                frequency='per_subject',
                                subject_id='subject2',
                                repository=repository),
                        Dataset('tens', text_format,
                                frequency='per_subject',
                                subject_id='subject2',
                                repository=repository)],
                    fields=[
                        Field('e', value=3.33333,
                              frequency='per_subject',
                              subject_id='subject2',
                              repository=repository)])],
            visits=[
                Visit(
                    'visit1', sessions=[s for s in sessions
                                        if s.visit_id == 'visit1'],
                    datasets=[
                        Dataset('ones', text_format,
                                frequency='per_visit',
                                visit_id='visit1',
                                repository=repository)],
                    fields=[
                        Field('f', value='dog',
                              frequency='per_visit',
                              visit_id='visit1',
                              repository=repository)]),
                Visit(
                    'visit2', sessions=[s for s in sessions
                                        if s.visit_id == 'visit2'],
                    datasets=[],
                    fields=[
                        Field('f', value='cat',
                              frequency='per_visit',
                              visit_id='visit2',
                              repository=repository)])],
            datasets=[
                Dataset('ones', text_format,
                        frequency='per_project',
                        repository=repository)],
            fields=[
                Field('g', value=100,
                      frequency='per_project',
                      repository=repository)])
        if set_ids:  # For xnat repository
            for dataset in project.datasets:
                dataset._id = dataset.name
            for visit in project.visits:
                for dataset in visit.datasets:
                    dataset._id = dataset.name
            for subject in project.subjects:
                for dataset in subject.datasets:
                    dataset._id = dataset.name
                for session in subject.sessions:
                    for dataset in session.datasets:
                        dataset._id = dataset.name
        return project

    @property
    def input_tree(self):
        return self.get_tree(self.local_repository)

    def test_project_info(self):
        # Add hidden file to local repository at project and subject
        # levels to test ignore
        a_subj_dir = os.listdir(self.project_dir)[0]
        open(op.join(op.join(self.project_dir, '.DS_Store')),
             'w').close()
        open(op.join(op.join(self.project_dir, a_subj_dir,
                                       '.DS_Store')), 'w').close()
        tree = self.repository.get_tree()
        self.assertEqual(
            tree, self.local_tree,
            "Generated project doesn't match reference:{}"
            .format(tree.find_mismatch(self.local_tree)))


class TestLocalInterfacePickle(TestCase):

    datasets = [DatasetSpec('a', text_format)]
    fields = [FieldSpec('b', int)]

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.pkl_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)
        shutil.rmtree(self.pkl_dir)

    def test_source(self):
        source = LocalSource('a_study', self.datasets, self.fields,
                             base_dir=self.tmp_dir)
        fname = op.join(self.pkl_dir, 'source.pkl')
        with open(fname, 'wb') as f:
            pkl.dump(source, f)
        with open(fname, 'rb') as f:
            re_source = pkl.load(f)
        self.assertEqual(source, re_source)
