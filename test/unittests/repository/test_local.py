from future import standard_library
standard_library.install_aliases()
import os  # @IgnorePep8
import os.path as op  # @IgnorePep8
from arcana.dataset.file_format.standard import text_format  # @IgnorePep8
from arcana.study import Study, StudyMetaClass  # @IgnorePep8
from arcana.dataset import (  # @IgnorePep8
    Dataset, DatasetSpec, Field)  # @IgnorePep8
from arcana.testing import BaseMultiSubjectTestCase  # @IgnorePep8
from arcana.repository import Project, Subject, Session, Visit  # @IgnorePep8
from future.utils import with_metaclass  # @IgnorePep8
from arcana.testing import BaseTestCase  # @IgnorePep8


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

    STUDY_NAME = 'local_repo'
    INPUT_DATASETS = {'source1': '1',
                      'source2': '2',
                      'source3': '3',
                      'source4': '4'}

    def test_get_dataset(self):
        pass


class TestLocalProjectInfo(BaseMultiSubjectTestCase):
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
        tree = self.repository.tree()
        self.assertEqual(
            tree, self.local_tree,
            "Generated project doesn't match reference:{}"
            .format(tree.find_mismatch(self.local_tree)))
