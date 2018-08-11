from future import standard_library
standard_library.install_aliases()
import os  # @IgnorePep8
import os.path as op  # @IgnorePep8
from arcana.data.file_format.standard import text_format  # @IgnorePep8
from arcana.study import Study, StudyMetaClass  # @IgnorePep8
from arcana.data import (  # @IgnorePep8
    Fileset, FilesetSpec, Field)  # @IgnorePep8
from arcana.testing import BaseMultiSubjectTestCase  # @IgnorePep8
from arcana.repository import Tree, Subject, Session, Visit  # @IgnorePep8
from future.utils import with_metaclass  # @IgnorePep8
from arcana.testing import BaseTestCase  # @IgnorePep8


class DummyStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        FilesetSpec('source1', text_format),
        FilesetSpec('source2', text_format),
        FilesetSpec('source3', text_format),
        FilesetSpec('source4', text_format,
                    optional=True),
        FilesetSpec('sink1', text_format, 'dummy_pipeline'),
        FilesetSpec('sink3', text_format, 'dummy_pipeline'),
        FilesetSpec('sink4', text_format, 'dummy_pipeline'),
        FilesetSpec('subject_sink', text_format, 'dummy_pipeline',
                    frequency='per_subject'),
        FilesetSpec('visit_sink', text_format, 'dummy_pipeline',
                    frequency='per_visit'),
        FilesetSpec('project_sink', text_format, 'dummy_pipeline',
                    frequency='per_study'),
        FilesetSpec('resink1', text_format, 'dummy_pipeline'),
        FilesetSpec('resink2', text_format, 'dummy_pipeline'),
        FilesetSpec('resink3', text_format, 'dummy_pipeline')]

    def dummy_pipeline(self):
        pass


class TestSimpleRepository(BaseTestCase):

    STUDY_NAME = 'local_repo'
    INPUT_DATASETS = {'source1': '1',
                      'source2': '2',
                      'source3': '3',
                      'source4': '4'}

    def test_get_fileset(self):
        pass


class TestLocalProjectInfo(BaseMultiSubjectTestCase):
    """
    This unittest tests out that extracting the existing scans and
    fields in a project returned in a Tree object.
    """

    DATASET_CONTENTS = {'ones': 1, 'tens': 10, 'hundreds': 100,
                        'thousands': 1000}

    def get_tree(self, repository, set_ids=False):
        sessions = [
            Session(
                'subject1', 'visit1', filesets=[
                    Fileset('hundreds', text_format,
                            subject_id='subject1', visit_id='visit1',
                            repository=repository),
                    Fileset('ones', text_format,
                            subject_id='subject1', visit_id='visit1',
                            repository=repository),
                    Fileset('tens', text_format,
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
                'subject1', 'visit2', filesets=[
                    Fileset('ones', text_format,
                            subject_id='subject1', visit_id='visit2',
                            repository=repository),
                    Fileset('tens', text_format,
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
                'subject2', 'visit1', filesets=[
                    Fileset('ones', text_format,
                            subject_id='subject2', visit_id='visit1',
                            repository=repository),
                    Fileset('tens', text_format,
                            subject_id='subject2', visit_id='visit1',
                            repository=repository)],
                fields=[]),
            Session(
                'subject2', 'visit2', filesets=[
                    Fileset('ones', text_format,
                            subject_id='subject2', visit_id='visit2',
                            repository=repository),
                    Fileset('tens', text_format,
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
        project = Tree(
            subjects=[
                Subject(
                    'subject1', sessions=[s for s in sessions
                                          if s.subject_id == 'subject1'],
                    filesets=[
                        Fileset('ones', text_format,
                                frequency='per_subject',
                                subject_id='subject1',
                                repository=repository),
                        Fileset('tens', text_format,
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
                    filesets=[
                        Fileset('ones', text_format,
                                frequency='per_subject',
                                subject_id='subject2',
                                repository=repository),
                        Fileset('tens', text_format,
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
                    filesets=[
                        Fileset('ones', text_format,
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
                    filesets=[],
                    fields=[
                        Field('f', value='cat',
                              frequency='per_visit',
                              visit_id='visit2',
                              repository=repository)])],
            filesets=[
                Fileset('ones', text_format,
                        frequency='per_study',
                        repository=repository)],
            fields=[
                Field('g', value=100,
                      frequency='per_study',
                      repository=repository)])
        if set_ids:  # For xnat repository
            for fileset in project.filesets:
                fileset._id = fileset.name
            for visit in project.visits:
                for fileset in visit.filesets:
                    fileset._id = fileset.name
            for subject in project.subjects:
                for fileset in subject.filesets:
                    fileset._id = fileset.name
                for session in subject.sessions:
                    for fileset in session.filesets:
                        fileset._id = fileset.name
        return project

    @property
    def input_tree(self):
        return self.get_tree(self.simple_repository)

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
            tree, self.simple_tree,
            "Generated project doesn't match reference:{}"
            .format(tree.find_mismatch(self.simple_tree)))
