from future import standard_library
standard_library.install_aliases()
import os  # @IgnorePep8
import os.path as op  # @IgnorePep8
from arcana.data.file_format import text_format  # @IgnorePep8
from arcana.study import Study, StudyMetaClass  # @IgnorePep8
from arcana.data import (  # @IgnorePep8
    Fileset, InputFilesetSpec, FilesetSpec, Field)  # @IgnorePep8
from arcana.utils.testing import BaseMultiSubjectTestCase  # @IgnorePep8
from arcana.repository import Tree  # @IgnorePep8
from future.utils import with_metaclass  # @IgnorePep8
from arcana.utils.testing import BaseTestCase  # @IgnorePep8
from arcana.data.file_format import FileFormat  # @IgnorePep8


# A dummy format that contains a header
with_header_format = FileFormat(name='with_header', extension='.whf',
                                aux_files={'header': '.hdr'})


class DummyStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        InputFilesetSpec('source1', text_format),
        InputFilesetSpec('source2', text_format),
        InputFilesetSpec('source3', text_format),
        InputFilesetSpec('source4', text_format,
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


class TestBasicRepo(BaseTestCase):

    STUDY_NAME = 'local_repo'
    INPUT_FILESETS = {'source1': '1',
                      'source2': '2',
                      'source3': '3',
                      'source4': '4'}

    def test_get_fileset(self):
        pass


class TestDirectoryProjectInfo(BaseMultiSubjectTestCase):
    """
    This unittest tests out that extracting the existing scans and
    fields in a project returned in a Tree object.
    """

    DATASET_CONTENTS = {'ones': 1, 'tens': 10, 'hundreds': 100,
                        'thousands': 1000, 'with_header': {'.': 'main',
                                                           'header': 'header'}}

    def get_tree(self, repo, sync_with_repo=False):  # @ReservedAssignment @IgnorePep8
        filesets = [
            # Subject1
            Fileset('ones', text_format,
                    frequency='per_subject',
                    subject_id='subject1',
                    resource_name='text',
                    repository=repo),
            Fileset('tens', text_format,
                    frequency='per_subject',
                    subject_id='subject1',
                    resource_name='text',
                    repository=repo),
            # subject1/visit1
            Fileset('hundreds', text_format,
                    subject_id='subject1', visit_id='visit1',
                    resource_name='text',
                    repository=repo),
            Fileset('ones', text_format,
                    subject_id='subject1', visit_id='visit1',
                    resource_name='text',
                    repository=repo),
            Fileset('tens', text_format,
                    subject_id='subject1', visit_id='visit1',
                    resource_name='text',
                    repository=repo),
            Fileset('with_header', text_format,
                    frequency='per_session',
                    subject_id='subject1', visit_id='visit1',
                    resource_name='text',
                    repository=repo),
            # subject1/visit2
            Fileset('ones', text_format,
                    subject_id='subject1', visit_id='visit2',
                    resource_name='text',
                    repository=repo),
            Fileset('tens', text_format,
                    subject_id='subject1', visit_id='visit2',
                    resource_name='text',
                    repository=repo),
            # Subject 2
            Fileset('ones', text_format,
                    frequency='per_subject',
                    subject_id='subject2',
                    resource_name='text',
                    repository=repo),
            Fileset('tens', text_format,
                    frequency='per_subject',
                    subject_id='subject2',
                    resource_name='text',
                    repository=repo),
            # subject2/visit1
            Fileset('ones', text_format,
                    subject_id='subject2', visit_id='visit1',
                    resource_name='text',
                    repository=repo),
            Fileset('tens', text_format,
                    subject_id='subject2', visit_id='visit1',
                    resource_name='text',
                    repository=repo),
            # subject2/visit2
            Fileset('ones', text_format,
                    subject_id='subject2', visit_id='visit2',
                    resource_name='text',
                    repository=repo),
            Fileset('tens', text_format,
                    subject_id='subject2', visit_id='visit2',
                    resource_name='text',
                    repository=repo),
            # Visit 1
            Fileset('ones', text_format,
                    frequency='per_visit',
                    visit_id='visit1',
                    resource_name='text',
                    repository=repo),
            # Study
            Fileset('ones', text_format,
                    frequency='per_study',
                    resource_name='text',
                    repository=repo)]
        fields = [
            # Subject 2
            Field('e', value=3.33333,
                  frequency='per_subject',
                  subject_id='subject2',
                  repository=repo),
            # subject2/visit2
            Field('a', value=22,
                  subject_id='subject2', visit_id='visit2',
                  repository=repo),
            Field('b', value=220,
                  subject_id='subject2', visit_id='visit2',
                  repository=repo),
            Field('c', value='buggy',
                  subject_id='subject2', visit_id='visit2',
                  repository=repo),
            # Subject1
            Field('e', value=4.44444,
                  frequency='per_subject',
                  subject_id='subject1',
                  repository=repo),
            # subject1/visit1
            Field('a', value=1,
                  subject_id='subject1', visit_id='visit1',
                  repository=repo),
            Field('b', value=10,
                  subject_id='subject1', visit_id='visit1',
                  repository=repo),
            Field('d', value=42.42,
                  subject_id='subject1', visit_id='visit1',
                  repository=repo),
            # subject1/visit2
            Field('a', value=2,
                  subject_id='subject1', visit_id='visit2',
                  repository=repo),
            Field('c', value='van',
                  subject_id='subject1', visit_id='visit2',
                  repository=repo),
            # Visit 1
            Field('f', value='dog',
                  frequency='per_visit',
                  visit_id='visit1',
                  repository=repo),
            # Visit 2
            Field('f', value='cat',
                  frequency='per_visit',
                  visit_id='visit2',
                  repository=repo),
            # Study
            Field('g', value=100,
                  frequency='per_study',
                  repository=repo)]
        # Set URI and IDs if necessary for repository type
        if sync_with_repo:
            for fileset in filesets:
                fileset.get()
            for field in fields:
                field.get()
        tree = Tree.construct(self.repository, filesets, fields)
        return tree

    @property
    def input_tree(self):
        return self.get_tree(self.local_repository)

    def test_project_info(self):
        # Add hidden file (i.e. starting with '.') to local repository at
        # project and subject levels to test ignore functionality
        a_subj_dir = os.listdir(self.project_dir)[0]
        open(op.join(op.join(self.project_dir, '.DS_Store')),
             'w').close()
        open(op.join(self.project_dir, a_subj_dir, '.DS_Store'), 'w').close()
        tree = self.repository.tree()
        for node in tree.nodes():
            for fileset in node.filesets:
                fileset.format = text_format
                fileset._resource_name = 'text'
        self.assertEqual(
            tree, self.local_tree,
            "Generated project doesn't match reference:{}"
            .format(tree.find_mismatch(self.local_tree)))
