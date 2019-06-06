from __future__ import absolute_import
from future.utils import with_metaclass
import os.path as op
import tempfile
import unittest
from arcana.utils.testing import BaseMultiSubjectTestCase
from arcana.repository.xnat import XnatRepo
from arcana.data import (
    InputFilesets, InputFilesetSpec)
from arcana.study import Study, StudyMetaClass
from arcana.data.file_format import text_format
from arcana.repository.tree import Tree, Subject, Session, Visit
from arcana.data import Fileset
import sys
from arcana.utils.testing.xnat import (
    TestMultiSubjectOnXnatMixin, SKIP_ARGS, SERVER)

# Import TestExistingPrereqs study to test it on XNAT
sys.path.insert(0, op.join(op.dirname(__file__), '..', '..', 'study'))
import test_study  # @UnresolvedImport @IgnorePep8
sys.path.pop(0)

# Import test_local to run TestProjectInfo on XNAT using TestOnXnat mixin
sys.path.insert(0, op.join(op.dirname(__file__), '..'))
import test_directory  # @UnresolvedImport @IgnorePep8
sys.path.pop(0)


class TestStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        InputFilesetSpec('fileset1', text_format),
        InputFilesetSpec('fileset2', text_format, optional=True),
        InputFilesetSpec('fileset3', text_format),
        InputFilesetSpec('fileset5', text_format, optional=True)]


class TestExistingPrereqsOnXnat(TestMultiSubjectOnXnatMixin,
                                test_study.TestExistingPrereqs):

    BASE_CLASS = test_study.TestExistingPrereqs

    @unittest.skipIf(*SKIP_ARGS)
    def test_per_session_prereqs(self):
        super(TestExistingPrereqsOnXnat, self).test_per_session_prereqs()


class TestProjectInfo(TestMultiSubjectOnXnatMixin,
                      test_directory.TestDirectoryProjectInfo):

    BASE_CLASS = test_directory.TestDirectoryProjectInfo

    @unittest.skipIf(*SKIP_ARGS)
    def test_project_info(self):
        tree = self.repository.tree()
        for node in tree.nodes():
            for fileset in node.filesets:
                fileset.format = text_format
                # Clear id and format name from regenerated tree
                fileset._id = None
#                 fileset.get()
        ref_tree = self.get_tree(self.repository)  #, sync_with_repo=True)
        self.assertEqual(
            tree, ref_tree,
            "Generated project doesn't match reference:{}"
            .format(tree.find_mismatch(ref_tree)))


class TestXnatCache(TestMultiSubjectOnXnatMixin,
                    BaseMultiSubjectTestCase):

    BASE_CLASS = BaseMultiSubjectTestCase
    STRUCTURE = {
        'subject1': {
            'visit1': ['fileset1', 'fileset2', 'fileset3'],
            'visit2': ['fileset1', 'fileset2', 'fileset3']},
        'subject2': {
            'visit1': ['fileset1', 'fileset2', 'fileset3'],
            'visit2': ['fileset1', 'fileset2', 'fileset3']}}

    DATASET_CONTENTS = {'fileset1': 1,
                        'fileset2': 2,
                        'fileset3': 3}

    @property
    def input_tree(self):
        filesets = []
        for subj_id, visits in list(self.STRUCTURE.items()):
            for visit_id, fileset_names in list(visits.items()):
                filesets.extend(
                    Fileset(d, text_format, subject_id=subj_id,
                            visit_id=visit_id) for d in fileset_names)
        return Tree.construct(self.repository, filesets=filesets)

    @unittest.skipIf(*SKIP_ARGS)
    def test_cache_download(self):
        repository = XnatRepo(
            project_id=self.project,
            server=SERVER,
            cache_dir=tempfile.mkdtemp())
        study = self.create_study(
            TestStudy, 'cache_download',
            inputs=[
                InputFilesets('fileset1', 'fileset1', text_format),
                InputFilesets('fileset3', 'fileset3', text_format)],
            repository=repository)
        study.cache_inputs()
        for subject_id, visits in list(self.STRUCTURE.items()):
            subj_dir = op.join(
                repository.cache_dir, self.project,
                '{}_{}'.format(self.project, subject_id))
            for visit_id in visits:
                sess_dir = op.join(
                    subj_dir,
                    '{}_{}_{}'.format(self.project, subject_id,
                                      visit_id))
                for inpt in study.inputs:
                    self.assertTrue(op.exists(op.join(sess_dir, inpt.name)))

    @property
    def base_name(self):
        return self.name
