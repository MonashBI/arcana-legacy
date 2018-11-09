from __future__ import absolute_import
from future.utils import with_metaclass
import os.path as op
import tempfile
import unittest
from arcana.testing import BaseMultiSubjectTestCase
from arcana.repository.xnat import XnatRepository
from arcana.data import (
    FilesetSelector, AcquiredFilesetSpec)
from arcana.study import Study, StudyMetaClass
from arcana.data.file_format.standard import text_format
from arcana.repository.tree import Tree, Subject, Session, Visit
from arcana.data import Fileset
import sys
from arcana.testing.xnat import (
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
        AcquiredFilesetSpec('fileset1', text_format),
        AcquiredFilesetSpec('fileset2', text_format, optional=True),
        AcquiredFilesetSpec('fileset3', text_format),
        AcquiredFilesetSpec('fileset5', text_format, optional=True)]


class TestExistingPrereqsOnXnat(TestMultiSubjectOnXnatMixin,
                                test_study.TestExistingPrereqs):

    BASE_CLASS = test_study.TestExistingPrereqs

    @unittest.skipIf(*SKIP_ARGS)
    def test_per_session_prereqs(self):
        super(TestExistingPrereqsOnXnat, self).test_per_session_prereqs()


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
        sessions = []
        visit_ids = set()
        for subj_id, visits in list(self.STRUCTURE.items()):
            for visit_id, filesets in list(visits.items()):
                sessions.append(Session(subj_id, visit_id, filesets=[
                    Fileset(d, text_format, subject_id=subj_id,
                            visit_id=visit_id) for d in filesets]))
                visit_ids.add(visit_id)
        subjects = [Subject(i, sessions=[s for s in sessions
                                         if s.subject_id == i])
                    for i in self.STRUCTURE]
        visits = [Visit(i, sessions=[s for s in sessions
                                     if s.visit == i])
                  for i in visit_ids]
        return Tree(subjects=subjects, visits=visits)

    @unittest.skipIf(*SKIP_ARGS)
    def test_cache_download(self):
        repository = XnatRepository(
            project_id=self.project,
            server=SERVER,
            cache_dir=tempfile.mkdtemp())
        study = self.create_study(
            TestStudy, 'cache_download',
            inputs=[
                FilesetSelector('fileset1', text_format, 'fileset1'),
                FilesetSelector('fileset3', text_format, 'fileset3')],
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
                    self.assertTrue(op.exists(op.join(
                        sess_dir, inpt.name + inpt.format.extension)))

    @property
    def base_name(self):
        return self.name


class TestProjectInfo(TestMultiSubjectOnXnatMixin,
                      test_directory.TestDirectoryProjectInfo):

    BASE_CLASS = test_directory.TestDirectoryProjectInfo

    @unittest.skipIf(*SKIP_ARGS)
    def test_project_info(self):
        tree = self.repository.tree()
        ref_tree = self.get_tree(self.repository, set_ids=True)
        self.assertEqual(
            tree, ref_tree,
            "Generated project doesn't match reference:{}"
            .format(tree.find_mismatch(ref_tree)))
