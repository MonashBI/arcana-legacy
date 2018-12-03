from __future__ import absolute_import
import os
import os.path as op
import stat
from .directory import DirectoryRepository
import logging
from .tree import Tree
from bids.layout import BIDSLayout
from arcana.data import Fileset, Field
from arcana.pipeline import Record

logger = logging.getLogger('arcana')


class BidsRepository(DirectoryRepository):
    """
    An 'Repository' class for directories on the local file system organised
    into sub-directories by subject and then visit.

    Parameters
    ----------
    root_dir : str
        The path to the root of the BidsRepository
    """

    type = 'bids'

    def __init__(self, root_dir):
        self._root_dir = root_dir
        self._layout = BIDSLayout(root_dir)

    @property
    def root_dir(self):
        return self._root_dir

    @property
    def derivatives_path(self):
        return op.join(self.root_dir, 'derivatives')

    @property
    def layout(self):
        return self._layout

    def __repr__(self):
        return "BidsRepository(root_dir='{}')".format(self.root_dir)

    def __eq__(self, other):
        try:
            return self.root_dir == other.root_dir
        except AttributeError:
            return False

    def find_data(self, subject_ids=None, visit_ids=None):
        """
        Return subject and session information for a project in the local
        repository

        Parameters
        ----------
        subject_ids : list(str)
            List of subject IDs with which to filter the tree with. If None all
            are returned
        visit_ids : list(str)
            List of visit IDs with which to filter the tree with. If None all
            are returned

        Returns
        -------
        project : arcana.repository.Tree
            A hierarchical tree of subject, session and fileset information for
            the repository
        """
        filesets = []
        fields = []
        records = []
        for bids_obj in self.layout.get(return_type='object'):
            subj_id = bids_obj.entities['subject']
            if subject_ids is not None and subj_id not in subject_ids:
                continue
            visit_id = bids_obj.entities['session']
            if visit_ids is not None and visit_id not in visit_ids:
                continue
            filesets.append(Fileset.from_path(
                bids_obj.path, frequency='per_session',
                subject_id=subj_id, visit_id=visit_id, repository=self,
                bids_attrs=bids_obj))
        return Tree.construct(filesets, fields, records)

    def session_dir(self, item):
        if item.frequency == 'per_study':
            subj_dir = self.SUMMARY_NAME
            visit_dir = self.SUMMARY_NAME
        elif item.frequency.startswith('per_subject'):
            subj_dir = str(item.subject_id)
            visit_dir = self.SUMMARY_NAME
        elif item.frequency.startswith('per_visit'):
            subj_dir = self.SUMMARY_NAME
            visit_dir = str(item.visit_id)
        elif item.frequency.startswith('per_session'):
            subj_dir = str(item.subject_id)
            visit_dir = str(item.visit_id)
        else:
            assert False, "Unrecognised frequency '{}'".format(
                item.frequency)
        if item.derived:
            base_dir = op.join(self.root_dir, 'derivatives')
        else:
            base_dir = self.root_dir
        # Make session dir if required
        if not op.exists(sess_dir):
            os.makedirs(sess_dir, stat.S_IRWXU | stat.S_IRWXG)
        return sess_dir
