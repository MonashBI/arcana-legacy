from __future__ import absolute_import
from builtins import str  # @UnusedImport
import os.path as op
from collections import defaultdict
from itertools import chain
from arcana.utils import makedirs
from .base import BaseRepository
import logging
from bids.layout import BIDSLayout
from .tree import Tree, Subject, Session, Visit
from arcana.data import Fileset
from arcana.exceptions import ArcanaNameError

logger = logging.getLogger('arcana')


class BidsRepository(BaseRepository):
    """
    An 'Repository' class for directories on the local file system organised
    into sub-directories by subject and then visit.

    Parameters
    ----------
    paths : str | list
        The path(s) where project files are located.
            Must be one of:

            - A string giving the name of a built-in config (e.g., 'bids')
            - A path to a directory containing files to index
            - A list of paths to directories to be indexed
            - A list of 2-tuples where each tuple encodes a mapping from
              directories to domains. The first element is a string or
              list giving the paths to one or more directories to index.
              The second element specifies which domains to apply to the
              specified files, and can be one of:
                * A string giving the name of a built-in config
                * A string giving the path to a JSON config file
                * A dictionary containing config information
                * A list of any combination of strings or dicts

        At present, built-in domains include 'bids' and 'derivatives'.

    root : str
        The root directory of the BIDS project. All other paths
        will be set relative to this if absolute_paths is False. If None,
        filesystem root ('/') is used.
    validate : bool
        If True, all files are checked for BIDS compliance
        when first indexed, and non-compliant files are ignored. This
        provides a convenient way to restrict file indexing to only those
        files defined in the "core" BIDS spec, as setting validate=True
        will lead files in supplementary folders like derivatives/, code/,
        etc. to be ignored.
    index_associated : bool
        Argument passed onto the BIDSValidator;
        ignored if validate = False.
    include : str | list
        String or list of strings giving paths to files or
        directories to include in indexing. Note that if this argument is
        passed, *only* files and directories that match at least one of the
        patterns in the include list will be indexed. Cannot be used
        together with 'exclude'.
    include : str | list
        String or list of strings giving paths to files or
        directories to exclude from indexing. If this argument is passed,
        all files and directories that match at least one of the patterns
        in the include list will be ignored. Cannot be used together with
        'include'.
    absolute_paths : bool
        If True, queries always return absolute paths.
        If False, queries return relative paths, unless the root argument
        was left empty (in which case the root defaults to the file system
        root).
    kwargs:
        Optional keyword arguments to pass onto the Layout initializer
        in grabbit.
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

    def get_fileset(self, fileset):
        """
        Set the path of the fileset from the repository
        """
        raise NotImplementedError

    def tree(self, subject_ids=None, visit_ids=None):
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
        bids_filesets = defaultdict(lambda: defaultdict(dict))
        derived_tree = super(BidsRepository, self).tree(
            subject_ids=None, visit_ids=None)
        for bids_obj in self.layout.get(return_type='object'):
            subj_id = bids_obj.entities['subject']
            if subject_ids is not None and subj_id not in subject_ids:
                continue
            visit_id = bids_obj.entities['session']
            if visit_ids is not None and visit_id not in visit_ids:
                continue
            bids_filesets[subj_id][visit_id] = Fileset.from_path(
                bids_obj.path, frequency='per_session',
                subject_id=subj_id, visit_id=visit_id, repository=self,
                bids_attrs=bids_obj)
        # Need to pull out all filesets and fields
        all_sessions = defaultdict(dict)
        all_visit_ids = set()
        for subj_id, visits in bids_filesets.items():
            for visit_id, filesets in visits.items():
                session = Session(
                    subject_id=subj_id, visit_id=visit_id,
                    filesets=filesets)
                try:
                    session.derived = derived_tree.subject(
                        subj_id).visit(visit_id)
                except ArcanaNameError:
                    pass  # No matching derived session
                all_sessions[subj_id][visit_id] = session
                all_visit_ids.add(visit_id)

        subjects = []
        for subj_id, subj_sessions in list(all_sessions.items()):
            try:
                derived_subject = derived_tree.subject(subj_id)
            except ArcanaNameError:
                filesets = []
                fields = []
            else:
                filesets = derived_subject.filesets
                fields = derived_subject.fields
            subjects.append(Subject(
                subj_id, sorted(subj_sessions.values()),
                filesets, fields))
        visits = []
        for visit_id in all_visit_ids:
            try:
                derived_visit = derived_tree.visit(subj_id)
            except ArcanaNameError:
                filesets = []
                fields = []
            else:
                filesets = derived_visit.filesets
                fields = derived_visit.fields
            visit_sessions = list(chain(
                sess[visit_id] for sess in list(all_sessions.values())))
            visits.append(
                Visit(visit_id, sorted(visit_sessions),
                      filesets, fields))
        return Tree(sorted(subjects), sorted(visits),
                       derived_tree.filesets, derived_tree.fields)
