from __future__ import absolute_import
from builtins import str
from past.builtins import basestring
from builtins import object
from abc import ABCMeta, abstractmethod
import os.path
from collections import defaultdict
from itertools import chain, groupby
from operator import attrgetter
import errno
from .local import LocalRepository
import stat
import shutil
import logging
import json
from bids import grabbids as gb
from .tree import Project, Subject, Session, Visit
from arcana.dataset import Dataset, Field
from arcana.exception import ArcanaError
from arcana.utils import NoContextWrapper


class BidsRepository(LocalRepository):
    """
    An 'Repository' class for directories on the local file system organised
    into sub-directories by subject and then visit.

    Parameters
    ----------
    base_dir : str (path)
        Path to local directory containing data
    """

    type = 'bids'

    def __repr__(self):
        return "BidsRepository(base_dir='{}')".format(self.base_dir)

    def __eq__(self, other):
        try:
            return self.base_dir == other.base_dir
        except AttributeError:
            return False

    def login(self):
        return NoContextWrapper(None)

    def get_tree(self, subject_ids=None, visit_ids=None):
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
        project : arcana.repository.Project
            A hierarchical tree of subject, session and dataset information for
            the repository
        """
        summaries = defaultdict(dict)
        all_sessions = defaultdict(dict)
        all_visit_ids = set()

        layout = gb.BIDSLayout(self.base_dir)
        bids_datasets = defaultdict(lambda: defaultdict(dict))
        derived_tree = super(BidsRepository, self).get_tree(
            subject_ids=None, visit_ids=None)
        for bids_obj in layout.get(return_type='object'):
            subj_id = bids_obj.entities['subject']
            if subject_ids is not None and subj_id not in subject_ids:
                continue
            visit_id = bids_obj.entities['session']
            if visit_ids is not None and visit_id not in visit_ids:
                continue
            path = bids_obj.path
            bids_datasets[subj_id][visit_id] = Dataset.from_path(
                path, frequency='per_session', subject_id=subj_id,
                visit_id=visit_id, repository=self)
        # Need to pull out all datasets and fields
        all_sessions = []
        for subj_id, visits in all_sessions.items():
            for visit_id, visit in visits.items():
                all_sessions.append(Session(
                    subject_id=subj_id, visit_id=visit_id,
                    datasets=datasets, fields=fields))

        subjects = []
        for subj_id, subj_sessions in list(all_sessions.items()):
            try:
                datasets, fields = summaries[subj_id][None]
            except KeyError:
                datasets = []
                fields = []
            subjects.append(Subject(
                subj_id, sorted(subj_sessions.values()), datasets,
                fields))
        visits = []
        for visit_id in all_visit_ids:
            visit_sessions = list(chain(
                sess[visit_id] for sess in list(all_sessions.values())))
            try:
                datasets, fields = summaries[None][visit_id]
            except KeyError:
                datasets = []
                fields = []
            visits.append(Visit(visit_id, sorted(visit_sessions),
                                datasets, fields))
        try:
            datasets, fields = summaries[None][None]
        except KeyError:
            datasets = []
            fields = []
        return Project(sorted(subjects), sorted(visits), datasets,
                       fields)

    def _get_derived_sub_path(self, spec):
        return os.path.join('derived', 'arcana', spec.study.name)

    def fields_from_json(self, fname, frequency,
                         subject_id=None, visit_id=None):
        with open(fname, 'r') as f:
            dct = json.load(f)
        return [Field(name=k, value=v, frequency=frequency,
                      subject_id=subject_id, visit_id=visit_id,
                      repository=self)
                for k, v in list(dct.items())]
