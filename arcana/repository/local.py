from builtins import str
from abc import abstractmethod
import os
import os.path as op
from collections import defaultdict
from itertools import chain
import errno
from .base import BaseRepository
import stat
import shutil
import logging
import json
from fasteners import InterProcessLock
from .tree import Project, Subject, Session, Visit
from arcana.dataset import Dataset, Field
from arcana.exception import (
    ArcanaError, ArcanaBadlyFormattedLocalRepositoryError,
    ArcanaMissingDataException)


logger = logging.getLogger('arcana')

SUMMARY_NAME = 'ALL'
FIELDS_FNAME = 'fields.json'

LOCK = '.lock'


class LocalRepository(BaseRepository):
    """
    An 'Repository' class for directories on the local file system organised
    into sub-directories by subject and then visit.

    Parameters
    ----------
    base_dir : str (path)
        Path to local directory containing data
    """

    type = 'local'

    def __init__(self, base_dir):
        super(LocalRepository, self).__init__()
        if not op.exists(base_dir):
            raise ArcanaError(
                "Base directory for LocalRepository '{}' does not exist"
                .format(base_dir))
        self._base_dir = op.abspath(base_dir)

    def __repr__(self):
        return "{}(base_dir='{}')".format(type(self).__name__,
                                          self.base_dir)

    def __eq__(self, other):
        try:
            return self.base_dir == other.base_dir
        except AttributeError:
            return False

    def __hash__(self):
        return hash(self.base_dir)

    @property
    def base_dir(self):
        return self._base_dir

    def get_dataset(self, dataset):
        """
        Set the path of the dataset from the repository
        """
        # Don't need to cache dataset as it is already local as long
        # as the path is set
        if dataset._path is None:
            path = op.join(self.session_dir(dataset), dataset.fname)
            if not op.exists(path):
                raise ArcanaMissingDataException(
                    "{} does not exist in the local repository {}"
                    .format(dataset, self))
        else:
            path = dataset.path
        return path

    def get_field(self, field):
        """
        Update the value of the field from the repository
        """
        # Load fields JSON, locking to prevent read/write conflicts
        # Would be better if only checked if locked to allow
        # concurrent reads but not possible with multi-process
        # locks (in my understanding at least).
        fpath = self.fields_json_path(field)
        try:
            with InterProcessLock(fpath + LOCK,
                                  logger=logger), open(fpath, 'r') as f:
                dct = json.load(f)
            return field.dtype(dct[field.name])
        except (KeyError, IOError) as e:
            try:
                # Check to see if the IOError wasn't just because of a
                # missing file
                if e.errno != errno.ENOENT:
                    raise
            except AttributeError:
                pass
            raise ArcanaMissingDataException(
                "{} does not exist in the local repository {}"
                .foramt(field, self))

    def put_dataset(self, dataset):
        """
        Inserts or updates a dataset in the repository
        """
        # Make session dir
        sess_dir = self.session_dir(dataset)
        if not op.exists(sess_dir):
            os.makedirs(sess_dir, stat.S_IRWXU | stat.S_IRWXG)
        target_path = op.join(self.session_dir(dataset),
                              dataset.name + dataset.format.extension)
        if op.isfile(dataset.path):
            shutil.copyfile(dataset.path, target_path)
        elif op.isdir(dataset.path):
            shutil.copytree(dataset.path, target_path)
        else:
            assert False

    def put_field(self, field):
        """
        Inserts or updates a field in the repository
        """
        # Make session dir
        sess_dir = self.session_dir(field)
        if not op.exists(sess_dir):
            os.makedirs(sess_dir, stat.S_IRWXU | stat.S_IRWXG)
        fpath = self.fields_json_path(field)
        # Open fields JSON, locking to prevent other processes
        # reading or writing
        with InterProcessLock(fpath + LOCK, logger=logger):
            try:
                with open(fpath, 'r') as f:
                    dct = json.load(f)
            except IOError as e:
                if e.errno == errno.ENOENT:
                    dct = {}
                else:
                    raise
            dct[field.name] = field.value
            with open(fpath, 'w') as f:
                json.dump(dct, f)

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
        project : arcana.repository.Project
            A hierarchical tree of subject, session and dataset information for
            the repository
        """
        summaries = defaultdict(dict)
        all_sessions = defaultdict(dict)
        all_visit_ids = set()
        for session_path, dirs, files in os.walk(self.base_dir):
            dnames = [d for d in chain(dirs, files)
                      if not d.startswith('.')]
            relpath = op.relpath(session_path, self.base_dir)
            path_parts = relpath.split(op.sep)
            depth = len(path_parts)
            if depth > 2:
                continue
            if depth < 2:
                if any(not f.startswith('.') for f in files):
                    raise ArcanaBadlyFormattedLocalRepositoryError(
                        "Files ('{}') not permitted at {} level in "
                        "local repository".format(
                            "', '".join(dnames),
                            ('subject' if depth else 'project')))
                continue  # Not a session directory
            subj_id, visit_id = path_parts
            subj_id = subj_id if subj_id != SUMMARY_NAME else None
            visit_id = visit_id if visit_id != SUMMARY_NAME else None
            if (subject_ids is not None and subj_id is not None and
                    subj_id not in subject_ids):
                continue
            if (visit_ids is not None and visit_id is not None and
                    visit_id not in visit_ids):
                continue
            if (subj_id, visit_id) == (None, None):
                frequency = 'per_project'
            elif subj_id is None:
                frequency = 'per_visit'
                all_visit_ids.add(visit_id)
            elif visit_id is None:
                frequency = 'per_subject'
            else:
                frequency = 'per_session'
                all_visit_ids.add(visit_id)
            datasets = []
            fields = []
            for dname in sorted(dnames):
                if dname.startswith(FIELDS_FNAME):
                    continue
                datasets.append(
                    Dataset.from_path(
                        op.join(session_path, dname),
                        frequency=frequency,
                        subject_id=subj_id, visit_id=visit_id,
                        repository=self))
            if FIELDS_FNAME in dnames:
                with open(op.join(session_path,
                                  FIELDS_FNAME), 'r') as f:
                    dct = json.load(f)
                fields = [Field(name=k, value=v, frequency=frequency,
                                subject_id=subj_id, visit_id=visit_id,
                                repository=self)
                          for k, v in list(dct.items())]
            datasets = sorted(datasets)
            fields = sorted(fields)
            if frequency == 'per_session':
                all_sessions[subj_id][visit_id] = Session(
                    subject_id=subj_id, visit_id=visit_id,
                    datasets=datasets, fields=fields)
            else:
                summaries[subj_id][visit_id] = (datasets, fields)
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

    def session_dir(self, item):
        if item.frequency == 'per_project':
            data_dir = op.join(
                self.base_dir, SUMMARY_NAME, SUMMARY_NAME)
        elif item.frequency.startswith('per_subject'):
            data_dir = op.join(
                self.base_dir, str(item.subject_id), SUMMARY_NAME)
        elif item.frequency.startswith('per_visit'):
            data_dir = op.join(
                self.base_dir, SUMMARY_NAME, str(item.visit_id))
        elif item.frequency.startswith('per_session'):
            data_dir = op.join(
                self.base_dir, str(item.subject_id), str(item.visit_id))
        else:
            assert False, "Unrecognised frequency '{}'".format(
                item.frequency)
        return data_dir

    def fields_json_path(self, field):
        return op.join(self.session_dir(field), FIELDS_FNAME)
