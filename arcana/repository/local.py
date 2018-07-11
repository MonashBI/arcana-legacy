from builtins import str
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
    SUMMARY_NAME = '__ALL__'
    FIELDS_FNAME = 'fields.json'
    LOCK_SUFFIX = '.lock'
    DERIVED_LABEL_FNAME = '.derived'

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
            with InterProcessLock(fpath + self.LOCK_SUFFIX,
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
        target_path = op.join(self.session_dir(dataset), dataset.fname)
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
        fpath = self.fields_json_path(field)
        # Open fields JSON, locking to prevent other processes
        # reading or writing
        with InterProcessLock(fpath + self.LOCK_SUFFIX, logger=logger):
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
        all_data = defaultdict(dict)
        all_visit_ids = set()
        for session_path, dirs, files in os.walk(self.base_dir):
            dnames = [d for d in chain(dirs, files)
                      if not d.startswith('.')]
            relpath = op.relpath(session_path, self.base_dir)
            path_parts = relpath.split(op.sep)
            depth = len(path_parts)
            if depth > 3:
                continue
            if depth < 2:
                if any(not f.startswith('.') for f in files):
                    raise ArcanaBadlyFormattedLocalRepositoryError(
                        "Files ('{}') not permitted at {} level in "
                        "local repository".format(
                            "', '".join(dnames),
                            ('subject' if depth else 'project')))
                continue  # Not a session directory
            if depth == 3:
                if self.DERIVED_LABEL_FNAME in files:
                    study_name = path_parts.pop()
                else:
                    continue  # Dataset directory
            else:
                study_name = None
            subj_id, visit_id = path_parts
            subj_id = subj_id if subj_id != self.SUMMARY_NAME else None
            visit_id = visit_id if visit_id != self.SUMMARY_NAME else None
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
            try:
                # Retrieve datasets and fields from other study directories
                # or root acquired directory
                datasets, fields = all_data[subj_id][visit_id]
            except KeyError:
                datasets = []
                fields = []
            for dname in sorted(dnames):
                if dname.startswith(self.FIELDS_FNAME):
                    continue
                datasets.append(
                    Dataset.from_path(
                        op.join(session_path, dname),
                        frequency=frequency,
                        subject_id=subj_id, visit_id=visit_id,
                        repository=self,
                        study_name=study_name))
            if self.FIELDS_FNAME in dnames:
                with open(op.join(session_path,
                                  self.FIELDS_FNAME), 'r') as f:
                    dct = json.load(f)
                fields = [Field(name=k, value=v, frequency=frequency,
                                subject_id=subj_id, visit_id=visit_id,
                                repository=self, study_name=study_name)
                          for k, v in list(dct.items())]
            datasets = sorted(datasets)
            fields = sorted(fields)
            all_data[subj_id][visit_id] = (datasets, fields)
        all_sessions = defaultdict(dict)
        for subj_id, subj_data in all_data.items():
            if subj_id is None:
                continue  # Create Subject summaries later
            for visit_id, (datasets, fields) in subj_data.items():
                if visit_id is None:
                    continue  # Create Visit summaries later
                all_sessions[subj_id][visit_id] = Session(
                    subject_id=subj_id, visit_id=visit_id,
                    datasets=datasets, fields=fields)
        subjects = []
        for subj_id, subj_sessions in list(all_sessions.items()):
            try:
                datasets, fields = all_data[subj_id][None]
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
                datasets, fields = all_data[None][visit_id]
            except KeyError:
                datasets = []
                fields = []
            visits.append(Visit(visit_id, sorted(visit_sessions),
                                datasets, fields))
        try:
            datasets, fields = all_data[None][None]
        except KeyError:
            datasets = []
            fields = []
        return Project(sorted(subjects), sorted(visits), datasets,
                       fields)

    def session_dir(self, item):
        if item.frequency == 'per_project':
            acq_dir = op.join(
                self.base_dir, self.SUMMARY_NAME, self.SUMMARY_NAME)
        elif item.frequency.startswith('per_subject'):
            acq_dir = op.join(
                self.base_dir, str(item.subject_id), self.SUMMARY_NAME)
        elif item.frequency.startswith('per_visit'):
            acq_dir = op.join(
                self.base_dir, self.SUMMARY_NAME, str(item.visit_id))
        elif item.frequency.startswith('per_session'):
            acq_dir = op.join(
                self.base_dir, str(item.subject_id), str(item.visit_id))
        else:
            assert False, "Unrecognised frequency '{}'".format(
                item.frequency)
        if item.study_name is None:
            sess_dir = acq_dir
        else:
            # Append study-name to path (i.e. make a sub-directory to
            # hold derived products)
            sess_dir = op.join(acq_dir, item.study_name)
        # Make session dir if required
        if not op.exists(sess_dir):
            os.makedirs(sess_dir, stat.S_IRWXU | stat.S_IRWXG)
            # write breadcrumb file t
            if item.study_name is not None:
                open(op.join(sess_dir,
                             self.DERIVED_LABEL_FNAME), 'w').close()
        return sess_dir

    def fields_json_path(self, field):
        return op.join(self.session_dir(field), self.FIELDS_FNAME)
