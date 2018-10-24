import os
import errno
import os.path as op
from collections import defaultdict
from itertools import chain
from .base import BaseRepository
import stat
import shutil
import logging
import json
from fasteners import InterProcessLock
from .tree import Tree, Subject, Session, Visit
from arcana.data import Fileset, Field
from arcana.exception import (
    ArcanaError, ArcanaUsageError,
    ArcanaBadlyFormattedDirectoryRepositoryError,
    ArcanaMissingDataException)


logger = logging.getLogger('arcana')


class DirectoryRepository(BaseRepository):
    """
    An 'Repository' class for data stored simply in file-system
    directories. Can be a single directory if it contains only one subject
    and visit, otherwise if sub-directories are present (that aren't
    recognised as single filesets) then they are assumed to be
    separate subjects. For multi-visit datasets an additional
    layer of sub-directories for each visit is required within each
    subject sub-directory.

    Parameters
    ----------
    root_dir : str (path)
        Path to local directory containing data
    depth : int
        The number of sub-directory layers under the base directory. For
        example, if depth == 0, then the base directory contains the
        data files and data, if depth == 1, then there is a layer of
        sub-directories for each subject, and if depth == 2 there is
        an additional layer of sub-directories for each visit of each
        subject.
    """

    type = 'simple'
    SUMMARY_NAME = '__ALL__'
    FIELDS_FNAME = 'fields.json'
    LOCK_SUFFIX = '.lock'
    DERIVED_LABEL_FNAME = '.derived'
    DEFAULT_SUBJECT_ID = 'SUBJECT'
    DEFAULT_VISIT_ID = 'VISIT'
    MAX_DEPTH = 2

    def __init__(self, root_dir, depth=None):
        super(DirectoryRepository, self).__init__()
        if not op.exists(root_dir):
            raise ArcanaError(
                "Base directory for DirectoryRepository '{}' does not exist"
                .format(root_dir))
        self._root_dir = op.abspath(root_dir)
        if depth is None:
            depth = self.guess_depth(root_dir)
        self._depth = depth

    def __repr__(self):
        return "{}(root_dir='{}')".format(type(self).__name__,
                                          self.root_dir)

    def __eq__(self, other):
        try:
            return self.root_dir == other.root_dir
        except AttributeError:
            return False

    def __hash__(self):
        return hash(self.root_dir)

    @property
    def root_dir(self):
        return self._root_dir

    @property
    def depth(self):
        return self._depth

    def get_fileset(self, fileset):
        """
        Set the path of the fileset from the repository
        """
        # Don't need to cache fileset as it is already local as long
        # as the path is set
        if fileset._path is None:
            path = op.join(self.session_dir(fileset), fileset.fname)
            if not op.exists(path):
                raise ArcanaMissingDataException(
                    "{} does not exist in the local repository {}"
                    .format(fileset, self))
        else:
            path = fileset.path
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
            val = dct[field.name]
            if field.array:
                val = [field.dtype(v) for v in val]
            else:
                val = field.dtype(val)
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
                .format(field.name, self))
        return val

    def put_fileset(self, fileset):
        """
        Inserts or updates a fileset in the repository
        """
        target_path = op.join(self.session_dir(fileset), fileset.fname)
        if op.isfile(fileset.path):
            shutil.copyfile(fileset.path, target_path)
        elif op.isdir(fileset.path):
            if op.exists(target_path):
                shutil.rmtree(target_path)
            shutil.copytree(fileset.path, target_path)
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
            if field.array:
                dct[field.name] = list(field.value)
            else:
                dct[field.name] = field.value
            with open(fpath, 'w') as f:
                json.dump(dct, f)

    def tree(self, subject_ids=None, visit_ids=None, **kwargs):
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
        all_data = defaultdict(dict)
        all_visit_ids = set()
        for session_path, dirs, files in os.walk(self.root_dir):
            relpath = op.relpath(session_path, self.root_dir)
            if relpath == '.':
                path_parts = []
            else:
                path_parts = relpath.split(op.sep)
            depth = len(path_parts)
            if depth == self._depth:
                # Load input data
                from_study = None
            elif (depth == (self._depth + 1) and
                  self.DERIVED_LABEL_FNAME in files):
                # Load study output
                from_study = path_parts.pop()
            elif (depth < self._depth and
                  any(not f.startswith('.') for f in files)):
                # Check to see if there are files in upper level
                # directories, which shouldn't be there (ignoring
                # "hidden" files that start with '.')
                raise ArcanaBadlyFormattedDirectoryRepositoryError(
                    "Files ('{}') not permitted at {} level in local "
                    "repository".format("', '".join(files),
                                        ('subject'
                                         if depth else 'project')))
            else:
                # Not a directory that contains data files or directories
                continue
            if len(path_parts) == 2:
                subj_id, visit_id = path_parts
            elif len(path_parts) == 1:
                subj_id = path_parts[0]
                visit_id = self.DEFAULT_SUBJECT_ID
            else:
                subj_id = self.DEFAULT_SUBJECT_ID
                visit_id = self.DEFAULT_VISIT_ID
            subj_id = subj_id if subj_id != self.SUMMARY_NAME else None
            visit_id = visit_id if visit_id != self.SUMMARY_NAME else None
            if (subject_ids is not None and subj_id is not None and
                    subj_id not in subject_ids):
                continue
            if (visit_ids is not None and visit_id is not None and
                    visit_id not in visit_ids):
                continue
            if (subj_id, visit_id) == (None, None):
                frequency = 'per_study'
            elif subj_id is None:
                frequency = 'per_visit'
                all_visit_ids.add(visit_id)
            elif visit_id is None:
                frequency = 'per_subject'
            else:
                frequency = 'per_session'
                all_visit_ids.add(visit_id)
            try:
                # Retrieve filesets and fields from other study directories
                # or root acquired directory
                filesets, fields = all_data[subj_id][visit_id]
            except KeyError:
                filesets = []
                fields = []
            for fname in chain(self._filter_files(files, session_path),
                               self._filter_dirs(dirs, session_path)):
                filesets.append(
                    Fileset.from_path(
                        op.join(session_path, fname),
                        frequency=frequency,
                        subject_id=subj_id, visit_id=visit_id,
                        repository=self,
                        from_study=from_study))
            if self.FIELDS_FNAME in files:
                with open(op.join(session_path,
                                  self.FIELDS_FNAME), 'r') as f:
                    dct = json.load(f)
                fields = [Field(name=k, value=v, frequency=frequency,
                                subject_id=subj_id, visit_id=visit_id,
                                repository=self, from_study=from_study)
                          for k, v in list(dct.items())]
            filesets = sorted(filesets)
            fields = sorted(fields)
            all_data[subj_id][visit_id] = (filesets, fields)
        all_sessions = defaultdict(dict)
        for subj_id, subj_data in all_data.items():
            if subj_id is None:
                continue  # Create Subject summaries later
            for visit_id, (filesets, fields) in subj_data.items():
                if visit_id is None:
                    continue  # Create Visit summaries later
                all_sessions[subj_id][visit_id] = Session(
                    subject_id=subj_id, visit_id=visit_id,
                    filesets=filesets, fields=fields)
        subjects = []
        for subj_id, subj_sessions in list(all_sessions.items()):
            try:
                filesets, fields = all_data[subj_id][None]
            except KeyError:
                filesets = []
                fields = []
            subjects.append(Subject(
                subj_id, sorted(subj_sessions.values()), filesets,
                fields))
        visits = []
        for visit_id in all_visit_ids:
            visit_sessions = list(chain(
                sess[visit_id] for sess in list(all_sessions.values())))
            try:
                filesets, fields = all_data[None][visit_id]
            except KeyError:
                filesets = []
                fields = []
            visits.append(Visit(visit_id, sorted(visit_sessions),
                                filesets, fields))
        try:
            filesets, fields = all_data[None][None]
        except KeyError:
            filesets = []
            fields = []
        return Tree(sorted(subjects), sorted(visits), filesets,
                    fields, **kwargs)

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
        if self.depth == 2:
            acq_dir = op.join(self.root_dir, subj_dir, visit_dir)
        elif self.depth == 1:
            acq_dir = op.join(self.root_dir, subj_dir)
        elif self.depth == 0:
            acq_dir = self.root_dir
        else:
            assert False
        if item.from_study is None:
            sess_dir = acq_dir
        else:
            # Append study-name to path (i.e. make a sub-directory to
            # hold derived products)
            sess_dir = op.join(acq_dir, item.from_study)
        # Make session dir if required
        if not op.exists(sess_dir):
            os.makedirs(sess_dir, stat.S_IRWXU | stat.S_IRWXG)
            # write breadcrumb file t
            if item.from_study is not None:
                open(op.join(sess_dir,
                             self.DERIVED_LABEL_FNAME), 'w').close()
        return sess_dir

    def fields_json_path(self, field):
        return op.join(self.session_dir(field), self.FIELDS_FNAME)

    def guess_depth(self, root_dir):
        """
        Try to guess the depth of a directory repository (i.e. whether it has
        sub-folders for multiple subjects or visits, depending on where files
        and/or derived label files are found in the hierarchy of
        sub-directories under the root dir.

        Parameters
        ----------
        root_dir : str
            Path to the root directory of the repository
        """
        deepest = -1
        for path, dirs, files in os.walk(root_dir):
            depth = self.path_depth(path)
            filtered_files = self._filter_files(files, path)
            if filtered_files:
                logger.info("Guessing depth of directory repository at '{}' is"
                            " {} due to unfiltered files ('{}') in '{}'"
                            .format(root_dir, depth,
                                    "', '".join(filtered_files), path))
                return depth
            if self.DERIVED_LABEL_FNAME in files:
                depth_to_return = max(depth - 1, 0)
                logger.info("Guessing depth of directory repository at '{}' is"
                            "{} due to \"Derived label file\" in '{}'"
                            .format(root_dir, depth_to_return, path))
                return depth_to_return
            if depth >= self.MAX_DEPTH:
                logger.info("Guessing depth of directory repository at '{}' is"
                            " {} as '{}' is already at maximum depth"
                            .format(root_dir, self.MAX_DEPTH, path))
                return self.MAX_DEPTH
            try:
                for fpath in chain(filtered_files,
                                   self._filter_dirs(dirs, path)):
                    Fileset.from_path(fpath)
            except ArcanaError:
                pass
            else:
                if depth > deepest:
                    deepest = depth
        if deepest == -1:
            raise ArcanaBadlyFormattedDirectoryRepositoryError(
                "Could not guess depth of '{}' repository as did not find "
                "a valid session directory within sub-directories."
                .format(root_dir))
        return deepest

    @classmethod
    def _filter_files(cls, files, base_dir):
        # Filter out hidden files (i.e. starting with '.')
        return [op.join(base_dir, f) for f in files
                 if not (f.startswith('.') or
                         f.startswith(cls.FIELDS_FNAME))]

    @classmethod
    def _filter_dirs(cls, dirs, base_dir):
        # Filter out hidden directories (i.e. starting with '.')
        # and derived study directories from fileset names
        return [
            op.join(base_dir, d) for d in dirs
            if not (d.startswith('.') or (
                cls.DERIVED_LABEL_FNAME in os.listdir(op.join(base_dir, d))))]

    def path_depth(self, dpath):
        relpath = op.relpath(dpath, self.root_dir)
        if '..' in relpath:
            raise ArcanaUsageError(
                "Path '{}' is not a sub-directory of '{}'".format(
                    dpath, self.root_dir))
        elif relpath == '.':
            depth = 0
        else:
            depth = relpath.count(op.sep) + 1
        return depth
