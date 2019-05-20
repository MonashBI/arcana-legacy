import os
import errno
import os.path as op
from itertools import chain
from .base import Repository
import stat
import shutil
import logging
import json
from fasteners import InterProcessLock
from arcana.data import Fileset, Field
from arcana.pipeline.provenance import Record
from arcana.exceptions import (
    ArcanaError, ArcanaUsageError,
    ArcanaRepositoryError,
    ArcanaMissingDataException,
    ArcanaInsufficientRepoDepthError)
from arcana.utils import get_class_info, HOSTNAME, split_extension


logger = logging.getLogger('arcana')


class BasicRepo(Repository):
    """
    A 'Repository' class for data stored simply in file-system
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

    type = 'directory'
    SUMMARY_NAME = '__ALL__'
    FIELDS_FNAME = 'fields.json'
    PROV_DIR = '__prov__'
    LOCK_SUFFIX = '.lock'
    DEFAULT_SUBJECT_ID = 'SUBJECT'
    DEFAULT_VISIT_ID = 'VISIT'
    MAX_DEPTH = 2

    def __init__(self, root_dir, depth=None, **kwargs):
        super(BasicRepo, self).__init__(**kwargs)
        if not op.exists(root_dir):
            raise ArcanaError(
                "Base directory for BasicRepo '{}' does not exist"
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
            return self.type == other.type and self.root_dir == other.root_dir
        except AttributeError:
            return False

    @property
    def prov(self):
        return {
            'type': get_class_info(type(self)),
            'root': self.root_dir,
            'host': HOSTNAME}

    def __hash__(self):
        return hash(self.type) ^ hash(self.root_dir)

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
            primary_path = self.fileset_path(fileset)
            aux_files = fileset.format.default_aux_file_paths(primary_path)
            if not op.exists(primary_path):
                raise ArcanaMissingDataException(
                    "{} does not exist in {}"
                    .format(fileset, self))
            for aux_name, aux_path in aux_files.items():
                if not op.exists(aux_path):
                    raise ArcanaMissingDataException(
                        "{} is missing '{}' side car in {}"
                        .format(fileset, aux_name, self))
        else:
            primary_path = fileset.path
            aux_files = fileset.aux_files
        return primary_path, aux_files

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
        target_path = self.fileset_path(fileset)
        if op.isfile(fileset.path):
            shutil.copyfile(fileset.path, target_path)
            # Copy side car files into repository
            for aux_name, aux_path in fileset.format.default_aux_file_paths(
                    target_path).items():
                shutil.copyfile(self.aux_file[aux_name], aux_path)
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
                json.dump(dct, f, indent=2)

    def put_record(self, record):
        fpath = self.prov_json_path(record)
        if not op.exists(op.dirname(fpath)):
            os.mkdir(op.dirname(fpath))
        record.save(fpath)

    def find_data(self, subject_ids=None, visit_ids=None, **kwargs):
        """
        Find all data within a repository, registering filesets, fields and
        provenance with the found_fileset, found_field and found_provenance
        methods, respectively

        Parameters
        ----------
        subject_ids : list(str)
            List of subject IDs with which to filter the tree with. If
            None all are returned
        visit_ids : list(str)
            List of visit IDs with which to filter the tree with. If
            None all are returned

        Returns
        -------
        filesets : list[Fileset]
            All the filesets found in the repository
        fields : list[Field]
            All the fields found in the repository
        records : list[Record]
            The provenance records found in the repository
        """
        all_filesets = []
        all_fields = []
        all_records = []
        for session_path, dirs, files in os.walk(self.root_dir):
            relpath = op.relpath(session_path, self.root_dir)
            path_parts = relpath.split(op.sep) if relpath != '.' else []
            ids = self._extract_ids_from_path(path_parts, dirs, files)
            if ids is None:
                continue
            subj_id, visit_id, from_study = ids
            # Check for summaries and filtered IDs
            if subj_id == self.SUMMARY_NAME:
                subj_id = None
            elif subject_ids is not None and subj_id not in subject_ids:
                continue
            if visit_id == self.SUMMARY_NAME:
                visit_id = None
            elif visit_ids is not None and visit_id not in visit_ids:
                continue
            # Map IDs into ID space of study
            subj_id = self.map_subject_id(subj_id)
            visit_id = self.map_visit_id(visit_id)
            # Determine frequency of session|summary
            if (subj_id, visit_id) == (None, None):
                frequency = 'per_study'
            elif subj_id is None:
                frequency = 'per_visit'
            elif visit_id is None:
                frequency = 'per_subject'
            else:
                frequency = 'per_session'
            filtered_files = self._filter_files(files, session_path)
            for fname in filtered_files:
                basename = split_extension(fname)[0]
                all_filesets.append(
                    Fileset.from_path(
                        op.join(session_path, fname),
                        frequency=frequency,
                        subject_id=subj_id, visit_id=visit_id,
                        repository=self,
                        from_study=from_study,
                        potential_aux_files=[
                            f for f in filtered_files
                            if (split_extension(f)[0] == basename and
                                f != fname)],
                        **kwargs))
            for fname in self._filter_dirs(dirs, session_path):
                all_filesets.append(
                    Fileset.from_path(
                        op.join(session_path, fname),
                        frequency=frequency,
                        subject_id=subj_id, visit_id=visit_id,
                        repository=self,
                        from_study=from_study,
                        **kwargs))
            if self.FIELDS_FNAME in files:
                with open(op.join(session_path,
                                  self.FIELDS_FNAME), 'r') as f:
                    dct = json.load(f)
                all_fields.extend(
                    Field(name=k, value=v, frequency=frequency,
                          subject_id=subj_id, visit_id=visit_id,
                          repository=self, from_study=from_study,
                          **kwargs)
                    for k, v in list(dct.items()))
            if self.PROV_DIR in dirs:
                if from_study is None:
                    raise ArcanaRepositoryError(
                        "Found provenance directory in session directory (i.e."
                        " not in study-specific sub-directory)")
                base_prov_dir = op.join(session_path, self.PROV_DIR)
                for fname in os.listdir(base_prov_dir):
                    all_records.append(Record.load(
                        split_extension(fname)[0],
                        frequency, subj_id, visit_id, from_study,
                        op.join(base_prov_dir, fname)))
        return all_filesets, all_fields, all_records

    def _extract_ids_from_path(self, path_parts, dirs, files):
        depth = len(path_parts)
        if depth == self._depth:
            # Load input data
            from_study = None
        elif (depth == (self._depth + 1) and
              self.PROV_DIR in dirs):
            # Load study output
            from_study = path_parts.pop()
        elif (depth < self._depth and
              any(not f.startswith('.') for f in files)):
            # Check to see if there are files in upper level
            # directories, which shouldn't be there (ignoring
            # "hidden" files that start with '.')
            raise ArcanaRepositoryError(
                "Files ('{}') not permitted at {} level in local "
                "repository".format("', '".join(files),
                                    ('subject'
                                     if depth else 'project')))
        else:
            # Not a directory that contains data files or directories
            return None
        if len(path_parts) == 2:
            subj_id, visit_id = path_parts
        elif len(path_parts) == 1:
            subj_id = path_parts[0]
            visit_id = self.DEFAULT_VISIT_ID
        else:
            subj_id = self.DEFAULT_SUBJECT_ID
            visit_id = self.DEFAULT_VISIT_ID
        return subj_id, visit_id, from_study

    def fileset_path(self, item, fname=None):
        if fname is None:
            fname = item.fname
        subject_id = self.inv_map_subject_id(item.subject_id)
        visit_id = self.inv_map_visit_id(item.visit_id)
        if item.frequency == 'per_study':
            subj_dir = self.SUMMARY_NAME
            visit_dir = self.SUMMARY_NAME
        elif item.frequency.startswith('per_subject'):
            if self.depth < 2:
                raise ArcanaInsufficientRepoDepthError(
                    "Basic repo needs to have depth of 2 (i.e. sub-directories"
                    " for subjects and visits) to hold 'per_subject' data")
            subj_dir = str(subject_id)
            visit_dir = self.SUMMARY_NAME
        elif item.frequency.startswith('per_visit'):
            if self.depth < 1:
                raise ArcanaInsufficientRepoDepthError(
                    "Basic repo needs to have depth of at least 1 (i.e. "
                    "sub-directories for subjects) to hold 'per_visit' data")
            subj_dir = self.SUMMARY_NAME
            visit_dir = str(visit_id)
        elif item.frequency.startswith('per_session'):
            subj_dir = str(subject_id)
            visit_dir = str(visit_id)
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
        if item.derived and not op.exists(sess_dir):
            os.makedirs(sess_dir, stat.S_IRWXU | stat.S_IRWXG)
        return op.join(sess_dir, fname)

    def fields_json_path(self, field):
        return self.fileset_path(field, self.FIELDS_FNAME)

    def prov_json_path(self, record):
        return self.fileset_path(
            record,
            fname=op.join(self.PROV_DIR, record.pipeline_name + '.json'))

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
            if self.PROV_DIR in dirs:
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
            raise ArcanaRepositoryError(
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
        filtered = [
            op.join(base_dir, d) for d in dirs
            if not (d.startswith('.') or d == cls.PROV_DIR or (
                cls.PROV_DIR in os.listdir(op.join(base_dir, d))))]
        return filtered

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
