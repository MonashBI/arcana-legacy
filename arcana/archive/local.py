from abc import ABCMeta, abstractmethod
import os.path
from collections import defaultdict
from itertools import chain, groupby
from operator import attrgetter
import errno
from .base import (
    Archive, ArchiveSource, ArchiveSink, ArchiveSourceInputSpec,
    ArchiveSinkInputSpec, ArchiveSubjectSinkInputSpec,
    ArchiveVisitSinkInputSpec,
    ArchiveProjectSinkInputSpec, ArchiveSubjectSink, ArchiveVisitSink,
    ArchiveProjectSink)
import stat
import shutil
import logging
import json
from fasteners import InterProcessLock
from nipype.interfaces.base import isdefined
from .tree import Project, Subject, Session, Visit
from arcana.dataset import Dataset, Field
from arcana.exception import (
    ArcanaError, ArcanaBadlyFormattedLocalArchiveError)
from arcana.utils import (
    split_extension, PATH_SUFFIX, FIELD_SUFFIX, NoContextWrapper)


logger = logging.getLogger('Arcana')

SUMMARY_NAME = 'ALL'
FIELDS_FNAME = 'fields.json'

LOCK = '.lock'


def lower(s):
    if s is None:
        return None
    return s.lower()


class LocalNodeMixin(object):

    def _get_data_dir(self, frequency):
        if frequency == 'per_project':
            data_dir = os.path.join(self.base_dir, SUMMARY_NAME,
                                    SUMMARY_NAME)
        elif frequency.startswith('per_subject'):
            data_dir = os.path.join(
                self.base_dir, str(self.inputs.subject_id),
                SUMMARY_NAME)
        elif frequency.startswith('per_visit'):
            data_dir = os.path.join(self.base_dir, SUMMARY_NAME,
                                    str(self.inputs.visit_id))
        elif frequency.startswith('per_session'):
            data_dir = os.path.join(
                self.base_dir, str(self.inputs.subject_id),
                str(self.inputs.visit_id))
        else:
            assert False, "Unrecognised frequency '{}'".format(
                frequency)
        return data_dir

    def fields_path(self, frequency):
        return os.path.join(self._get_data_dir(frequency),
                            FIELDS_FNAME)

    @property
    def base_dir(self):
        return self._base_dir

    def __eq__(self, other):
        return (super(LocalNodeMixin, self).__eq__(other) and
                self.base_dir == other.base_dir)


class LocalSource(ArchiveSource, LocalNodeMixin):

    input_spec = ArchiveSourceInputSpec

    def __init__(self, study_name, datasets, fields, base_dir):
        self._base_dir = base_dir
        super(LocalSource, self).__init__(study_name, datasets, fields)

    def _list_outputs(self):
        # Directory that holds session-specific
        outputs = {}
        # Source datasets
        for dataset in self.datasets:
            fname = dataset.fname(subject_id=self.inputs.subject_id,
                                  visit_id=self.inputs.visit_id)
            outputs[dataset.name + PATH_SUFFIX] = os.path.join(
                self._get_data_dir(dataset.frequency), fname)
        # Source fields from JSON file
        for freq, spec_grp in groupby(
            sorted(self.fields, key=attrgetter('frequency')),
                key=attrgetter('frequency')):
            # Load fields JSON, locking to prevent read/write conflicts
            # Would be better if only checked if locked to allow
            # concurrent reads but not possible with freqi-process
            # locks I believe.
            fpath = self.fields_path(freq)
            try:
                with InterProcessLock(
                        fpath + LOCK, logger=logger), open(fpath) as f:
                    fields = json.load(f)
            except IOError as e:
                if e.errno == errno.ENOENT:
                    fields = {}
                else:
                    raise
            for field in spec_grp:
                outputs[field.name + FIELD_SUFFIX] = field.dtype(
                    fields[self.prefix_study_name(field.name,
                                                  field.is_spec)])
        return outputs


class LocalSinkMixin(LocalNodeMixin):

    __metaclass = ABCMeta

    def __init__(self, study_name, datasets, fields, base_dir):
        self._base_dir = base_dir
        super(LocalSinkMixin, self).__init__(study_name, datasets,
                                             fields)
        LocalNodeMixin.__init__(self)

    def _list_outputs(self):
        """Execute this module.
        """
        # Initiate outputs
        outputs = self._base_outputs()
        out_files = []
        missing_files = []
        # Get output dir from base ArchiveSink class (will change depending on
        # whether it is per session/subject/visit/project)
        out_path = self._get_output_path()
        out_dir = os.path.abspath(os.path.join(*out_path))
        # Make session dir
        if not os.path.exists(out_dir):
            os.makedirs(out_dir, stat.S_IRWXU | stat.S_IRWXG)
        # Loop through datasets connected to the sink and copy them to archive
        # directory
        for spec in self.datasets:
            assert spec.derived, (
                "Should only be sinking derived datasets, not '{}'"
                .format(spec.name))
            filename = getattr(self.inputs, spec.name + PATH_SUFFIX)
            ext = spec.format.extension
            if not isdefined(filename):
                missing_files.append(spec.name)
                continue  # skip the upload for this file
            if lower(split_extension(filename)[1]) != lower(ext):
                raise ArcanaError(
                    "Mismatching extension '{}' for format '{}' ('{}')"
                    .format(split_extension(filename)[1],
                            spec.format, ext))
            assert spec.frequency == self.frequency
            # Copy to local system
            src_path = os.path.abspath(filename)
            out_fname = spec.fname()
            dst_path = os.path.join(out_dir, out_fname)
            out_files.append(dst_path)
            if os.path.isfile(src_path):
                shutil.copyfile(src_path, dst_path)
            elif os.path.isdir(src_path):
                shutil.copytree(src_path, dst_path)
            else:
                assert False
        if missing_files:
            # FIXME: Not sure if this should be an exception or not,
            #        indicates a problem but stopping now would throw
            #        away the datasets that were created
            logger.warning(
                "Missing input datasets '{}' in LocalSink".format(
                    "', '".join(missing_files)))
        # Return cache file paths
        outputs['out_files'] = out_files
        # Loop through fields connected to the sink and save them in the
        # fields JSON file
        out_fields = []
        fpath = self.fields_path(self.frequency)
        # Open fields JSON, locking to prevent other processes
        # reading or writing
        if self.fields:
            with InterProcessLock(fpath + LOCK, logger=logger):
                try:
                    with open(fpath, 'rb') as f:
                        fields = json.load(f)
                except IOError as e:
                    if e.errno == errno.ENOENT:
                        fields = {}
                    else:
                        raise
                # Update fields JSON and write back to file.
                for spec in self.fields:
                    value = getattr(self.inputs,
                                    spec.name + FIELD_SUFFIX)
                    qual_name = self.prefix_study_name(spec.name)
                    if spec.dtype is str:
                        if not isinstance(value, basestring):
                            raise ArcanaError(
                                "Provided value for field '{}' ({}) "
                                "does not match string datatype"
                                .format(spec.name, value))
                    else:
                        if not isinstance(value, spec.dtype):
                            raise ArcanaError(
                                "Provided value for field '{}' ({}) "
                                "does not match datatype {}"
                                .format(spec.name, value, spec.dtype))
                    fields[qual_name] = value
                    out_fields.append((qual_name, value))
                with open(fpath, 'wb') as f:
                    json.dump(fields, f)
        outputs['out_fields'] = out_fields
        return outputs

    @abstractmethod
    def _get_output_path(self):
        "Get the output path to save the generated datasets into"


class LocalSink(LocalSinkMixin, ArchiveSink):

    input_spec = ArchiveSinkInputSpec

    def _get_output_path(self):
        return [
            self.base_dir, self.inputs.subject_id,
            self.inputs.visit_id]


class LocalSubjectSink(LocalSinkMixin, ArchiveSubjectSink):

    input_spec = ArchiveSubjectSinkInputSpec

    def _get_output_path(self):
        return [
            self.base_dir, self.inputs.subject_id, SUMMARY_NAME]


class LocalVisitSink(LocalSinkMixin, ArchiveVisitSink):

    input_spec = ArchiveVisitSinkInputSpec

    def _get_output_path(self):
        return [
            self.base_dir, SUMMARY_NAME, self.inputs.visit_id]


class LocalProjectSink(LocalSinkMixin, ArchiveProjectSink):

    input_spec = ArchiveProjectSinkInputSpec

    def _get_output_path(self):
        return [
            self.base_dir, SUMMARY_NAME, SUMMARY_NAME]


class LocalArchive(Archive):
    """
    An 'Archive' class for directories on the local file system organised
    into sub-directories by subject and then visit.

    Parameters
    ----------
    base_dir : str (path)
        Path to local directory containing data
    """

    type = 'local'
    Source = LocalSource
    Sink = LocalSink
    SubjectSink = LocalSubjectSink
    VisitSink = LocalVisitSink
    ProjectSink = LocalProjectSink

    def __init__(self, base_dir):
        if not os.path.exists(base_dir):
            raise ArcanaError(
                "Base directory for LocalArchive '{}' does not exist"
                .format(base_dir))
        self._base_dir = os.path.abspath(base_dir)

    def __repr__(self):
        return "LocalArchive(base_dir='{}')".format(self.base_dir)

    def __eq__(self, other):
        try:
            return self.base_dir == other.base_dir
        except AttributeError:
            return False

    def source(self, *args, **kwargs):
        source = super(LocalArchive, self).source(
            *args, base_dir=self.base_dir, **kwargs)
        return source

    def sink(self, *args, **kwargs):
        sink = super(LocalArchive, self).sink(
            *args, base_dir=self.base_dir, **kwargs)
        return sink

    def login(self):
        return NoContextWrapper(None)

    def get_tree(self, subject_ids=None, visit_ids=None):
        """
        Return subject and session information for a project in the local
        archive

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
        project : arcana.archive.Project
            A hierarchical tree of subject, session and dataset information for
            the archive
        """
        summaries = defaultdict(dict)
        all_sessions = defaultdict(dict)
        all_visit_ids = set()
        for session_path, dirs, files in os.walk(self.base_dir):
            dnames = [d for d in chain(dirs, files)
                      if not d.startswith('.')]
            relpath = os.path.relpath(session_path, self.base_dir)
            path_parts = relpath.split(os.path.sep)
            depth = len(path_parts)
            if depth > 2:
                continue
            if depth < 2:
                if any(not f.startswith('.') for f in files):
                    raise ArcanaBadlyFormattedLocalArchiveError(
                        "Files ('{}') not permitted at {} level in "
                        "local archive".format(
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
            fields = {}
            for dname in sorted(dnames):
                if dname.startswith(FIELDS_FNAME):
                    continue
                datasets.append(
                    Dataset.from_path(
                        os.path.join(session_path, dname),
                        frequency=frequency,
                        subject_id=subj_id, visit_id=visit_id,
                        archive=self))
            if FIELDS_FNAME in dnames:
                fields = self.fields_from_json(os.path.join(
                    session_path, FIELDS_FNAME),
                    frequency=frequency,
                    subject_id=subj_id, visit_id=visit_id)
            datasets = sorted(datasets)
            fields = sorted(fields)
            if frequency == 'per_session':
                all_sessions[subj_id][visit_id] = Session(
                    subject_id=subj_id, visit_id=visit_id,
                    datasets=datasets, fields=fields)
            else:
                summaries[subj_id][visit_id] = (datasets, fields)
        subjects = []
        for subj_id, subj_sessions in all_sessions.items():
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
                sess[visit_id] for sess in all_sessions.values()))
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

    @classmethod
    def _check_only_dirs(cls, dirs, path):
        if any(not os.path.isdir(os.path.join(path, d))
               for d in dirs):
            raise ArcanaError(
                "Files found in local archive directory '{}' "
                "('{}') instead of sub-directories".format(
                    path, "', '".join(dirs)))

    def all_session_ids(self, project_id):
        project = self.project(project_id)
        return chain(*[
            (s.id for s in subj.sessions) for subj in project.subjects])

    def cache(self, dataset):
        # Don't need to cache dataset as it is already local
        assert dataset._path is not None
        return dataset.path

    @property
    def base_dir(self):
        return self._base_dir

    def subject_summary_path(self, project_id, subject_id):
        return os.path.join(self.base_dir, project_id, subject_id,
                            SUMMARY_NAME)

    def visit_summary_path(self, project_id, visit_id):
        return os.path.join(self.base_dir, project_id,
                            SUMMARY_NAME, visit_id)

    def project_summary_path(self, project_id):
        return os.path.join(self.base_dir, project_id, SUMMARY_NAME,
                            SUMMARY_NAME)

    def fields_from_json(self, fname, frequency,
                         subject_id=None, visit_id=None):
        with open(fname) as f:
            dct = json.load(f)
        return [Field(name=k, value=v, frequency=frequency,
                      subject_id=subject_id, visit_id=visit_id,
                      archive=self)
                for k, v in dct.items()]
