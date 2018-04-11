from abc import ABCMeta, abstractmethod
import os.path
import pydicom
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
from nipype.interfaces.base import (
    Directory, isdefined)
from .base import Project, Subject, Session, Visit
from nianalysis.dataset import Dataset, Field
from nianalysis.exceptions import (
    NiAnalysisError, NiAnalysisBadlyFormattedLocalArchiveError)
from nianalysis.data_formats import data_formats
from nianalysis.utils import split_extension
from nianalysis.utils import PATH_SUFFIX, FIELD_SUFFIX


logger = logging.getLogger('NiAnalysis')

SUMMARY_NAME = 'ALL'
FIELDS_FNAME = 'fields.json'

LOCK = '.lock'


def lower(s):
    if s is None:
        return None
    return s.lower()


class LocalNodeMixin(object):

    def _get_data_dir(self, multiplicity):
        if multiplicity == 'per_project':
            data_dir = os.path.join(self.base_dir, SUMMARY_NAME,
                                    SUMMARY_NAME)
        elif multiplicity.startswith('per_subject'):
            data_dir = os.path.join(
                self.base_dir, str(self.inputs.subject_id),
                SUMMARY_NAME)
        elif multiplicity.startswith('per_visit'):
            data_dir = os.path.join(self.base_dir, SUMMARY_NAME,
                                    str(self.inputs.visit_id))
        elif multiplicity.startswith('per_session'):
            data_dir = os.path.join(
                self.base_dir, str(self.inputs.subject_id),
                str(self.inputs.visit_id))
        else:
            assert False, "Unrecognised multiplicity '{}'".format(
                multiplicity)
        return data_dir

    def fields_path(self, multiplicity):
        return os.path.join(self._get_data_dir(multiplicity),
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
                self._get_data_dir(dataset.multiplicity), fname)
        # Source fields from JSON file
        for mult, spec_grp in groupby(
            sorted(self.fields, key=attrgetter('multiplicity')),
                key=attrgetter('multiplicity')):
            # Load fields JSON, locking to prevent read/write conflicts
            # Would be better if only checked if locked to allow
            # concurrent reads but not possible with multi-process
            # locks I believe.
            fpath = self.fields_path(mult)
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
            assert spec.processed, (
                "Should only be sinking processed datasets, not '{}'"
                .format(spec.name))
            filename = getattr(self.inputs, spec.name + PATH_SUFFIX)
            ext = spec.format.extension
            if not isdefined(filename):
                missing_files.append(spec.name)
                continue  # skip the upload for this file
            if lower(split_extension(filename)[1]) != lower(ext):
                raise NiAnalysisError(
                    "Mismatching extension '{}' for format '{}' ('{}')"
                    .format(split_extension(filename)[1],
                            spec.format, ext))
            assert spec.multiplicity == self.multiplicity
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
        fpath = self.fields_path(self.multiplicity)
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
                    name, dtype = spec[:2]
                    value = getattr(self.inputs, name + FIELD_SUFFIX)
                    qual_name = self.prefix_study_name(name)
                    if dtype is str:
                        assert isinstance(value, basestring)
                    else:
                        assert isinstance(value, dtype)
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
    Abstract base class for all Archive systems, DaRIS, XNAT and local file
    system. Sets out the interface that all Archive classes should implement.
    """

    type = 'local'
    Source = LocalSource
    Sink = LocalSink
    SubjectSink = LocalSubjectSink
    VisitSink = LocalVisitSink
    ProjectSink = LocalProjectSink

    def __init__(self, base_dir):
        if not os.path.exists(base_dir):
            raise NiAnalysisError(
                "Base directory for LocalArchive '{}' does not exist"
                .format(base_dir))
        self._base_dir = os.path.abspath(base_dir)

    def __repr__(self):
        return "LocalArchive(base_dir='{}')".format(self.base_dir)

    def source(self, *args, **kwargs):
        source = super(LocalArchive, self).source(
            *args, base_dir=self.base_dir, **kwargs)
        return source

    def sink(self, *args, **kwargs):
        sink = super(LocalArchive, self).sink(
            *args, base_dir=self.base_dir, **kwargs)
        return sink

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
        project : nianalysis.archive.Project
            A hierarchical tree of subject, session and dataset information for
            the archive
        """
        summaries = defaultdict(dict)
        all_sessions = defaultdict(dict)
        all_visit_ids = set()
        for session_path, _, all_fnames in os.walk(self.base_dir):
            fnames = [f for f in all_fnames if not f.startswith('.')]
            relpath = os.path.relpath(session_path, self.base_dir)
            path_parts = relpath.split(os.path.sep)
            depth = len(path_parts)
            if depth > 2:
                continue
            if depth < 2:
                if fnames:
                    raise NiAnalysisBadlyFormattedLocalArchiveError(
                        "Files ('{}') not permitted at {} level in "
                        "local archive".format(
                            "', '".join(fnames),
                            ('subject' if depth else 'project')))
                continue  # Not a session directory
            subj_id, visit_id = path_parts
            if (subject_ids is not None and
                subj_id is not SUMMARY_NAME and
                    subj_id not in subject_ids):
                continue
            if (visit_ids is not None and
                visit_id is not SUMMARY_NAME and
                    visit_id not in visit_ids):
                continue
            if subj_id == SUMMARY_NAME and visit_id == SUMMARY_NAME:
                multiplicity = 'per_project'
            elif subj_id == SUMMARY_NAME:
                multiplicity = 'per_visit'
                all_visit_ids.add(visit_id)
            elif visit_id == SUMMARY_NAME:
                multiplicity = 'per_subject'
            else:
                multiplicity = 'per_session'
                all_visit_ids.add(visit_id)
            datasets = []
            fields = {}
            for fname in sorted(fnames):
                if fname.startswith(FIELDS_FNAME):
                    continue
                datasets.append(
                    Dataset.from_path(
                        os.path.join(session_path, fname),
                        multiplicity=multiplicity))
            if FIELDS_FNAME in fnames:
                fields = self.fields_from_json(os.path.join(
                    session_path, FIELDS_FNAME),
                    multiplicity=multiplicity)
            datasets = sorted(datasets)
            fields = sorted(fields)
            if multiplicity == 'per_session':
                all_sessions[subj_id][visit_id] = Session(
                    subject_id=subj_id, visit_id=visit_id,
                    datasets=datasets, fields=fields)
            else:
                summaries[subj_id][visit_id] = (datasets, fields)
        subjects = []
        for subj_id, subj_sessions in all_sessions.items():
            try:
                datasets, fields = summaries[subj_id][SUMMARY_NAME]
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
                datasets, fields = summaries[SUMMARY_NAME][visit_id]
            except KeyError:
                datasets = []
                fields = []
            visits.append(Visit(visit_id, sorted(visit_sessions),
                                datasets, fields))
        try:
            datasets, fields = summaries[SUMMARY_NAME][SUMMARY_NAME]
        except KeyError:
            datasets = []
            fields = []
        return Project(sorted(subjects), sorted(visits), datasets,
                       fields)

    def retrieve_dicom_tags(self, dataset):
        with open(dataset.path) as f:
            return pydicom.dcmread(f)

    @classmethod
    def _check_only_dirs(cls, dirs, path):
        if any(not os.path.isdir(os.path.join(path, d))
               for d in dirs):
            raise NiAnalysisError(
                "Files found in local archive directory '{}' "
                "('{}') instead of sub-directories".format(
                    path, "', '".join(dirs)))

    def all_session_ids(self, project_id):
        project = self.project(project_id)
        return chain(*[
            (s.id for s in subj.sessions) for subj in project.subjects])

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

    def fields_from_json(self, fname, multiplicity):
        with open(fname) as f:
            dct = json.load(f)
        return [Field(name=k, value=v, multiplicity=multiplicity)
                for k, v in dct.items()]
