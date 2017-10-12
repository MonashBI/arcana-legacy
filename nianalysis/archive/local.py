from abc import ABCMeta, abstractmethod
import os.path
from collections import defaultdict
from itertools import chain
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
from nipype.interfaces.base import (
    Directory, isdefined)
from .base import Project, Subject, Session, Visit
from nianalysis.dataset import Dataset
from nianalysis.exceptions import NiAnalysisError
from nianalysis.data_formats import data_formats
from nianalysis.utils import split_extension
from nianalysis.utils import PATH_SUFFIX, FIELD_SUFFIX


logger = logging.getLogger('NiAnalysis')

SUMMARY_NAME = 'ALL'
FIELDS_FNAME = 'fields.json'


class LocalSourceInputSpec(ArchiveSourceInputSpec):

    base_dir = Directory(
        exists=True, desc=("Path to the base directory where the datasets will"
                           " be cached before uploading"))


class LocalNodeMixin(object):

    def _get_data_dir(self, multiplicity):
        project_dir = os.path.join(self.inputs.base_dir,
                                   str(self.inputs.project_id))
        subject_dir = os.path.join(project_dir, str(self.inputs.subject_id))
        if multiplicity == 'per_project':
            data_dir = os.path.join(project_dir, SUMMARY_NAME, SUMMARY_NAME)
        elif multiplicity.startswith('per_subject'):
            data_dir = os.path.join(subject_dir, SUMMARY_NAME)
        elif multiplicity.startswith('per_visit'):
            data_dir = os.path.join(project_dir, SUMMARY_NAME,
                                    str(self.inputs.visit_id))
        elif multiplicity.startswith('per_session'):
            data_dir = os.path.join(subject_dir, str(self.inputs.visit_id))
        else:
            assert False, "Unrecognised multiplicity '{}'".format(
                multiplicity)
        return data_dir

    def _get_fields_dict(self, multiplicity):
        try:
            cache = self.fields_cache
        except AttributeError:
            cache = self.fields_cache = {}
        try:
            fields = cache[multiplicity]
        except KeyError:
            data_dir = self._get_data_dir(multiplicity)
            try:
                with open(os.path.join(data_dir, FIELDS_FNAME)) as f:
                    fields = json.load(f)
            except IOError as e:
                if e.errno == errno.ENOENT:
                    fields = {}
                else:
                    raise
            cache[multiplicity] = fields
        return fields


class LocalSource(ArchiveSource, LocalNodeMixin):

    input_spec = LocalSourceInputSpec

    def _list_outputs(self):
        # Directory that holds session-specific
        outputs = {}
        for (name, dataset_format,
             multiplicity, _, is_spec) in self.inputs.datasets:
            ext = data_formats[dataset_format].extension
            fname = name + (ext if ext is not None else '')
            fname = self.prefixed_name(fname, is_spec)
            outputs[name + PATH_SUFFIX] = os.path.join(
                self._get_data_dir(multiplicity), fname)
        for (name, dtype, multiplicity, _, is_spec) in self.inputs.fields:
            fields = self._get_fields_dict(multiplicity)
            outputs[name + FIELD_SUFFIX] = dtype(
                fields[self.prefixed_name(name, is_spec)])
        return outputs


class LocalSinkInputSpecMixin(object):

    base_dir = Directory(
        exists=True, desc=("Path to the base directory where the datasets will"
                           " be cached before uploading"))


class LocalSinkInputSpec(ArchiveSinkInputSpec, LocalSinkInputSpecMixin):

    pass


class LocalSubjectSinkInputSpec(ArchiveSubjectSinkInputSpec,
                                LocalSinkInputSpecMixin):
    pass


class LocalVisitSinkInputSpec(ArchiveVisitSinkInputSpec,
                                  LocalSinkInputSpecMixin):
    pass


class LocalProjectSinkInputSpec(ArchiveProjectSinkInputSpec,
                                LocalSinkInputSpecMixin):
    pass


class LocalSinkMixin(LocalNodeMixin):

    __metaclass = ABCMeta
    input_spec = LocalSinkInputSpec

    def __init__(self, *args, **kwargs):
        super(LocalSinkMixin, self).__init__(*args, **kwargs)
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
        for (name, dataset_format, mult, processed, _) in self.inputs.datasets:
            assert processed, (
                "Should only be sinking processed datasets, not '{}'"
                .format(name))
            filename = getattr(self.inputs, name + PATH_SUFFIX)
            ext = data_formats[dataset_format].extension
            if not isdefined(filename):
                missing_files.append(name)
                continue  # skip the upload for this file
            assert (split_extension(filename)[1] == ext), (
                "Mismatching extension '{}' for format '{}' ('{}')"
                .format(split_extension(filename)[1],
                        data_formats[dataset_format].name, ext))
            assert mult in self.ACCEPTED_MULTIPLICITIES
            # Copy to local system
            src_path = os.path.abspath(filename)
            out_fname = self.prefixed_name(
                name + (ext if ext is not None else ''))
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
        for (name, dtype, mult, _, _) in self.inputs.fields:
            fields = self._get_fields_dict(mult)
            value = getattr(self.inputs, name + FIELD_SUFFIX)
            qual_name = self.prefixed_name(name)
            assert isinstance(value, dtype)
            fields[qual_name] = value
            out_fields.append((qual_name, value))
        outputs['out_fields'] = out_fields
        # Save updated fields dicts to files
        for mult, fields in self.fields_cache.iteritems():
            with open(os.path.join(self._get_data_dir(mult),
                                   FIELDS_FNAME), 'w') as f:
                json.dump(fields, f)
        return outputs

    @abstractmethod
    def _get_output_path(self):
        "Get the output path to save the generated datasets into"


class LocalSink(LocalSinkMixin, ArchiveSink):

    input_spec = LocalSinkInputSpec

    def _get_output_path(self):
        return [
            self.inputs.base_dir, self.inputs.project_id,
            self.inputs.subject_id, self.inputs.visit_id]


class LocalSubjectSink(LocalSinkMixin, ArchiveSubjectSink):

    input_spec = LocalSubjectSinkInputSpec

    def _get_output_path(self):
        return [
            self.inputs.base_dir, self.inputs.project_id,
            self.inputs.subject_id, SUMMARY_NAME]


class LocalVisitSink(LocalSinkMixin, ArchiveVisitSink):

    input_spec = LocalVisitSinkInputSpec

    def _get_output_path(self):
        return [
            self.inputs.base_dir, self.inputs.project_id,
            SUMMARY_NAME, self.inputs.visit_id]


class LocalProjectSink(LocalSinkMixin, ArchiveProjectSink):

    input_spec = LocalProjectSinkInputSpec

    def _get_output_path(self):
        return [
            self.inputs.base_dir, self.inputs.project_id, SUMMARY_NAME,
            SUMMARY_NAME]


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
        source = super(LocalArchive, self).source(*args, **kwargs)
        source.inputs.base_dir = self.base_dir
        return source

    def sink(self, *args, **kwargs):
        sink = super(LocalArchive, self).sink(*args, **kwargs)
        sink.inputs.base_dir = self.base_dir
        return sink

    def project(self, project_id, subject_ids=None, visit_ids=None):
        """
        Return subject and session information for a project in the local
        archive

        Parameters
        ----------
        project_id : str
            ID of the project to inspect
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
        project_dir = os.path.join(self.base_dir, str(project_id))
        subjects = []
        subject_dirs = [d for d in os.listdir(project_dir)
                        if not d.startswith('.') and d != SUMMARY_NAME]
        if subject_ids is not None:
            # Ensure ids are strings
            subject_ids = [str(i) for i in subject_ids]
        if subject_ids is not None:
            if any(subject_id not in subject_dirs
                   for subject_id in subject_ids):
                raise NiAnalysisError(
                    "'{}' subject(s) is/are missing from '{}' project in local"
                    " archive at '{}' (found '{}')".format(
                        "', '".join(set(subject_ids) - set(subject_dirs)),
                        project_id, self._base_dir, "', '".join(subject_dirs)))
            subject_dirs = subject_ids
        self._check_only_dirs(subject_dirs, project_dir)
        visit_sessions = defaultdict(list)
        for subject_id in subject_dirs:
            subject_path = os.path.join(project_dir, subject_id)
            sessions = []
            session_dirs = [d for d in os.listdir(subject_path)
                            if (not d.startswith('.') and d != SUMMARY_NAME)]
            if visit_ids is not None:
                if any(visit_id not in session_dirs
                       for visit_id in visit_ids):
                    raise NiAnalysisError(
                        "'{}' sessions(s) is/are missing from '{}' subject of "
                        "'{}' project in local archive (found '{}')"
                        .format("', '".join(visit_ids), subject_id,
                                project_id, "', '".join(session_dirs)))
                session_dirs = visit_ids
            self._check_only_dirs(session_dirs, subject_path)
            # Get datasets in all sessions
            for visit_id in session_dirs:
                session_path = os.path.join(subject_path, visit_id)
                datasets = []
                files = [d for d in os.listdir(session_path)
                         if not d.startswith('.')]
                for f in files:
                    datasets.append(
                        Dataset.from_path(os.path.join(session_path, f)))
                session = Session(subject_id=subject_id,
                                  visit_id=visit_id, datasets=datasets)
                sessions.append(session)
                visit_sessions[visit_id].append(session)
            # Get subject summary datasets
            subject_summary_path = self.subject_summary_path(project_id,
                                                             subject_id)
            subj_datasets = []
            if os.path.exists(subject_summary_path):
                files = [d for d in os.listdir(subject_summary_path)
                         if not d.startswith('.')]
                for f in files:
                    subj_datasets.append(
                        Dataset.from_path(
                            os.path.join(subject_summary_path, f),
                            multiplicity='per_subject'))
            subjects.append(Subject(subject_id=subject_id, sessions=sessions,
                                    datasets=subj_datasets))
        # Get visits
        visits = []
        for visit_id, sessions in visit_sessions.iteritems():
            visit_summary_path = self.visit_summary_path(project_id, visit_id)
            if os.path.exists(visit_summary_path):
                files = [d for d in os.listdir(visit_summary_path)
                         if not d.startswith('.')]
                visit_datasets = []
                for f in files:
                    visit_datasets.append(
                        Dataset.from_path(
                            os.path.join(visit_summary_path, f),
                            multiplicity='per_visit'))
            else:
                visit_datasets = []
            visits.append(Visit(visit_id, sessions, visit_datasets))
        # Get project summary datasets
        proj_summary_path = self.project_summary_path(project_id)
        if os.path.exists(proj_summary_path):
            files = [d for d in os.listdir(proj_summary_path)
                     if not d.startswith('.')]
            proj_datasets = []
            for f in files:
                proj_datasets.append(
                    Dataset.from_path(
                        os.path.join(proj_summary_path, f),
                        multiplicity='per_project'))
        else:
            proj_datasets = []
        project = Project(project_id, subjects, visits, proj_datasets)
        return project

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
