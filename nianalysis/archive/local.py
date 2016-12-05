import os.path
from .base import (
    Archive, ArchiveSource, ArchiveSink, ArchiveSourceInputSpec,
    ArchiveSinkInputSpec, ArchiveSubjectSinkInputSpec,
    ArchiveProjectSinkInputSpec)
import stat
import shutil
import logging
from nipype.interfaces.base import (
    Directory, isdefined)
from .base import Project, Subject, Session
from nianalysis.dataset import Dataset
from nianalysis.exceptions import NiAnalysisError
from nianalysis.data_formats import data_formats
from nianalysis.utils import split_extension


logger = logging.getLogger('NiAnalysis')

PROJECT_SUMMARY_NAME = '__SUMMARY__'
SUBJECT_SUMMARY_NAME = '__SUMMARY__'


class LocalSourceInputSpec(ArchiveSourceInputSpec):

    base_dir = Directory(
        exists=True, desc=("Path to the base directory where the datasets will"
                           " be cached before uploading"))


class LocalSource(ArchiveSource):

    input_spec = LocalSourceInputSpec

    def _list_outputs(self):
        # Directory that holds session-specific
        base_subject_dir = os.path.join(*(str(p) for p in (
            self.inputs.base_dir, self.inputs.project_id,
            self.inputs.subject_id)))
        session_dir = os.path.join(base_subject_dir,
                                   self.inputs.session_id)
        subject_dir = os.path.join(base_subject_dir, SUBJECT_SUMMARY_NAME)
        project_dir = os.path.join(
            self.inputs.base_dir, self.inputs.project_id,
            PROJECT_SUMMARY_NAME)
        outputs = {}
        for name, dataset_format, multiplicity, _ in self.inputs.datasets:
            if multiplicity == 'per_project':
                download_dir = project_dir
            elif multiplicity.startswith('per_subject'):
                download_dir = subject_dir
            elif multiplicity.startswith('per_session'):
                download_dir = session_dir
            else:
                assert False, "Unrecognised multiplicity '{}'".format(
                    multiplicity)
            fname = name + data_formats[dataset_format].extension
            outputs[name + self.OUTPUT_SUFFIX] = os.path.join(download_dir,
                                                              fname)
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


class LocalProjectSinkInputSpec(ArchiveProjectSinkInputSpec,
                                LocalSinkInputSpecMixin):

    pass


class LocalSink(ArchiveSink):

    input_spec = LocalSinkInputSpec

    ACCEPTED_MULTIPLICITIES = ('per_session', 'per_session_subset')

    def _list_outputs(self):
        """Execute this module.
        """
        # Initiate outputs
        outputs = self.output_spec().get()
        out_files = []
        missing_files = []
        # Get cache dir for session
        out_dir = self._get_output_dir()
        # Make session cache dir
        if not os.path.exists(out_dir):
            os.makedirs(out_dir, stat.S_IRWXU | stat.S_IRWXG)
        # Loop through datasets connected to the sink and copy them to the
        # cache directory and upload to daris.
        for name, dataset_format, multiplicity, _ in self.inputs.datasets:
            filename = getattr(self.inputs, name + self.INPUT_SUFFIX)
            ext = data_formats[dataset_format].extension
            if not isdefined(filename):
                missing_files.append(name)
                continue  # skip the upload for this file
            assert (split_extension(filename)[1] == ext), (
                "Mismatching extension '{}' for format '{}' ('{}')"
                .format(split_extension(filename)[1],
                        data_formats[dataset_format].name, ext))
            assert isdefined(filename), (
                "Previous node returned undefined input to Local sink for "
                "'{}' output".format(name))
            assert multiplicity in self.ACCEPTED_MULTIPLICITIES
            # Copy to local store
            src_path = os.path.abspath(filename)
            dst_path = os.path.join(out_dir, name + (ext if ext is not None
                                                     else ''))
            out_files.append(dst_path)
            shutil.copyfile(src_path, dst_path)
        if missing_files:
            # FIXME: Not sure if this should be an exception or not,
            #        indicates a problem but stopping now would throw
            #        away the datasets that were created
            logger.warning(
                "Missing input datasets '{}' in DarisSink".format(
                    "', '".join(missing_files)))
        # Return cache file paths
        outputs['out_files'] = out_files
        return outputs

    def _get_output_dir(self):
        return os.path.abspath(os.path.join(*(str(d) for d in (
            self.inputs.base_dir, self.inputs.project_id,
            self.inputs.subject_id, self.inputs.session_id))))


class LocalSubjectSink(LocalSink):

    input_spec = LocalSubjectSinkInputSpec

    ACCEPTED_MULTIPLICITIES = ('per_subject', 'per_subject_subset')

    def _get_output_dir(self):
        return os.path.abspath(os.path.join(*(str(d) for d in (
            self.inputs.base_dir, self.inputs.project_id,
            self.inputs.subject_id, PROJECT_SUMMARY_NAME))))


class LocalProjectSink(LocalSink):

    input_spec = LocalProjectSinkInputSpec

    ACCEPTED_MULTIPLICITIES = ('per_project',)

    def _get_output_dir(self):
        return os.path.abspath(os.path.join(*(str(d) for d in (
            self.inputs.base_dir, self.inputs.project_id,
            SUBJECT_SUMMARY_NAME))))


class LocalArchive(Archive):
    """
    Abstract base class for all Archive systems, DaRIS, XNAT and local file
    system. Sets out the interface that all Archive classes should implement.
    """

    type = 'local'
    Source = LocalSource
    Sink = LocalSink
    SubjectSink = LocalSubjectSink
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

    def project(self, project_id, subject_ids=None, session_ids=None):
        project_dir = os.path.join(self.base_dir, str(project_id))
        subjects = []
        subject_dirs = [d for d in os.listdir(project_dir)
                        if not d.startswith('.') and d != PROJECT_SUMMARY_NAME]
        subject_ids = [str(i) for i in subject_ids]  # Ensure ids are strings
        if subject_ids is not None:
            if any(subject_id not in subject_dirs
                   for subject_id in subject_ids):
                raise NiAnalysisError(
                    "'{}' sujbect(s) is/are missing from '{}' project in local"
                    " archive at '{}' (found '{}')".format(
                        "', '".join(set(subject_ids) - set(subject_dirs)),
                        project_id, self._base_dir, "', '".join(subject_dirs)))
            subject_dirs = subject_ids
        self._check_only_dirs(subject_dirs, project_dir)
        for subject_dir in subject_dirs:
            subject_path = os.path.join(project_dir, subject_dir)
            sessions = []
            session_dirs = [d for d in os.listdir(subject_path)
                            if (not d.startswith('.') and
                                d != SUBJECT_SUMMARY_NAME)]
            if session_ids is not None:
                if any(session_id not in session_dirs
                       for session_id in session_ids):
                    raise NiAnalysisError(
                        "'{}' sessions(s) is/are missing from '{}' subject of "
                        "'{}' project in local archive (found '{}')"
                        .format("', '".join(session_ids), subject_dir,
                                project_id, "', '".join(session_dirs)))
                session_dirs = session_ids
            self._check_only_dirs(session_dirs, subject_path)
            for session_dir in session_dirs:
                session_path = os.path.join(subject_path, session_dir)
                datasets = []
                files = [d for d in os.listdir(session_path)
                            if not os.path.isdir(d)]
                for f in files:
                    datasets.append(
                        Dataset.from_path(os.path.join(session_path, f)))
                sessions.append(Session(session_dir, datasets))
            subject_summary_path = os.path.join(subject_path,
                                                SUBJECT_SUMMARY_NAME)
            if os.path.exists(subject_summary_path):
                files = [d for d in os.listdir(subject_summary_path)
                            if not os.path.isdir(d)]
                for f in files:
                    datasets.append(
                        Dataset.from_path(
                            os.path.join(subject_summary_path, f),
                            multiplicity='per_subject'))
            subjects.append(Subject(subject_dir, sessions, datasets))
        project_summary_path = os.path.join(project_dir, PROJECT_SUMMARY_NAME)
        if os.path.exists(project_summary_path):
            files = [d for d in os.listdir(project_summary_path)
                        if not os.path.isdir(d)]
            for f in files:
                datasets.append(
                    Dataset.from_path(os.path.join(project_summary_path, f),
                                      multiplicity='per_project'))
        project = Project(project_id, subjects, datasets)
        return project

    @classmethod
    def _check_only_dirs(cls, dirs, path):
        if any(not os.path.isdir(os.path.join(path, d))
               for d in dirs):
            raise NiAnalysisError(
                "Files found in local archive directory '{}' "
                "('{}') instead of sub-directories".format(
                    path, "', '".join(dirs)))

    def sessions_with_dataset(self, dataset, project_id, sessions=None):
        if sessions is None:
            sessions = self.all_sessions(project_id)
        with_dataset = []
        for session in sessions:
            if os.path.exists(
                os.path.join(self._base_dir, str(project_id),
                             session.subject_id, session.session_id,
                             dataset.filename)):
                with_dataset.append(session)
        return with_dataset

    @property
    def base_dir(self):
        return self._base_dir
