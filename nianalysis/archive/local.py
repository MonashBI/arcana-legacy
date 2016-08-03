import os.path
from .base import (
    Archive, ArchiveSource, ArchiveSink, ArchiveSourceInputSpec,
    ArchiveSinkInputSpec)
import stat
import shutil
import logging
from nipype.interfaces.base import (
    Directory, isdefined)
from .base import Session
from nianalysis.exceptions import NiAnalysisError
from nianalysis.formats import scan_formats


logger = logging.getLogger('NiAnalysis')


class LocalSourceInputSpec(ArchiveSourceInputSpec):

    base_dir = Directory(
        exists=True, desc=("Path to the base directory where the files will"
                           " be cached before uploading"))


class LocalSource(ArchiveSource):

    input_spec = LocalSourceInputSpec

    PROJECT_SUMMARY_DIR = '__PROJECT_SUMMARY__'
    SUBJECT_SUMMARY_DIR = '__SUBJECT_SUMMARY__'

    def _list_outputs(self):
        # Directory that holds session-specific
        base_subject_dir = os.path.join(*(str(p) for p in (
            self.inputs.base_dir, self.inputs.project_id,
            self.inputs.session[0])))
        session_dir = os.path.join(base_subject_dir,
                                   str(self.inputs.session[1]))
        subject_dir = os.path.join(base_subject_dir, self.SUBJECT_SUMMARY_DIR)
        project_dir = os.path.join(self.inputs.base_dir,
                                   self.PROJECT_SUMMARY_DIR)
        outputs = {}
        for name, scan_format, multiplicity, _ in self.inputs.files:
            if multiplicity == 'per_project':
                download_dir = project_dir
            elif multiplicity.startswith('per_subject'):
                download_dir = subject_dir
            elif multiplicity.startswith('per_session'):
                download_dir = session_dir
            else:
                assert False, "Unrecognised multiplicity '{}'".format(
                    multiplicity)
            fname = name + scan_formats[scan_format].extension
            outputs[fname] = os.path.join(download_dir, fname)
        return outputs


class LocalSinkInputSpec(ArchiveSinkInputSpec):

    base_dir = Directory(
        exists=True, desc=("Path to the base directory where the files will"
                           " be cached before uploading"))


class LocalSink(ArchiveSink):

    input_spec = LocalSinkInputSpec

    def _list_outputs(self):
        """Execute this module.
        """
        # Initiate outputs
        outputs = self.output_spec().get()
        out_files = []
        missing_files = []
        # Get cache dir for study
        out_dir = os.path.abspath(os.path.join(*(str(d) for d in (
            self.inputs.base_dir, self.inputs.project_id,
            self.inputs.session[0], self.inputs.session[1]))))
        # Make study cache dir
        if not os.path.exists(out_dir):
            os.makedirs(out_dir, stat.S_IRWXU | stat.S_IRWXG)
        # Loop through files connected to the sink and copy them to the
        # cache directory and upload to daris.
        for name, (scan_name, scan_format, _,
                   multiplicity) in self.inputs._outputs.iteritems():
            filename = scan_name + scan_formats[scan_format].extension
            if not isdefined(filename):
                raise NiAnalysisError(
                    "Previous node returned undefined input to Local sink for "
                    "'{}' output".format(name))
            src_path = os.path.abspath(filename)
            if not isdefined(src_path):
                missing_files.append((name, src_path))
                continue  # skip the upload for this file
            # Copy to local cache
            dst_path = os.path.join(out_dir, name)
            out_files.append(dst_path)
            shutil.copyfile(src_path, dst_path)
        if missing_files:
            # FIXME: Not sure if this should be an exception or not,
            #        indicates a problem but stopping now would throw
            #        away the files that were created
            logger.warning(
                "Missing output files '{}' mapped to names '{}' in "
                "DarisSink".format("', '".join(f for _, f in missing_files),
                                   "', '".join(n for n, _ in missing_files)))
        # Return cache file paths
        outputs['out_file'] = out_files
        return outputs


class LocalArchive(Archive):
    """
    Abstract base class for all Archive systems, DaRIS, XNAT and local file
    system. Sets out the interface that all Archive classes should implement.
    """

    type = 'local'
    Source = LocalSource
    Sink = LocalSink

    def __init__(self, base_dir):
        if not os.path.exists(base_dir):
            raise NiAnalysisError(
                "Base directory for LocalArchive '{}' does not exist"
                .format(base_dir))
        self._base_dir = os.path.abspath(base_dir)

    def __repr__(self):
        return "LocalArchive(base_dir='{}')".format(self.base_dir)

    def source(self, project_id, input_files):
        source = super(LocalArchive, self).source(project_id, input_files)
        source.inputs.base_dir = self.base_dir
        return source

    def sink(self, project_id):
        sink = super(LocalArchive, self).sink(project_id)
        sink.inputs.base_dir = self.base_dir
        return sink

    def all_sessions(self, project_id, study_id=None):
        project_dir = os.path.join(self.base_dir, str(project_id))
        sessions = []
        for subject_dir in os.listdir(project_dir):
            if not subject_dir.startswith('.'):
                study_dirs = [
                    d for d in os.listdir(os.path.join(project_dir,
                                                       subject_dir))
                    if not d.startswith('.')]
                if study_id is not None:
                    try:
                        study_ids = [study_id]
                    except TypeError:
                        study_ids = study_id
                    study_dirs = [d for d in study_dirs if d in study_ids]
                if not all(os.path.isdir(os.path.join(project_dir,
                                                      subject_dir, d))
                           for d in study_dirs):
                    raise NiAnalysisError(
                        "Files found in local archive subject directory '{}' "
                        "('{}') instead of study directories".format(
                            os.path.join(project_dir, subject_dir),
                            "', '".join(
                                d for d in study_dirs
                                if not os.path.isdir(os.path.join(
                                    project_dir, subject_dir, d)))))
                sessions.extend(Session(subject_dir, study_dir)
                                for study_dir in study_dirs)
        return sessions

    def sessions_with_file(self, scan, project_id, sessions=None):
        if sessions is None:
            sessions = self.all_sessions(project_id)
        with_dataset = []
        for session in sessions:
            if os.path.exists(
                os.path.join(self._base_dir, str(project_id),
                             session.subject_id, session.study_id,
                             scan.filename)):
                with_dataset.append(session)
        return with_dataset

    @property
    def base_dir(self):
        return self._base_dir
