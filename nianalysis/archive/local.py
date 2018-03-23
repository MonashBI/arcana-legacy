from abc import ABCMeta, abstractmethod
import os.path
from collections import defaultdict
from itertools import chain, groupby
from operator import itemgetter
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
locking_logger = logging.getLogger('NiAnalysisLocking')

SUMMARY_NAME = 'ALL'
FIELDS_FNAME = 'fields.json'

LOCK = '.lock'


def lower(s):
    if s is None:
        return None
    return s.lower()


class LocalSourceInputSpec(ArchiveSourceInputSpec):

    base_dir = Directory(
        exists=True, desc=("Path to the base directory where the datasets will"
                           " be cached before uploading"))


class LocalNodeMixin(object):

    def _get_data_dir(self, multiplicity):
        project_dir = os.path.join(self.inputs.base_dir,
                                   str(self.inputs.project_id))
        if multiplicity == 'per_project':
            data_dir = os.path.join(project_dir, SUMMARY_NAME, SUMMARY_NAME)
        elif multiplicity.startswith('per_subject'):
            data_dir = os.path.join(
                project_dir, str(self.inputs.subject_id), SUMMARY_NAME)
        elif multiplicity.startswith('per_visit'):
            data_dir = os.path.join(project_dir, SUMMARY_NAME,
                                    str(self.inputs.visit_id))
        elif multiplicity.startswith('per_session'):
            data_dir = os.path.join(
                project_dir, str(self.inputs.subject_id),
                str(self.inputs.visit_id))
        else:
            assert False, "Unrecognised multiplicity '{}'".format(
                multiplicity)
        return data_dir

    def fields_path(self, multiplicity):
        return os.path.join(self._get_data_dir(multiplicity),
                            FIELDS_FNAME)


class LocalSource(ArchiveSource, LocalNodeMixin):

    input_spec = LocalSourceInputSpec

    def _list_outputs(self):
        # Directory that holds session-specific
        outputs = {}
        for (name, dataset_format,
             multiplicity, _, is_spec) in self.inputs.datasets:
            ext = data_formats[dataset_format].extension
            fname = name + (ext if ext is not None else '')
            fname = self.prefix_study_name(fname, is_spec)
            outputs[name + PATH_SUFFIX] = os.path.join(
                self._get_data_dir(multiplicity), fname)
        for mult, spec_grp in groupby(sorted(self.inputs.fields,
                                             key=itemgetter(2)),
                                      key=itemgetter(2)):
            # Load fields JSON, locking to prevent read/write conflicts
            # Would be better if only check locked
            fpath = self.fields_path(mult)
            try:
                with InterProcessLock(
                        fpath + LOCK,
                        logger=locking_logger), open(fpath) as f:
                    fields = json.load(f)
            except IOError as e:
                if e.errno == errno.ENOENT:
                    fields = {}
                else:
                    raise
            for name, dtype, _, _, is_spec in spec_grp:
                outputs[name + FIELD_SUFFIX] = dtype(
                    fields[self.prefix_study_name(name, is_spec)])
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
            if lower(split_extension(filename)[1]) != lower(ext):
                raise NiAnalysisError(
                    "Mismatching extension '{}' for format '{}' ('{}')"
                    .format(split_extension(filename)[1],
                            data_formats[dataset_format].name, ext))
            assert mult == self.multiplicity
            # Copy to local system
            src_path = os.path.abspath(filename)
            out_fname = self.prefix_study_name(
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
        fpath = self.fields_path(self.multiplicity)
        # Open fields JSON, locking to prevent other processes
        # reading or writing
        if self.inputs.fields:
            with InterProcessLock(fpath + LOCK,
                                  logger=locking_logger):
                try:
                    with open(fpath, 'rb') as f:
                        fields = json.load(f)
                except IOError as e:
                    if e.errno == errno.ENOENT:
                        fields = {}
                    else:
                        raise
                # Update fields JSON and write back to file.
                for spec in self.inputs.fields:
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
        project_dir = os.path.abspath(
            os.path.join(self.base_dir, str(project_id)))
        summaries = defaultdict(dict)
        all_sessions = defaultdict(dict)
        all_visit_ids = set()
        for session_path, _, all_fnames in os.walk(project_dir):
            fnames = [f for f in all_fnames if not f.startswith('.')]
            relpath = os.path.relpath(session_path, project_dir)
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
            try:
                visit_sessions = list(chain(
                    sess[visit_id] for sess in all_sessions.values()))
            except:
                raise
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
        return Project(project_id, sorted(subjects), sorted(visits),
                       datasets, fields)

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
