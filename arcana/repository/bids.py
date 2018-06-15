from __future__ import absolute_import
from builtins import str
from past.builtins import basestring
from builtins import object
from abc import ABCMeta, abstractmethod
import os.path
from collections import defaultdict
from itertools import chain, groupby
from operator import attrgetter
import errno
from .base import (
    Repository, RepositorySource, RepositorySink, RepositorySourceInputSpec,
    RepositorySinkInputSpec, RepositorySubjectSinkInputSpec,
    RepositoryVisitSinkInputSpec,
    RepositoryProjectSinkInputSpec, RepositorySubjectSink, RepositoryVisitSink,
    RepositoryProjectSink)
import stat
import shutil
import logging
import json
from bids import grabbids as gb
from fasteners import InterProcessLock
from arcana.utils import JSON_ENCODING
from nipype.interfaces.base import isdefined
from .tree import Project, Subject, Session, Visit
from arcana.dataset import Dataset, Field
from arcana.exception import ArcanaError
from arcana.utils import (
    split_extension, PATH_SUFFIX, FIELD_SUFFIX, NoContextWrapper)


logger = logging.getLogger('arcana')

SUMMARY_NAME = 'ALL'
FIELDS_FNAME = 'fields.json'

LOCK = '.lock'


def lower(s):
    if s is None:
        return None
    return s.lower()


class BidsNodeMixin(object):

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
        return (super(BidsNodeMixin, self).__eq__(other) and
                self.base_dir == other.base_dir)


class BidsSource(RepositorySource, BidsNodeMixin):

    input_spec = RepositorySourceInputSpec

    def __init__(self, study_name, datasets, fields, base_dir):
        self._base_dir = base_dir
        super(BidsSource, self).__init__(study_name, datasets, fields)

        if not isdefined(self.inputs.output_query):
            self.inputs.output_query = {"func": {"modality": "func"},
                                        "anat": {"modality": "anat"}}

        # If infields is empty, use all BIDS entities
        bids_config = os.path.join(os.path.dirname(gb.__file__),
                                   'config', 'bids.json')
        bids_config = json.load(open(bids_config, 'r'))
        infields = [i['name'] for i in bids_config['entities']]

        self._infields = infields or []

    def _list_outputs(self):
        layout = gb.BIDSLayout(self.inputs.base_dir)

        # If infield is not given nm input value, silently ignore
        filters = {}
        for key in self._infields:
            value = getattr(self.inputs, key)
            if isdefined(value):
                filters[key] = value

        outputs = {}
        for key, query in self.inputs.output_query.items():
            args = query.copy()
            args.update(filters)
            filelist = layout.get(return_type=self.inputs.return_type, **args)
            outputs[key] = filelist
        #======================================================================
        # 
        #======================================================================
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
                    fpath + LOCK,
                        logger=logger), open(fpath, 'r') as f:
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


class BidsSinkMixin(BidsNodeMixin):

    __metaclass = ABCMeta

    def __init__(self, study_name, datasets, fields, base_dir):
        self._base_dir = base_dir
        super(BidsSinkMixin, self).__init__(study_name, datasets,
                                             fields)
        BidsNodeMixin.__init__(self)

    def _list_outputs(self):
        """Execute this module.
        """
        # Initiate outputs
        outputs = self._base_outputs()
        out_files = []
        missing_files = []
        # Get output dir from base RepositorySink class (will change depending on
        # whether it is per session/subject/visit/project)
        out_path = self._get_output_path()
        out_dir = os.path.abspath(os.path.join(*out_path))
        # Make session dir
        if not os.path.exists(out_dir):
            os.makedirs(out_dir, stat.S_IRWXU | stat.S_IRWXG)
        # Loop through datasets connected to the sink and copy them to repository
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
                "Missing input datasets '{}' in BidsSink".format(
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
                with open(fpath, 'w', **JSON_ENCODING) as f:
                    json.dump(fields, f)
        outputs['out_fields'] = out_fields
        return outputs

    @abstractmethod
    def _get_output_path(self):
        "Get the output path to save the generated datasets into"


class BidsSink(BidsSinkMixin, RepositorySink):

    input_spec = RepositorySinkInputSpec

    def _get_output_path(self):
        return [
            self.base_dir, self.inputs.subject_id,
            self.inputs.visit_id]


class BidsSubjectSink(BidsSinkMixin, RepositorySubjectSink):

    input_spec = RepositorySubjectSinkInputSpec

    def _get_output_path(self):
        return [
            self.base_dir, self.inputs.subject_id, SUMMARY_NAME]


class BidsVisitSink(BidsSinkMixin, RepositoryVisitSink):

    input_spec = RepositoryVisitSinkInputSpec

    def _get_output_path(self):
        return [
            self.base_dir, SUMMARY_NAME, self.inputs.visit_id]


class BidsProjectSink(BidsSinkMixin, RepositoryProjectSink):

    input_spec = RepositoryProjectSinkInputSpec

    def _get_output_path(self):
        return [
            self.base_dir, SUMMARY_NAME, SUMMARY_NAME]


class BidsRepository(Repository):
    """
    An 'Repository' class for directories on the local file system organised
    into sub-directories by subject and then visit.

    Parameters
    ----------
    base_dir : str (path)
        Path to local directory containing data
    """

    type = 'local'
    Source = BidsSource
    Sink = BidsSink
    SubjectSink = BidsSubjectSink
    VisitSink = BidsVisitSink
    ProjectSink = BidsProjectSink

    def __init__(self, base_dir):
        if not os.path.exists(base_dir):
            raise ArcanaError(
                "Base directory for BidsRepository '{}' does not exist"
                .format(base_dir))
        self._base_dir = os.path.abspath(base_dir)

    def __repr__(self):
        return "BidsRepository(base_dir='{}')".format(self.base_dir)

    def __eq__(self, other):
        try:
            return self.base_dir == other.base_dir
        except AttributeError:
            return False

    def source(self, *args, **kwargs):
        source = super(BidsRepository, self).source(
            *args, base_dir=self.base_dir, **kwargs)
        return source

    def sink(self, *args, **kwargs):
        sink = super(BidsRepository, self).sink(
            *args, base_dir=self.base_dir, **kwargs)
        return sink

    def login(self):
        return NoContextWrapper(None)

    def get_tree(self, subject_ids=None, visit_ids=None):
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

        # Need to pull out all datasets and fields

        all_sessions[subj_id][visit_id] = Session(
            subject_id=subj_id, visit_id=visit_id,
            datasets=datasets, fields=fields)

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

    @classmethod
    def _check_only_dirs(cls, dirs, path):
        if any(not os.path.isdir(os.path.join(path, d))
               for d in dirs):
            raise ArcanaError(
                "Files found in local repository directory '{}' "
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
        with open(fname, 'r') as f:
            dct = json.load(f)
        return [Field(name=k, value=v, frequency=frequency,
                      subject_id=subject_id, visit_id=visit_id,
                      repository=self)
                for k, v in list(dct.items())]
