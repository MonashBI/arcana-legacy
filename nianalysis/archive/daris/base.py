import os.path
import shutil
from abc import ABCMeta, abstractmethod
import subprocess
from copy import copy
import stat
import tempfile
import logging
from collections import defaultdict
from lxml import etree
from nipype.interfaces.base import (
    Directory, traits, isdefined)
from nianalysis.exceptions import (
    DarisException, DarisNameNotFoundException)
from nianalysis.archive.base import (
    Archive, ArchiveSource, ArchiveSink, ArchiveSourceInputSpec,
    ArchiveSinkInputSpec, ArchiveSubjectSinkInputSpec,
    ArchiveProjectSinkInputSpec, Session, Subject, Project, ArchiveSubjectSink,
    ArchiveProjectSink)
from nianalysis.utils import PATH_SUFFIX
from nianalysis.data_formats import data_formats
import re
import collections
from nianalysis.utils import split_extension
from .login import DarisLogin


logger = logging.getLogger('NiAnalysis')

SUBJECT_SUMMARY_ID = 3
PROJECT_SUMMARY_ID = 4


class DarisSourceInputSpec(ArchiveSourceInputSpec):

    repo_id = traits.Int(2, mandatory=True, usedefault=True,  # @UndefinedVariable @IgnorePep8
                         desc='The ID of the repository')
    cache_dir = Directory(
        exists=True, desc=("Path to the base directory where the downloaded"
                           "datasets will be cached"))
    server = traits.Str('mf-erc.its.monash.edu.au', mandatory=True,  # @UndefinedVariable @IgnorePep8
                        usedefault=True, desc="The address of the MF server")
    domain = traits.Str('monash-ldap', mandatory=True, usedefault=True,  # @UndefinedVariable @IgnorePep8
                        desc="The domain of the username/password")
    user = traits.Str(None, mandatory=True, usedefault=True,  # @UndefinedVariable @IgnorePep8
                      desc="The DaRIS username to log in with")
    password = traits.Password(None, mandatory=True, usedefault=True,  # @UndefinedVariable @IgnorePep8
                               desc="The password of the DaRIS user")


class DarisSource(ArchiveSource):
    """
    A NiPype IO interface for grabbing datasets off DaRIS (analogous to
    DataGrabber)
    """

    input_spec = DarisSourceInputSpec

    def _list_outputs(self):
        with DarisLogin(server=self.inputs.server,
                          domain=self.inputs.domain,
                          user=self.inputs.user,
                          password=self.inputs.password) as daris:
            outputs = {}
            datasets = {}
            for mult_proc_tple in (('per_session', False),
                                   ('per_session', True),
                                   ('per_subject', True),
                                   ('per_project', True)):
                (ex_method_id,
                 subject_id, session_id) = self._get_daris_ids(*mult_proc_tple)
                datasets[mult_proc_tple] = dict(
                    (d.name, d) for d in daris.get_datasets(
                        repo_id=self.inputs.repo_id,
                        project_id=self.inputs.project_id,
                        subject_id=subject_id,
                        ex_method_id=ex_method_id,
                        session_id=session_id).itervalues())
            base_cache_dir = os.path.join(*(str(p) for p in (
                self.inputs.cache_dir, self.inputs.repo_id,
                self.inputs.project_id)))
            for (name, dataset_format,
                 mult, processed, _) in self.inputs.datasets:
                (ex_method_id,
                 subject_id, session_id) = self._get_daris_ids(mult, processed)

                cache_dir = os.path.join(base_cache_dir, str(subject_id),
                                         str(ex_method_id), str(session_id))
                if not os.path.exists(cache_dir):
                    os.makedirs(cache_dir)
                fname = name + data_formats[dataset_format].extension
                try:
                    dataset = datasets[(mult, processed)][fname]
                except KeyError:
                    # The extension is not always saved in the filename
                    dataset = datasets[(mult, processed)][name]
                cache_path = os.path.join(cache_dir, fname)
                if not os.path.exists(cache_path):
                    daris.download(
                        cache_path, repo_id=self.inputs.repo_id,
                        project_id=self.inputs.project_id,
                        subject_id=subject_id,
                        ex_method_id=ex_method_id,
                        session_id=session_id,
                        dataset_id=dataset.id)
                outputs[name + PATH_SUFFIX] = cache_path
        return outputs

    def _get_daris_ids(self, multiplicity, processed):
        """
        Returns the ex-method ID, subject ID and session ID for a given
        multiplicity (i.e. 'per_session', 'per_subject' or 'per_project')
        whether the input dataset is processed or not

        Parameters
        ----------
        multiplicity : str
            The "multiplicity" of the input dataset, one of 'per_session',
            'per_subject' or 'per_project'
        processed: bool
            Whether the dataset is processed (by this package) or not

        Returns
        -------
        ex_method_id : int
            DaRIS ex-method ID
        subject_id : int
            DaRIS subject ID
        session_id : int
            DaRIS session ID
        """
        if multiplicity == 'per_session':
            ex_method_id = int(processed) + 1
            session_id = self.inputs.session_id
            subject_id = self.inputs.subject_id
        elif multiplicity == 'per_subject':
            ex_method_id = SUBJECT_SUMMARY_ID
            session_id = 1
            subject_id = self.inputs.subject_id
        elif multiplicity == 'per_project':
            ex_method_id = PROJECT_SUMMARY_ID
            session_id = 1
            subject_id = 1
        else:
            assert False, "unrecognised multiplicity {}".format(multiplicity)
        return ex_method_id, subject_id, session_id


class DarisSinkInputSpecMixin(object):

    repo_id = traits.Int(2, mandatory=True, usedefault=True,  # @UndefinedVariable @IgnorePep8
                         desc='The ID of the repository')
    cache_dir = Directory(
        exists=True, desc=("Path to the base directory where the datasets will"
                           " be cached before uploading"))
    server = traits.Str('mf-erc.its.monash.edu.au', mandatory=True,  # @UndefinedVariable @IgnorePep8
                        usedefault=True, desc="The address of the MF server")
    domain = traits.Str('monash-ldap', mandatory=True, usedefault=True,  # @UndefinedVariable @IgnorePep8
                        desc="The domain of the username/password")
    user = traits.Str(None, mandatory=True, usedefault=True,  # @UndefinedVariable @IgnorePep8
                      desc="The DaRIS username to log in with")
    password = traits.Password(None, mandatory=True, usedefault=True,  # @UndefinedVariable @IgnorePep8
                               desc="The password of the DaRIS user")


class DarisSinkInputSpec(ArchiveSinkInputSpec, DarisSinkInputSpecMixin):

    pass


class DarisSubjectSinkInputSpec(ArchiveSubjectSinkInputSpec,
                                DarisSinkInputSpecMixin):

    pass


class DarisProjectSinkInputSpec(ArchiveProjectSinkInputSpec,
                                DarisSinkInputSpecMixin):

    pass


class DarisSinkMixin(object):
    """
    A NiPype IO interface for putting processed datasets onto DaRIS (analogous
    to DataSink)
    """

    __metaclass__ = ABCMeta

    def _list_outputs(self):
        """Execute this module.
        """
        # Initiate outpu
        outputs = self._base_outputs()
        out_files = []
        missing_files = []
        # Get the ex-method, subject and session IDs specific to the sink
        # multiplicity (overridden in derived classes)
        ex_method_id, subject_id, session_id = self._get_daris_ids()
        # Open DaRIS session
        with DarisLogin(server=self.inputs.server,
                          domain=self.inputs.domain,
                          user=self.inputs.user,
                          password=self.inputs.password) as daris:
            # Add ex-method if not present
            if not daris.exists(project_id=self.inputs.project_id,
                                subject_id=subject_id,
                                ex_method_id=ex_method_id,
                                repo_id=self.inputs.repo_id):
                daris.add_ex_method(
                    project_id=self.inputs.project_id,
                    subject_id=subject_id,
                    repo_id=self.inputs.repo_id,
                    ex_method_id=ex_method_id)
            # Add session if not present
            if not daris.exists(project_id=self.inputs.project_id,
                                subject_id=subject_id,
                                session_id=session_id,
                                ex_method_id=ex_method_id,
                                repo_id=self.inputs.repo_id):
                # Add session to hold output
                daris.add_session(
                    project_id=self.inputs.project_id,
                    subject_id=subject_id,
                    session_id=session_id,
                    repo_id=self.inputs.repo_id,
                    ex_method_id=ex_method_id, name=self.inputs.name,
                    description=self.inputs.description)
            # Get cache dir for session
            out_dir = os.path.abspath(os.path.join(*(str(d) for d in (
                self.inputs.cache_dir, self.inputs.repo_id,
                self.inputs.project_id, subject_id, ex_method_id,
                session_id))))
            # Make session cache dir
            if not os.path.exists(out_dir):
                os.makedirs(out_dir, stat.S_IRWXU | stat.S_IRWXG)
            # Loop through datasets connected to the sink and copy them to the
            # cache directory and upload to daris.
            for (name, format_name, mult,
                 processed, _) in self.inputs.datasets:
                assert mult in self.ACCEPTED_MULTIPLICITIES
                assert processed, ("{} (format: {}, mult: {}) isn't processed"
                                   .format(name, format_name, mult))
                filename = getattr(self.inputs, name + PATH_SUFFIX)
                if not isdefined(filename):
                    missing_files.append(name)
                    continue  # skip the upload for this file
                dataset_format = data_formats[format_name]
                assert (
                    split_extension(filename)[1] ==
                    dataset_format.extension), (
                    "Mismatching extension '{}' for format '{}' ('{}')"
                    .format(split_extension(filename)[1],
                            data_formats[format_name].name,
                            dataset_format.extension))
                src_path = os.path.abspath(filename)
                # Copy to local cache
                dst_path = os.path.join(out_dir,
                                        name + dataset_format.extension)
                out_files.append(dst_path)
                shutil.copyfile(src_path, dst_path)
                # Upload to DaRIS
                dataset_id = daris.add_dataset(
                    project_id=self.inputs.project_id,
                    subject_id=subject_id,
                    repo_id=self.inputs.repo_id, ex_method_id=ex_method_id,
                    session_id=session_id, name=name,
                    description="Uploaded from DarisSink")
                daris.upload(
                    src_path, project_id=self.inputs.project_id,
                    subject_id=subject_id,
                    repo_id=self.inputs.repo_id, ex_method_id=ex_method_id,
                    session_id=session_id, dataset_id=dataset_id,
                    lctype=dataset_format.lctype)
        if missing_files:
            # FIXME: Not sure if this should be an exception or not,
            #        indicates a problem but stopping now would throw
            #        away the datasets that were created
            logger.warning(
                "Missing output datasets '{}' in DarisSink".format(
                    "', '".join(str(f) for f in missing_files)))
        # Return cache file paths
        outputs['out_files'] = out_files
        return outputs

    @abstractmethod
    def _get_daris_ids(self):
        "Return the daris IDS for the session corresponding to this sink"


class DarisSink(DarisSinkMixin, ArchiveSink):

    input_spec = DarisSinkInputSpec

    def _get_daris_ids(self):
        ex_method_id = 2
        subject_id = self.inputs.subject_id
        session_id = self.inputs.session_id
        return ex_method_id, subject_id, session_id


class DarisSubjectSink(DarisSinkMixin, ArchiveSubjectSink):

    input_spec = DarisSubjectSinkInputSpec

    def _get_daris_ids(self):
        ex_method_id = SUBJECT_SUMMARY_ID
        subject_id = self.inputs.subject_id
        session_id = 1
        return ex_method_id, subject_id, session_id


class DarisProjectSink(DarisSinkMixin, ArchiveProjectSink):

    input_spec = DarisProjectSinkInputSpec

    def _get_daris_ids(self):
        ex_method_id = PROJECT_SUMMARY_ID
        subject_id = 1
        session_id = 1
        return ex_method_id, subject_id, session_id


class DarisArchive(Archive):
    """
    An 'Archive' class for the DaRIS research management system.
    """

    type = 'daris'
    Sink = DarisSink
    Source = DarisSource
    SubjectSink = DarisSubjectSink
    ProjectSink = DarisProjectSink

    def __init__(self, user, password, cache_dir=None, repo_id=2,
                 server='mf-erc.its.monash.edu.au', domain='monash-ldap'):
        self._server = server
        self._domain = domain
        self._user = user
        self._password = password
        if cache_dir is None:
            self._cache_dir = os.path.join(os.getcwd(), '.daris')
        else:
            self._cache_dir = cache_dir
        self._repo_id = repo_id

    def source(self, *args, **kwargs):
        source = super(DarisArchive, self).source(*args, **kwargs)
        source.inputs.server = self._server
        source.inputs.domain = self._domain
        source.inputs.user = self._user
        source.inputs.password = self._password
        source.inputs.cache_dir = self._cache_dir
        source.inputs.repo_id = self._repo_id
        return source

    def sink(self, *args, **kwargs):
        sink = super(DarisArchive, self).sink(*args, **kwargs)
        sink.inputs.server = self._server
        sink.inputs.domain = self._domain
        sink.inputs.user = self._user
        sink.inputs.password = self._password
        sink.inputs.cache_dir = self._cache_dir
        sink.inputs.repo_id = self._repo_id
        return sink

    def all_sessions(self, project_id, session_id=None):
        """
        Parameters
        ----------
        project_id : int
            The project id to return the sessions for
        repo_id : int
            The id of the repository (2 for monash daris)
        session_ids: int|List[int]|None
            Id or ids of sessions of which to return sessions for. If None all
            are returned
        """
        with self._daris() as daris:
            entries = daris.get_sessions(self, project_id,
                                         repo_id=self._repo_id)
            if session_id is not None:
                # Attempt to convert session_ids into a single int and then
                # wrap in a list in case session ids is a single integer (or
                # string representation of an integer)
                try:
                    session_ids = [int(session_id)]
                except TypeError:
                    session_ids = session_id
                entries = [e for e in entries if e.id in session_ids]
        return [Session(subject_id=e.cid.split('.')[-3], session_id=e.id)
                for e in entries]

    def project(self, project_id, subject_ids=None, session_ids=None):
        with self._daris() as daris:
            # Get all datasets in project
            entries = daris.get_datasets(
                self, project_id, repo_id=self._repo_id, subject_id=None,
                session_id=None, ex_method_id=None)
        subject_dict = defaultdict(defaultdict(defaultdict(list)))
        for entry in entries:
            (subject_id, ex_method_id,
             session_id, dataset_id) = entry.cid.split('.')[-4:]
            subject_dict[subject_id][ex_method_id][session_id].append(
                dataset_id)
        subjects = []
        project_summary = []
        for subject_id, method_dict in subject_dict.iteritems():
            if subject_ids is not None and subject_id not in subject_ids:
                continue
            if any(int(m) > 4 or int(m) < 0 for m in method_dict):
                raise DarisException(
                    "Unrecognised ex-method IDs {} found in subject {}"
                    .format(', '.join(str(m) for m in method_dict
                                      if int(m) > 4 or int(m) < 0),
                            subject_id))
            sessions = []
            subject_summary = []
            for ex_method_id, session_dict in method_dict.iteritems():
                processed = ex_method_id > 1
                if ex_method_id == 4:
                    if subject_id != 1 or session_dict.keys() != ['1']:
                        raise DarisException(
                            "Session(s) {} found in ex-method 4 of subject {}."
                            "Project summaries are only allowed in session 1 "
                            "of subject 1".format(', '.join(session_dict),
                                                  subject_id))
                    project_summary = session_dict['1']
                elif ex_method_id == 3:
                    if session_dict.keys() != ['1']:
                        raise DarisException(
                            "Session(s) {} found in ex-method 3 of subject {}."
                            "Subject summaries are only allowed in session 1"
                            .format(', '.join(session_dict), subject_id))
                    subject_summary = session_dict['1']
                else:
                    for session_id, datasets in session_dict.iteritems():
                        if session_ids is None or session_id in session_ids:
                            sessions.append(Session(session_id, datasets,
                                                    processed=processed))
            subjects.append(Subject(subject_id, sessions, subject_summary))
        project = Project(project_id, subjects, project_summary)
        return project

    def _daris(self):
        return DarisLogin(server=self._server, domain=self._domain,
                            user=self._user, password=self._password)

    @property
    def base_dir(self):
        return self._cache_dir


if __name__ == '__main__':
    daris = DarisLogin(domain='system', user='manager',
                         password='t0gp154sp!')
    with daris:
        daris.copy_session(135, 3, 2, new_project_id=144, new_subject_id=2,
                           new_session_id=1)
