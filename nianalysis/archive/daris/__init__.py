import os.path
import shutil
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
    ArchiveProjectSinkInputSpec, Session, Subject, Project)
from nianalysis.data_formats import data_formats
import re
import collections
from nianalysis.utils import split_extension

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
            for mult_proc in (('per_session', False), ('per_session', True),
                              ('per_subject', True), ('per_project', True)):
                (ex_method_id,
                 subject_id, session_id) = self._get_daris_ids(*mult_proc)
                datasets[mult_proc] = dict(
                    (d.name, d) for d in daris.get_datasets(
                        repo_id=self.inputs.repo_id,
                        project_id=self.inputs.project_id,
                        subject_id=subject_id,
                        ex_method_id=ex_method_id,
                        session_id=session_id).itervalues())
            base_cache_dir = os.path.join(*(str(p) for p in (
                self.inputs.cache_dir, self.inputs.repo_id,
                self.inputs.project_id)))
            for (name, dataset_format, mult, processed) in self.inputs.datasets:
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
                outputs[name + self.OUTPUT_SUFFIX] = cache_path
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


class DarisSink(ArchiveSink):
    """
    A NiPype IO interface for putting processed datasets onto DaRIS (analogous to
    DataSink)
    """

    input_spec = DarisSinkInputSpec
    ACCEPTED_MULTIPLICITIES = ('per_session',)

    def _list_outputs(self):
        """Execute this module.
        """
        # Initiate outpu
        outputs = self.output_spec().get()
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
                self.inputs.project_id, subject_id, ex_method_id, session_id))))
            # Make session cache dir
            if not os.path.exists(out_dir):
                os.makedirs(out_dir, stat.S_IRWXU | stat.S_IRWXG)
            # Loop through datasets connected to the sink and copy them to the
            # cache directory and upload to daris.
            for name, format_name, mult, processed in self.inputs.datasets:
                assert mult in self.ACCEPTED_MULTIPLICITIES
                assert processed, ("{} (format: {}, mult: {}) isn't processed"
                                   .format(name, format_name, mult))
                filename = getattr(self.inputs, name + self.INPUT_SUFFIX)
                if not isdefined(filename):
                    missing_files.append(name)
                    continue  # skip the upload for this file
                dataset_format = data_formats[format_name]
                assert (
                    split_extension(filename)[1] == dataset_format.extension), (
                    "Mismatching extension '{}' for format '{}' ('{}')"
                    .format(split_extension(filename)[1],
                            data_formats[format_name].name,
                            dataset_format.extension))
                src_path = os.path.abspath(filename)
                # Copy to local cache
                dst_path = os.path.join(out_dir, name + dataset_format.extension)
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

    def _get_daris_ids(self):
        ex_method_id = 2
        subject_id = self.inputs.subject_id
        session_id = self.inputs.session_id
        return ex_method_id, subject_id, session_id


class DarisSubjectSink(DarisSink):

    input_spec = DarisSubjectSinkInputSpec

    ACCEPTED_MULTIPLICITIES = ('per_subject',)

    def _get_daris_ids(self):
        ex_method_id = SUBJECT_SUMMARY_ID
        subject_id = self.inputs.subject_id
        session_id = 1
        return ex_method_id, subject_id, session_id


class DarisProjectSink(DarisSink):

    input_spec = DarisProjectSinkInputSpec

    ACCEPTED_MULTIPLICITIES = ('per_project',)

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
                # Attempt to convert session_ids into a single int and then wrap
                # in a list in case session ids is a single integer (or string
                # representation of an integer)
                try:
                    session_ids = [int(session_id)]
                except TypeError:
                    session_ids = session_id
                entries = [e for e in entries if e.id in session_ids]
        return Session(subject_id=e.cid.split('.')[-3], session_id=e.id)

    def project(self, project_id, subject_ids=None, session_ids=None):
        with self._daris() as daris:
            # Get all datasets in project
            entries = daris.get_datasets(self, project_id, repo_id=self._repo_id,
                                      subject_id=None, session_id=None,
                                      ex_method_id=None)
        subject_dict = defaultdict(defaultdict(defaultdict(list)))
        for entry in entries:
            (subject_id, ex_method_id,
             session_id, dataset_id) = entry.cid.split('.')[-4:]
            subject_dict[subject_id][ex_method_id][session_id].append(dataset_id)
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

    def sessions_with_dataset(self, dataset, project_id, sessions):
        """
        Parameters
        ----------
        dataset : Dataset
            A file (name) for which to return the sessions that contain it
        project_id : int
            The id of the project
        sessions : List[Session]
            List of sessions of which to test for the dataset
        """
        if sessions is None:
            sessions = self.all_sessions(project_id=project_id)
        sess_with_dataset = []
        with self._daris() as daris:
            for session in sessions:
                entries = daris.get_datasets(
                    project_id, session.subject_id, session.session_id,
                    repo_id=self._repo_id,
                    ex_method_id=int(dataset.processed) + 1)
                if dataset.filename() in (e.name for e in entries):
                    sess_with_dataset.append(session)
        return sess_with_dataset

    def _daris(self):
        return DarisLogin(server=self._server, domain=self._domain,
                            user=self._user, password=self._password)

    @property
    def local_dir(self):
        return self._cache_dir


class DarisLogin:
    """
    Handles the connection to the MediaFlux server, logs into the DaRIS
    application and runs MediaFlux commands
    """
    _namespaces = {'daris': 'daris'}
    DEFAULT_REPO = 2
    XPATHS = {'cid': 'cid',
              'name': 'meta/daris:pssd-object/name',
              'description': 'meta/daris:pssd-object/description',
              'ctime': 'ctime',
              'mtime': 'mtime',
              'lctype': 'type',
              'processed': 'meta/daris:pssd-derivation/processed',
              'method': 'meta/daris:pssd-derivation/method',
              'dataset_type': 'meta/daris:pssd-dataset/type',
              'creator_domain': 'creator/domain',
              'creator_user': 'creator/user',
              'url': 'content/url'}

    def __init__(self, server='mf-erc.its.monash.edu.au', domain='monash-ldap',
                 user=None, password=None, token_path=None,
                 app_name='python_daris'):
        """
        server     -- the host name or IP of the daris server
        domain     -- the login domain of the user to login with
        user       -- the username of the user to login with
        password   -- the password for the user
        token_path -- path to the token file to use for authentication. If it
                      doesn't exist it will be created using the username and
                      password provided
        """
        if user is None:
            user = os.environ.get('DARIS_USER', None)
        if password is None:
            password = os.environ.get('DARIS_PASSWORD', None)
        if ((token_path is None or not os.path.exists(token_path)) and
                None in (user, password)):
            raise DarisException(
                "Username and password must be provided if no token is "
                "given and the environment variables 'DARIS_USER' and "
                "'DARIS_PASSWORD' are not set")
        self._server = server
        self._domain = domain
        self._user = user
        self._password = password
        self._token_path = token_path
        self._app_name = app_name
        self._mfsid = None
        if token_path is not None and os.path.exists(token_path):
            with open(token_path) as f:
                self._token = f.readline()
        else:
            self._token = None

    def open(self):
        """
        Opens the session. Should usually be used within a 'with' context, e.g.

            with DarisLogin() as session:
                session.run("my-cmd")

        to ensure that the session is always closed afterwards
        """
        if self._token is not None:
            # Get MediaFlux SID from token logon
            self._mfsid = self.run("system.logon :app {} :token {}"
                                   .format(self._app_name, self._token),
                                   logon=True)
        else:
            # Logon to DaRIS using user name
            self._mfsid = self.run("logon {} {} {}".format(
                self._domain, self._user, self._password), logon=True)
            if self._token_path is not None:
                # Generate token if it doesn't already exist
                self._token = self.run(
                    "secure.identity.token.create :app {}"
                    .format(self._app_name), logon=True)
                # ":destroy-on-service-call system.logoff"
                with open(self._token_path, 'w') as f:
                    f.write(self._token)
                # Change permissions to owner read only
                os.chmod(self._token_path, stat.S_IRUSR)

    def close(self):
        if self._mfsid:
            self.run('logoff')
            self._mfsid = None

    def __enter__(self):
        """
        This allows the daris session to be used in 'with' statements, e.g.

            with DarisLogin() as daris:
                daris.print_entries(daris.list_projects())

        and ensure that the session is closed again after the code runs
        (including on errors)
        """
        self.open()
        return self

    def __exit__(self, type_, value, traceback):  # @UnusedVariable
        self.close()

    def __del__(self):
        if self.is_open():
            self.close()

    def is_open(self):
        return self._mfsid is not None

    def get_projects(self, repo_id=2):
        """
        Lists all projects in the repository

        repo_id     -- the ID of the DaRIS repo (Monash is 2)
        """
        return self.query(
            "cid starts with '1008.{}' and model='om.pssd.project'"
            .format(repo_id))

    def get_subjects(self, project_id, repo_id=2):
        """
        Lists all projects in a project

        project_id  -- the ID of the project to list the subjects for
        repo_id     -- the ID of the DaRIS repo (Monash is 2)
        """
        return self.query(
            "cid starts with '1008.{}.{}' and model='om.pssd.subject'"
            .format(repo_id, project_id))

    def get_ex_methods(self, project_id, subject_id, repo_id=2):
        """
        Lists all projects in a project

        project_id  -- the ID of the project to list the subjects for
        repo_id     -- the ID of the DaRIS repo (Monash is 2)
        """
        return self.query(
            "cid starts with '{}' and model='om.pssd.ex-method'"
            .format(construct_cid(project_id=project_id, subject_id=subject_id,
                                  repo_id=repo_id)))

    def get_sessions(self, project_id, subject_id, ex_method_id=1,
                     repo_id=2):
        return self.query(
            "cid starts with '{}' and model='om.pssd.study'"
            .format(construct_cid(project_id=project_id, subject_id=subject_id,
                                  ex_method_id=ex_method_id, repo_id=repo_id)))

    def get_datasets(self, project_id, subject_id, session_id=1, repo_id=2,
                     ex_method_id=1):
        return self.query(
            "cid starts with '{}' and model='om.pssd.dataset'"
            .format(construct_cid(
                project_id=project_id, subject_id=subject_id,
                ex_method_id=ex_method_id, repo_id=repo_id,
                session_id=session_id)))

    def print_entries(self, entries):
        for entry in entries.itervalues():
            print '{} {}: {}'.format(entry.id, entry.name, entry.descr)

    def add_subject(self, project_id, subject_id=None, name=None,
                    description='\"\"', repo_id=2):
        """
        Adds a new subject with the given subject_id within the given
        project_id.

        project_id  -- The id of the project to add the subject to
        subject_id  -- The subject_id of the subject to add. If not provided
                       the next available subject_id is used
        name        -- The name of the subject
        description -- A description of the subject
        """
        if subject_id is None:
            # Get the next unused subject id
            try:
                max_subject_id = max(
                    self.get_subjects(project_id, repo_id=repo_id))
            except ValueError:
                max_subject_id = 0  # If there are no subjects
            subject_id = max_subject_id + 1
        if name is None:
            name = str(subject_id)
        cmd = (
            "om.pssd.subject.create :data-use \"unspecified\" :description "
            "\"{}\" :method \"1008.1.16\" :name \"{}\" :pid 1008.{}.{} "
            ":subject-number {}".format(
                description, name, repo_id, project_id, subject_id))
        # Return the id of the newly created subject
        return int(
            self.run(cmd, '/result/id', expect_single=True).split('.')[-1])

    def add_ex_method(self, project_id, subject_id, ex_method_id, repo_id=2):
        """
        Adds a new subject with the given subject_id within the given
        project_id.

        project_id  -- The id of the project to add the subject to
        subject_id  -- The subject_id of the subject to add. If not provided
                       the next available subject_id is used
        name        -- The name of the subject
        description -- A description of the subject
        """
        cmd = (
            "om.pssd.ex-method.create :mid \"1008.1.3\" "
            ":sid 1008.{}.{}.{} :exmethod-number {}".format(
                repo_id, project_id, subject_id, ex_method_id))
        # Return the id of the newly created subject
        return int(
            self.run(cmd, '/result/id', expect_single=True).split('.')[-1])

    def add_session(self, project_id, subject_id, session_id=None, name=None,
                    description='\"\"', ex_method_id=2, repo_id=2,
                    processed=None):
        """
        Adds a new subject with the given subject_id within the given
        project_id

        project_id  -- The id of the project to add the session to
        subject_id  -- The id of the subject to add the session to
        session_id    -- The session_id of the session to add. If not provided
                       the next available session_id is used
        name        -- The name of the subject
        description -- A description of the subject
        """
        if session_id is None:
            # Get the next unused session id
            try:
                max_session_id = max(
                    self.get_sessions(project_id, subject_id,
                                     ex_method_id=ex_method_id,
                                     repo_id=repo_id))
            except ValueError:
                max_session_id = 0
            session_id = max_session_id + 1
        if name is None:
            name = str(session_id)
        if processed is not None:
            processed_switch = ' :processed {}'.format(str(processed).lower())
        else:
            processed_switch = ''
        cmd = (
            "om.pssd.study.create :pid 1008.{}.{}.{}.{} "
            ":name \"{}\" :description \"{}\" :step 1 :study-number {}"
            "{processed}".format(
                repo_id, project_id, subject_id, ex_method_id,
                name, description, session_id, processed=processed_switch))
        # Return the id of the newly created session
        return int(
            self.run(cmd, '/result/id', expect_single=True).split('.')[-1])

    def copy_session(self, old_project_id, old_subject_id, old_session_id,
                     new_session_id, new_project_id=None, new_subject_id=None,
                     repo_id=2, old_ex_method_id=1, tmp_dir=None,
                     download=True, create_session=True, new_session_name=None,
                     new_ex_method_id=None):
        """
        Swaps a session and its meta-data from an incorrect ID to the desired
        one

        Parameters
        ----------
        create_session : bool
            Whether to create a new session or expect that there is an existing
            one (should only be used if there is a blank one there typically
        """
        if tmp_dir is None:
            tmp_dir = tempfile.mkdtemp()
        if new_project_id is None:
            new_project_id = old_project_id
        datasets = self.get_datasets(old_project_id, old_subject_id,
                                  session_id=old_session_id,
                                  repo_id=repo_id,
                                  ex_method_id=old_ex_method_id)
        # Download datasets first just to check whether there are any problems
        # before creating the new session
        for dataset in datasets.itervalues():
            self.download(os.path.join(tmp_dir, '{}_{}.zip'.format(dataset.id,
                                                                   dataset.name)),
                          project_id=old_project_id,
                          subject_id=old_subject_id,
                          session_id=old_session_id,
                          dataset_id=dataset.id,
                          repo_id=repo_id,
                          ex_method_id=old_ex_method_id)
        # Create a new subject if required
        if new_subject_id is not None:
            subjects = self.get_subjects(old_project_id, repo_id=repo_id)
            old_subject = subjects[old_subject_id]
            if new_subject_id not in subjects:
                self.add_subject(new_project_id, new_subject_id,
                                 repo_id=repo_id, name=old_subject.name,
                                 description=old_subject.description)
        else:
            new_subject_id = old_subject_id
        # Add the ex-method if required
        if new_ex_method_id is not None:
            methods = self.get_ex_methods(new_project_id, new_subject_id,
                                          repo_id=repo_id)
            if new_ex_method_id not in methods:
                self.add_ex_method(new_project_id, new_subject_id,
                                   new_ex_method_id, repo_id=repo_id)
        else:
            new_ex_method_id = old_ex_method_id
        # Get list of sessions in old and new locations
        old_sessions = self.get_sessions(old_project_id, old_subject_id,
                                         ex_method_id=old_ex_method_id,
                                         repo_id=repo_id)
        new_sessions = self.get_sessions(new_project_id, new_subject_id,
                                         ex_method_id=new_ex_method_id,
                                         repo_id=repo_id)
        old_session = old_sessions[old_session_id]
        # Add the new session if required
        if new_session_id not in new_sessions:
            if not create_session:
                raise DarisException("Session {} is not present for subject {} "
                                     "in project {}".format(new_session_id,
                                                            new_subject_id,
                                                            new_project_id))
            self.add_session(
                new_project_id, new_subject_id, session_id=new_session_id,
                name=(new_session_name
                      if new_session_name is not None else old_session.name),
                description=old_session.description,
                ex_method_id=new_ex_method_id, repo_id=repo_id)
        if download:
            for dataset in datasets.itervalues():
                new_dataset_id = self.add_dataset(
                    new_project_id, new_subject_id, session_id=new_session_id,
                    dataset_id=dataset.id, name=dataset.name,
                    description=dataset.description,
                    ex_method_id=new_ex_method_id, repo_id=repo_id)
                self.upload(os.path.join(tmp_dir,
                                         '{}_{}.zip'.format(dataset.id,
                                                            dataset.name)),
                            new_project_id, new_subject_id,
                            session_id=new_session_id, dataset_id=new_dataset_id,
                            ex_method_id=new_ex_method_id, repo_id=repo_id,
                            lctype=dataset.lctype)
        else:
            session_cid = construct_cid(
                project_id=new_project_id, subject_id=new_subject_id,
                session_id=new_session_id, ex_method_id=new_ex_method_id,
                repo_id=repo_id)
            for dataset_id in sorted(datasets):
                dataset_cid = construct_cid(
                    project_id=new_project_id, subject_id=old_subject_id,
                    session_id=old_session_id, ex_method_id=new_ex_method_id,
                    dataset_id=dataset_id, repo_id=repo_id)
                self.run('om.pssd.dataset.move :id {} :pid {}'
                         .format(dataset_cid, session_cid))
        return new_session_id

    def move_session(self, project_id, old_subject_id, old_session_id,
                     new_session_id, old_ex_method_id=1, repo_id=2, **kwargs):
        self.copy_session(project_id, old_subject_id, old_session_id,
                          new_session_id, old_ex_method_id=old_ex_method_id,
                          repo_id=repo_id, **kwargs)
        self.delete_session(project_id, old_subject_id, old_session_id,
                            ex_method_id=old_ex_method_id, repo_id=repo_id)

    def add_dataset(self, project_id, subject_id, session_id, dataset_id=None,
                    name=None, description='\"\"', ex_method_id=2, repo_id=2,
                    processed=None):
        """
        Adds a new dataset with the given subject_id within the given session id

        project_id  -- The id of the project to add the dataset to
        subject_id  -- The id of the subject to add the dataset to
        session_id    -- The id of the session to add the dataset to
        dataset_id     -- The dataset_id of the dataset to add. If not provided
                       the next available dataset_id is used
        name        -- The name of the subject
        description -- A description of the subject
        """
        if dataset_id is None:
            # Get the next unused dataset id
            try:
                max_dataset_id = max(
                    self.get_datasets(project_id, subject_id,
                                      session_id=session_id,
                                      ex_method_id=ex_method_id,
                                      repo_id=repo_id))
            except ValueError:
                max_dataset_id = 0
            dataset_id = max_dataset_id + 1
        if name is None:
            name = 'Project_{}'.format(dataset_id)
        if ex_method_id:
            meta = (" :meta \< :mbi.processed.study.properties \< "  # :step 1
                    ":study-reference 1008.{}.{}.{}.1 \> \>".format(
                        repo_id, project_id, subject_id))
        else:
            meta = ""
        if processed is None:
            processed = (ex_method_id > 1)
        cmd = ("om.pssd.dataset.derivation.create :pid 1008.{}.{}.{}.{}.{}"
               " :processed {} :name \"{}\" :description \"{}\"{}".format(
                   repo_id, project_id, subject_id, ex_method_id, session_id,
                   str(processed).lower(), name, description, meta))
        # Return the id of the newly created remote dataset
        return int(
            self.run(cmd, '/result/id', expect_single=True).split('.')[-1])

    def download(self, location, project_id, subject_id, dataset_id,
                 session_id=1, ex_method_id=1, repo_id=2):
        """
        Downloads an asset to a location on the local file system

        Parameters
        ----------
        location : str (file path)
            Path on the local file system to download the asset to.
            The extension must match the extension of the asset on DaRIS, i.e.
            typically '.zip'.
        project_id: int
            Id of the project
        subject_id: int
            Id of the subject
        dataset_id: int
            Id of the dataset to download
        session_id: int
            Id of the session
        ex_method_id: int
            Id of the experiment/method
        repo_id: int
            Id of the repo. 2 corrresponds to the Monash DaRIS instance.
        """
        # Construct CID
        cid = "1008.{}.{}.{}.{}.{}.{}".format(
            repo_id, project_id, subject_id, ex_method_id, session_id,
            dataset_id)
        self.run("asset.get :cid {} :out file:\"{}\"".format(cid, location))

    def download_match(self, location, project_id, sub_id, session_id=1,
                       match_dataset=None, dataset_type=None):
        """
        Downloads multiple assets to a location on the local file system

        Parameters
        ----------
        location : str (file path)
            Path on the local file system to download the assets to.
        project_id: int
            Id of the project
        subject_id: int
            Id of the subject
        match_dataset: string
            Recursive expression to match in order to download the asset
        dataset_type: list or string
            Name(s) of the dataset to download. Available datasets: asl (arterial
            spin labeling) t1,t2,epi,diffusion,proton_density, mt(Magnetization
            Transfer), ute umap(UTE umap), dixon,gre(field map),multiband
        """
        if match_dataset is None and dataset_type is None:
            raise Exception(
                "You must provide one between match_dataset OR dataset_type")

        datasets = self.get_datasets(project_id, sub_id, session_id=session_id)

        list_datasets = {}
        list_datasets['diffusion'] = (r'(R-L|L-R) ep2d([a-zA-Z_ ]+)([0-9]+)$|'
                                   r'(R-L|L-R) ep2d([a-zA-Z_ ]+)diff_motion$|'
                                   r'(R-L|L-R) ep2d([a-zA-Z_ ]+)diff$')
        list_datasets['epi'] = (
            r'(.*)ep2d([_ ])motion([_ ]+)correction$|(.*)'
            r'ep2d_rest([a-zA-Z_ ]+)|(.*)ep2d_task([a-zA-Z_ ]+)|'
            r'(.*)ep2d([_ ])bold([a-zA-Z_ ]+)')
        list_datasets['multiband'] = list_datasets['mb'] = (
            r'(A-P|P-A)([a-zA-Z_ ]+)mbep2d_bold$')
        list_datasets['asl'] = r'(.*)ep2d_tra_pasl$'
        list_datasets['pd'] = r'pd_tse.*'
        list_datasets['proton density'] = list_datasets['pd']
        list_datasets['proton_density'] = list_datasets['pd']
        list_datasets['t2'] = r'(.*)t2_spc.*|FLAIR'
        list_datasets['t1'] = r'(.*)t1_mprage.*|MPRAGE'
        list_datasets['mt'] = r'(.*)MT fl3d([a-zA-Z_ ]+)'
        list_datasets['ute'] = r'([a-zA-Z_ ]+)UTE$'
        list_datasets['dixon'] = r'([a-zA-Z_ ]+)DIXON([a-zA-Z_ ]+)_in'
        list_datasets['gre'] = r'(.*)gre([a-zA-Z_ ]+)field_map'
        list_datasets['field_map'] = list_datasets['field map'] = list_datasets['gre']
        list_datasets['umap'] = r'([a-zA-Z_ ]+)UTE([a-zA-Z_ ]+)UMAP'

        if isinstance(dataset_type, basestring):
            dataset_type = re.split(' |,', dataset_type)
        elif (not isinstance(dataset_type, collections.Iterable) and
              dataset_type is not None):
            raise Exception(
                "Dataset type '{}' is not a list or string".format(dataset_type))

        if dataset_type is not None:
            if match_dataset is not None:
                raise Exception(
                    "You need to provied just ONE input between dataset_type and "
                    "match_dataset")
            for dataset in dataset_type:
                match_dataset = list_datasets[dataset]
                for dataset_id in datasets:
                    if re.match(match_dataset, datasets[dataset_id].name):
                        name = datasets[dataset_id].cid
                        self.download(location + name + '.zip', project_id,
                                      sub_id, dataset_id,
                                      session_id=session_id)
        else:
            for dataset_id in datasets:
                if re.match(match_dataset, datasets[dataset_id].name):
                    name = datasets[dataset_id].cid
                    self.download(location + name + '.zip', project_id, sub_id,
                                  dataset_id, session_id=session_id)

    def upload(self, location, project_id, subject_id, session_id, dataset_id,
               name=None, repo_id=2, ex_method_id=2, lctype=None):
        # Use the name of the file to be uploaded if the 'name' kwarg is
        # present
        if name is None:
            name = os.path.basename(location)
        # Determine whether file is NifTI depending on file extension
        # FIXME: Need a better way to determine the filetype
        if lctype is not None:
            lctype_str = " :lctype {}".format(lctype)
        else:
            lctype_str = ""
        cmd = (
            "om.pssd.dataset.derivation.update :id 1008.{}.{}.{}.{}.{}.{} "
            " :in file:\"{}\" :filename \"{}\"{}".format(
                repo_id, project_id, subject_id, ex_method_id, session_id,
                dataset_id, location, name, lctype_str))
        self.run(cmd)

    def delete_subject(self, project_id, subject_id, repo_id=2):
        cmd = (
            "om.pssd.object.destroy :cid 1008.{}.{}.{} "
            ":destroy-cid true".format(repo_id, project_id, subject_id))
        self.run(cmd)

    def delete_ex_method(self, project_id, subject_id, ex_method_id,
                         repo_id=2):
        cmd = (
            "om.pssd.object.destroy :cid 1008.{}.{}.{}.{} "
            ":destroy-cid true".format(repo_id, project_id, subject_id,
                                       ex_method_id))
        self.run(cmd)

    def delete_session(self, project_id, subject_id, session_id,
                       ex_method_id=2, repo_id=2):
        cmd = (
            "om.pssd.object.destroy :cid 1008.{}.{}.{}.{}.{} "
            ":destroy-cid true".format(
                repo_id, project_id, subject_id, ex_method_id, session_id))
        self.run(cmd)

    def delete_dataset(self, project_id, subject_id, session_id, dataset_id,
                       ex_method_id=2, repo_id=2):
        cmd = (
            "om.pssd.object.destroy :cid 1008.{}.{}.{}.{}.{}.{} "
            ":destroy-cid true".format(
                repo_id, project_id, subject_id, ex_method_id, session_id,
                dataset_id))
        self.run(cmd)

    def find_session(self, name, project_id, subject_id, ex_method_id,
                     repo_id=2):
        sessions = self.get_sessions(
            project_id=project_id, subject_id=subject_id,
            repo_id=self.inputs.repo_id, ex_method_id=2).itervalues()
        try:
            return next(s for s in sessions.itervalues() if s.name == name)
        except StopIteration:
            raise DarisNameNotFoundException(
                "Did not find session named '{}' in 1008.{}.{}.{}.{}"
                .format(repo_id, project_id, subject_id, ex_method_id))

    def run(self, cmd, xpath=None, expect_single=False, logon=False):
        """
        Executes the aterm.jar and runs the provided aterm command within it

        cmd    -- The aterm command to run
        xpath  -- An xpath filter to the desired element(s)
        single -- Whether the filtered elements should only contain a single
                  result, and if so return its text field instead of the
                  etree.Element
        """
        if not logon and self._mfsid is None:
            raise DarisException(
                "Daris session is closed. DarisLogins are typically used "
                "within 'with' blocks, which ensures they are opened and "
                "closed properly")
        full_cmd = (
            "java -Djava.net.preferIPv4Stack=true -Dmf.host={server} "
            "-Dmf.port=8443 -Dmf.transport=https {mfsid}"
            "-Dmf.result=xml -cp {aterm_path} arc.mf.command.Execute {cmd}"
            .format(server=self._server, cmd=cmd, aterm_path=self.aterm_path(),
                    mfsid=('-Dmf.sid={} '.format(self._mfsid)
                           if not logon else '')))
        try:
            result = subprocess.check_output(
                full_cmd, stderr=subprocess.STDOUT, shell=True).strip()
        except subprocess.CalledProcessError as e:
            raise DarisException(
                "{} (Error code {}):\n{}".format(e.output.decode(),
                                                 e.returncode,
                                                 full_cmd))
        # Extract results from result XML if xpath is provided
        if xpath is not None:
            if isinstance(xpath, basestring):
                result = self._extract_from_xml(result, xpath)
                if expect_single:
                    try:
                        result = result[0].text
                    except IndexError:
                        raise DarisException(
                            "No results found for '{}' xpath".format(xpath))
            else:
                # If 'xpath' is a iterable of xpaths then extract each in turn
                result = [self._extract_from_xml(result, p) for p in xpath]
        return result

    def query(self, query, cid_index=False):
        """
        Runs a query command and returns the elements corresponding to the
        provided xpaths
        """
        cmd = ("asset.query :where \"{}\" :action get-meta :size infinity"
               .format(query))
        elements = self.run(cmd, '/result/asset')
        entries = []
        for element in elements:
            kwargs = {}
            for name, xpath in self.XPATHS.iteritems():
                extracted = element.xpath(xpath, namespaces=self._namespaces)
                if len(extracted) == 1:
                    attr = extracted[0].text
                elif not extracted:
                    attr = None
                else:
                    raise DarisException(
                        "Multiple results for given xpath '{}': {}"
                        .format(xpath, "', '".join(e.text for e in extracted)))
                kwargs[name] = attr
            # Strip the ID of the entry from the returned CID (i.e. the
            # number after the last '.'
            entries.append(DarisEntry(**kwargs))
        if cid_index:
            id_attr = 'cid'
        else:
            id_attr = 'id'
        return dict((getattr(e, id_attr), e) for e in entries)

    def exists(self, *args, **kwargs):
        if args:
            assert len(args) == 1
            cid = args[0]
        else:
            try:
                cid = kwargs['cid']
            except KeyError:
                cid = construct_cid(**kwargs)
        result = self.run("asset.exists :cid {}".format(cid), '/result/exists',
                          expect_single=True)
        return result == 'true'

    @classmethod
    def _extract_from_xml(cls, xml_string, xpath):
        doc = etree.XML(xml_string.strip())
        return doc.xpath(xpath, namespaces=cls._namespaces)

    @classmethod
    def aterm_path(cls):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            'aterm.jar')


class DarisEntry(object):

    def __init__(self, cid, name, description, ctime=None, mtime=None,
                 lctype=None, processed=None, method=None, dataset_type=None,
                 creator_domain=None, creator_user=None, url=None):
        self._cid = cid
        self._name = name
        self._description = description
        self._ctime = ctime
        self._mtime = mtime
        self._lctype = lctype
        self._processed = processed
        self._method = method
        self._dataset_type = dataset_type
        self._creator_domain = creator_domain
        self._creator_user = creator_user
        self._url = url

    def __repr__(self):
        return ("DarisEntry(cid={}, name={}, description='{}'{})"
                .format(self.cid, self.name, self.description,
                        ("lctype='{}'".format(self.lctype)
                         if self.lctype is not None else '')))

    @property
    def cid(self):
        return self._cid

    @property
    def id(self):
        return int(self._cid.split('.')[-1])

    @property
    def name(self):
        return self._name

    @property
    def description(self):
        return self._description

    @property
    def lctype(self):
        return self._lctype

    @property
    def ctime(self):
        return self._ctime

    @property
    def mtime(self):
        return self._mtime

    @property
    def processed(self):
        return self._processed

    @property
    def method(self):
        return self._method

    @property
    def dataset_type(self):
        return self._dataset_type

    @property
    def creator_domain(self):
        return self._creator_domain

    @property
    def creator_user(self):
        return self._creator_user

    @property
    def url(self):
        return self._url


def construct_cid(project_id, subject_id=None, session_id=None,
                  ex_method_id=None, dataset_id=None, repo_id=2):
    """
    Returns the CID (unique asset identifier for DaRIS) from the combination of
    sub ids
    """
    cid = '1008.{}.{}'.format(repo_id, project_id)
    ids = (subject_id, ex_method_id, session_id, dataset_id)
    for i, id_ in enumerate(ids):
        if id_ is not None:
            cid += '.{}'.format(int(id_))
        else:
            # Check to see that all subsequent ids are None (which they should
            # be).
            if any(d is not None for d in ids[(i + 1):]):
                raise DarisException(
                    "Not None IDs followed None value in constructing CID for "
                    "project_id={project_id}, subject_id={subject_id}, "
                    "session_id={session_id}, ex_method_id={ex_method_id}, "
                    "dataset_id={dataset_id}, repo_id={repo_id}".format(
                        project_id=project_id, subject_id=subject_id,
                        session_id=session_id, ex_method_id=ex_method_id,
                        dataset_id=dataset_id, repo_id=repo_id))
            else:
                break
    return cid


if __name__ == '__main__':
    daris = DarisLogin(domain='system', user='manager',
                         password='t0gp154sp!')
    with daris:
        daris.copy_session(135, 3, 2, new_project_id=144, new_subject_id=2,
                         new_session_id=1)
