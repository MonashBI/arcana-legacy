import os.path
import shutil
import stat
import logging
from collections import defaultdict
from nipype.interfaces.base import (
    Directory, traits, isdefined)
from nianalysis.exceptions import (
    XNATException)
from nianalysis.archive.base import (
    Archive, ArchiveSource, ArchiveSink, ArchiveSourceInputSpec,
    ArchiveSinkInputSpec, ArchiveSubjectSinkInputSpec,
    ArchiveProjectSinkInputSpec, Session, Subject, Project)
from nianalysis.data_formats import data_formats
from nianalysis.utils import split_extension
import xnat

logger = logging.getLogger('NiAnalysis')

PROJECT_SUMMARY_NAME = 'PROJECT_SUMMARY'
SUBJECT_SUMMARY_NAME = 'SUBJECT_SUMMARY'


class XNATSourceInputSpec(ArchiveSourceInputSpec):
    server = traits.Str('https://mf-erc.its.monash.edu.au', mandatory=True,
                        usedefault=True, desc="The address of the MF server")
    user = traits.Str(
        mandatory=False,
        desc=("The XNAT username to connect with in with if not "
              "supplied it can be read from ~/.netrc (see "
              "https://xnat.readthedocs.io/en/latest/static/tutorial.html"
              "#connecting-to-a-server)"))
    password = traits.Password(
        mandatory=False,
        desc=("The XNAT password corresponding to the supplied username, if "
              "not supplied it can be read from ~/.netrc (see "
              "https://xnat.readthedocs.io/en/latest/static/tutorial.html"
              "#connecting-to-a-server)"))
    cache_dir = Directory(
        exists=True, desc=("Path to the base directory where the downloaded"
                           "datasets will be cached"))


class XNATSource(ArchiveSource):
    """
    A NiPype IO interface for grabbing datasets off DaRIS (analogous to
    DataGrabber)
    """

    input_spec = XNATSourceInputSpec

    def _list_outputs(self):
        outputs = {}
        base_cache_dir = os.path.join(self.inputs.cache_dir,
                                      self.inputs.project_id)
        sess_kwargs = {}
        if isdefined(self.inputs.user):
            sess_kwargs['user'] = self.inputs.user
        if isdefined(self.inputs.password):
            sess_kwargs['password'] = self.inputs.password
        with xnat.connect(server=self.inputs.server,
                          **sess_kwargs) as xnat_login:
            project = xnat_login.projects[self.inputs.project_id]
            subject = project.subjects[self.inputs.subject_id]
            session = subject.experiments[self.inputs.session_id]
            datasets = dict(
                (s.series_description, s) for s in session.scans.itervalues())
            try:
                proc_session = xnat_login.experiments[
                    self.inputs.session_id + XNATArchive.PROCESSED_SUFFIX]
                proc_datasets = dict(
                    (s.series_description, s)
                    for s in proc_session.scans.itervalues())
            except KeyError:
                proc_datasets = {}
            try:
                subj_summary = subject.experiments[SUBJECT_SUMMARY_NAME]
                subj_datasets = dict(
                    (s.series_description, s)
                    for s in subj_summary.scans.itervalues())
            except KeyError:
                subj_datasets = {}
            try:
                proj_summary = subject.experiments[PROJECT_SUMMARY_NAME]
                proj_datasets = dict(
                    (s.series_description, s)
                    for s in proj_summary.scans.itervalues())
            except KeyError:
                proj_datasets = {}
            for (name, data_format, mult, processed) in self.inputs.datasets:
                if mult == 'per_session':
                    if processed:
                        dataset = proc_datasets[name]
                    else:
                        dataset = datasets[name]
                    cache_dir = os.path.join(
                        base_cache_dir, self.inputs.subject_id,
                        self.inputs.session_id)
                elif mult == 'per_subject':
                    assert processed
                    dataset = subj_datasets[name]
                    cache_dir = os.path.join(
                        base_cache_dir, self.inputs.subject_id,
                        SUBJECT_SUMMARY_NAME)
                elif mult == 'per_project':
                    assert processed
                    dataset = proj_datasets[name]
                    cache_dir = os.path.join(
                        base_cache_dir, PROJECT_SUMMARY_NAME)
                else:
                    assert False, "Unrecognised multiplicity '{}'".format(mult)
                if not os.path.exists(cache_dir):
                    os.makedirs(cache_dir)
                fname = name + data_formats[data_format].extension
                cache_path = os.path.join(cache_dir, fname)
                if not os.path.exists(cache_path):
                    dataset.resources[data_format.name].download_dir(
                        cache_path)
                outputs[name + self.OUTPUT_SUFFIX] = cache_path
        return outputs


class XNATSinkInputSpecMixin(object):

    cache_dir = Directory(
        exists=True, desc=("Path to the base directory where the datasets will"
                           " be cached before uploading"))
    server = traits.Str('https://mf-erc.its.monash.edu.au', mandatory=True,
                        usedefault=True, desc="The address of the MF server")
    user = traits.Str(
        mandatory=False,
        desc=("The XNAT username to connect with in with if not "
              "supplied it can be read from ~/.netrc (see "
              "https://xnat.readthedocs.io/en/latest/static/tutorial.html"
              "#connecting-to-a-server)"))
    password = traits.Password(
        mandatory=False,
        desc=("The XNAT password corresponding to the supplied username, if "
              "not supplied it can be read from ~/.netrc (see "
              "https://xnat.readthedocs.io/en/latest/static/tutorial.html"
              "#connecting-to-a-server)"))


class XNATSinkInputSpec(ArchiveSinkInputSpec, XNATSinkInputSpecMixin):

    pass


class XNATSubjectSinkInputSpec(ArchiveSubjectSinkInputSpec,
                               XNATSinkInputSpecMixin):

    pass


class XNATProjectSinkInputSpec(ArchiveProjectSinkInputSpec,
                               XNATSinkInputSpecMixin):

    pass


class XNATSink(ArchiveSink):
    """
    A NiPype IO interface for putting processed datasets onto DaRIS (analogous
    to DataSink)
    """

    input_spec = XNATSinkInputSpec
    ACCEPTED_MULTIPLICITIES = ('per_session',)

    def _list_outputs(self):
        """Execute this module.
        """
        # Initiate output
        outputs = self.output_spec().get()
        out_files = []
        missing_files = []
        # Open XNAT session
        sess_kwargs = {}
        if isdefined(self.inputs.user):
            sess_kwargs['user'] = self.inputs.user
        if isdefined(self.inputs.password):
            sess_kwargs['password'] = self.inputs.password
        with xnat.connect(server=self.inputs.server,
                          **sess_kwargs) as xnat_login:
            project = xnat_login.projects[self.inputs.project_id]
            subject = project.subjects[self.inputs.subject_id]
            assert self.inputs.session_id in subject.experiments
            # Add session for processed scans if not present
            session = self._get_session(xnat_login)
            # Get cache dir for session
            out_dir = os.path.abspath(os.path.join(*(str(d) for d in (
                self.inputs.cache_dir, self.inputs.project_id,
                self.inputs.subject_id, self.inputs.session_id))))
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
                    split_extension(filename)[1] ==
                    dataset_format.extension), (
                    "Mismatching extension '{}' for format '{}' ('{}')"
                    .format(split_extension(filename)[1],
                            data_formats[format_name].name,
                            dataset_format.extension))
                src_path = os.path.abspath(filename)
                # Copy to local cache
                out_fname = name + dataset_format.extension
                dst_path = os.path.join(out_dir, out_fname)
                out_files.append(dst_path)
                shutil.copyfile(src_path, dst_path)
                # Upload to XNAT
                dataset = xnat_login.classes.MrScanData(type=name,
                                                        parent=session)
                resource = dataset.create_resource(format_name)
                resource.upload(dst_path, out_fname)
        if missing_files:
            # FIXME: Not sure if this should be an exception or not,
            #        indicates a problem but stopping now would throw
            #        away the datasets that were created
            logger.warning(
                "Missing output datasets '{}' in XNATSink".format(
                    "', '".join(str(f) for f in missing_files)))
        # Return cache file paths
        outputs['out_files'] = out_files
        return outputs

    def _get_session(self, project, subject, xnat_login):  # @UnusedVariable
        proc_session_id = (self.inputs.session_id +
                           XNATArchive.PROCESSED_SUFFIX)
        try:
            session = subject.experiments[proc_session_id]
        except KeyError:
            session = xnat_login.mbi_xnat.classes.MrSessionData(
                label=proc_session_id, parent=subject)
        return session


class XNATSubjectSink(XNATSink):

    input_spec = XNATSubjectSinkInputSpec

    ACCEPTED_MULTIPLICITIES = ('per_subject',)

    def _get_session(self, project, subject, xnat_login):  # @UnusedVariable
        try:
            session = subject.experiments[SUBJECT_SUMMARY_NAME]
        except KeyError:
            session = xnat_login.mbi_xnat.classes.MrSessionData(
                label=SUBJECT_SUMMARY_NAME, parent=subject)
        return session


class XNATProjectSink(XNATSink):

    input_spec = XNATProjectSinkInputSpec

    ACCEPTED_MULTIPLICITIES = ('per_project',)

    def _get_session(self, project, subject, xnat_login):
        try:
            subject = project.subjects[PROJECT_SUMMARY_NAME]
        except KeyError:
            subject = xnat_login.mbi_xnat.classes.MrSessionData(
                label=PROJECT_SUMMARY_NAME, parent=subject)
        try:
            session = subject.experiments[PROJECT_SUMMARY_NAME]
        except KeyError:
            session = xnat_login.mbi_xnat.classes.MrSessionData(
                label=PROJECT_SUMMARY_NAME, parent=subject)
        return session


class XNATArchive(Archive):
    """
    An 'Archive' class for the DaRIS research management system.
    """

    type = 'xnat'
    Sink = XNATSink
    Source = XNATSource
    SubjectSink = XNATSubjectSink
    ProjectSink = XNATProjectSink

    def __init__(self, user, password, cache_dir=None,
                 server='https://mbi-xnat.erc.monash.edu.au'):
        self._server = server
        self._user = user
        self._password = password
        if cache_dir is None:
            self._cache_dir = os.path.join(os.getcwd(), '.xnat')
        else:
            self._cache_dir = cache_dir

    def source(self, *args, **kwargs):
        source = super(XNATArchive, self).source(*args, **kwargs)
        source.inputs.server = self._server
        source.inputs.user = self._user
        source.inputs.password = self._password
        source.inputs.cache_dir = self._cache_dir
        return source

    def sink(self, *args, **kwargs):
        sink = super(XNATArchive, self).sink(*args, **kwargs)
        sink.inputs.server = self._server
        sink.inputs.user = self._user
        sink.inputs.password = self._password
        sink.inputs.cache_dir = self._cache_dir
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
        return Session(subject_id=e.cid.split('.')[-3], session_id=e.id)

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
                raise XNATException(
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
                        raise XNATException(
                            "Session(s) {} found in ex-method 4 of subject {}."
                            "Project summaries are only allowed in session 1 "
                            "of subject 1".format(', '.join(session_dict),
                                                  subject_id))
                    project_summary = session_dict['1']
                elif ex_method_id == 3:
                    if session_dict.keys() != ['1']:
                        raise XNATException(
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

    @property
    def local_dir(self):
        return self._cache_dir

    @classmethod
    def get_session_id(cls, project_id, subject_id, session_id):
        '_'.join(project_id, subject_id, session_id)
