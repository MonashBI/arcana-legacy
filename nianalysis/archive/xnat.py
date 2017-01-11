import os.path
import shutil
import stat
import logging
from collections import defaultdict
from nipype.interfaces.base import (
    Directory, traits, isdefined)
from nianalysis.exceptions import (
    XNATException)
from nianalysis.dataset import Dataset
from nianalysis.archive.base import (
    Archive, ArchiveSource, ArchiveSink, ArchiveSourceInputSpec,
    ArchiveSinkInputSpec, ArchiveSubjectSinkInputSpec,
    ArchiveProjectSinkInputSpec, Session, Subject, Project)
from nianalysis.data_formats import data_formats
from nianalysis.utils import split_extension
import xnat  # NB: XNATPy not PyXNAT

logger = logging.getLogger('NiAnalysis')


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
            subj_summ_session_name = XNATArchive.subject_summary_session_name(
                project.label, subject.label)
            proj_summ_subject_name = XNATArchive.project_summary_subject_name(
                project.label)
            proj_summ_session_name = XNATArchive.project_summary_session_name(
                project.label)
            try:
                proc_session = xnat_login.experiments[
                    self.inputs.session_id + XNATArchive.PROCESSED_SUFFIX]
                proc_datasets = dict(
                    (s.series_description, s)
                    for s in proc_session.scans.itervalues())
            except KeyError:
                proc_datasets = {}
            try:
                subj_summary = subject.experiments[subj_summ_session_name]
                subj_datasets = dict(
                    (s.series_description, s)
                    for s in subj_summary.scans.itervalues())
            except KeyError:
                subj_datasets = {}
            try:
                proj_summary = project.subjects[
                    proj_summ_subject_name].experiments[proj_summ_session_name]
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
                        subj_summ_session_name)
                elif mult == 'per_project':
                    assert processed
                    dataset = proj_datasets[name]
                    cache_dir = os.path.join(
                        base_cache_dir, proj_summ_subject_name,
                        proj_summ_session_name)
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
        session_name = XNATArchive.subject_summary_session_name(
            project.label, subject.label)
        try:
            session = subject.experiments[session_name]
        except KeyError:
            session = xnat_login.mbi_xnat.classes.MrSessionData(
                label=session_name, parent=subject)
        return session


class XNATProjectSink(XNATSink):

    input_spec = XNATProjectSinkInputSpec

    ACCEPTED_MULTIPLICITIES = ('per_project',)

    def _get_session(self, project, subject, xnat_login):
        subject_name = XNATArchive.project_summary_subject_name(project.label)
        try:
            subject = project.subjects[subject_name]
        except KeyError:
            subject = xnat_login.mbi_xnat.classes.MrSessionData(
                label=subject_name, parent=subject)
        session_name = XNATArchive.project_summary_session_name(project.label)
        try:
            session = subject.experiments[session_name]
        except KeyError:
            session = xnat_login.mbi_xnat.classes.MrSessionData(
                label=session_name, parent=subject)
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

    SUMMARY_NAME = 'SUMMARY'

    def __init__(self, user=None, password=None, cache_dir=None,
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
        if self._user is not None:
            source.inputs.user = self._user
        if self._password is not None:
            source.inputs.password = self._password
        source.inputs.cache_dir = self._cache_dir
        return source

    def sink(self, *args, **kwargs):
        sink = super(XNATArchive, self).sink(*args, **kwargs)
        sink.inputs.server = self._server
        if self._user is not None:
            sink.inputs.user = self._user
        if self._password is not None:
            sink.inputs.password = self._password
        sink.inputs.cache_dir = self._cache_dir
        return sink

    def _login(self):
        sess_kwargs = {}
        if self._user is not None:
            sess_kwargs['user'] = self._user
        if self._password is not None:
            sess_kwargs['password'] = self._password
        return xnat.connect(server=self.inputs.server, **sess_kwargs)

    def all_session_ids(self, project_id):
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
        sess_kwargs = {}
        if self._user is not None:
            sess_kwargs['user'] = self._user
        if self._password is not None:
            sess_kwargs['password'] = self._password
        with self.login() as xnat_login:
            return [
                s.label for s in xnat_login.projects[
                    project_id].experiments.itervalues()]

    def project(self, project_id, subject_ids=None, session_ids=None):
        """
        Return subject and session information for a project in the XNAT
        archive

        Parameters
        ----------
        project_id : str
            ID of the project to inspect
        subject_ids : list(str)
            List of subject IDs with which to filter the tree with. If None all
            are returned
        session_ids : list(str)
            List of session IDs with which to filter the tree with. If None all
            are returned

        Returns
        -------
        project : nianalysis.archive.Project
            A hierarchical tree of subject, session and dataset information for
            the archive
        """
        subjects = []
        with self.login() as xnat_login:
            xproject = xnat_login.projects[project_id]
            for xsubject in xproject.subjects.itervalues():
                if subject_ids is None or xsubject.label in subject_ids:
                    sessions = []
                    for xsession in subjects.experiments.itervalues():
                        if (session_ids is None or
                                xsession.label in session_ids):
                            sessions.append(Session(
                                xsession.label,
                                datasets=self._get_datasets(xsession,
                                                            'per_session'),
                                processed=False))
                    subj_summary = xsubject.experiments[
                        self.subject_summary_session_name(project_id,
                                                          xsubject.label)]
                    subjects.append(Subject(
                        xsubject.label, sessions,
                        self._get_datasets(subj_summary, 'per_subject')))
                proj_summary = xproject.subjects[
                    self.project_summary_session_name(project_id)].experiments[
                        self.project_summary_session_name(project_id)]
        return Project(project_id, subjects, self._get_datasets(proj_summary,
                                                                'per_project'))

    def _get_datasets(self, xsession, mult):
        """
        Returns a list of datasets within an XNAT session

        Parameters
        ----------
        xsession : xnat.classes.MrSessionData
            The XNAT session to extract the datasets from
        mult : str
            The multiplicity of the returned datasets (either 'per_session',
            'per_subject' or 'per_project')

        Returns
        -------
        datasets : list(nianalysis.dataset.Dataset)
            List of datasets within an XNAT session
        """
        datasets = []
        for dataset in xsession.scans.itervalues():
            datasets.append(Dataset(
                dataset.type, format=None, processed=False,  # @ReservedAssignment @IgnorePep8
                multiplicity=mult, location=None))
        return datasets

    def sessions_with_dataset(self, dataset, project_id, sessions):
        """
        Return all sessions containing the given dataset

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
    def subject_summary_session_name(cls, project_id, subject_id):
        return '{}_{}_{}'.format(project_id, subject_id, cls.SUMMARY_NAME)

    @classmethod
    def project_summary_session_name(cls, project_id):
        return '{}_{}_{}'.format(project_id, cls.SUMMARY_NAME,
                                 cls.SUMMARY_NAME)

    @classmethod
    def project_summary_subject_name(cls, project_id):
        return '{}_{}_{}'.format(project_id, cls.SUMMARY_NAME,
                                 cls.SUMMARY_NAME)
