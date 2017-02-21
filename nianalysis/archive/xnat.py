from __future__ import absolute_import
import os.path
import shutil
import stat
import logging
import errno
from collections import defaultdict
from nipype.interfaces.base import Directory, traits, isdefined
from nianalysis.dataset import Dataset
from nianalysis.archive.base import (
    Archive, ArchiveSource, ArchiveSink, ArchiveSourceInputSpec,
    ArchiveSinkInputSpec, ArchiveSubjectSinkInputSpec,
    ArchiveProjectSinkInputSpec, Session, Subject, Project)
from nianalysis.data_formats import data_formats
from nianalysis.utils import split_extension
from nianalysis.exceptions import NiAnalysisError
import re
import xnat  # NB: XNATPy not PyXNAT
from nianalysis.utils import INPUT_SUFFIX, OUTPUT_SUFFIX

logger = logging.getLogger('NiAnalysis')

sanitize_re = re.compile(r'[^a-zA-Z_0-9]')


class XNATMixin(object):

    @property
    def full_session_id(self):
        if '_' not in self.inputs.session_id:
            session_id = (self.inputs.subject_id + '_' +
                          self.inputs.session_id)
        return session_id


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


class XNATSource(ArchiveSource, XNATMixin):
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
            session = subject.experiments[self.full_session_id]
            datasets = dict(
                (s.type, s) for s in session.scans.itervalues())
            subj_summ_session_name = XNATArchive.subject_summary_session_name(
                subject.label)
            proj_summ_subject_name = XNATArchive.project_summary_subject_name(
                project.id)
            proj_summ_session_name = XNATArchive.project_summary_session_name(
                project.id)
            try:
                proc_session = xnat_login.experiments[
                    self.full_session_id + XNATArchive.PROCESSED_SUFFIX]
                proc_datasets = dict(
                    (s.type, s) for s in proc_session.scans.itervalues())
            except KeyError:
                proc_datasets = {}
            try:
                subj_summary = subject.experiments[subj_summ_session_name]
                subj_datasets = dict(
                    (s.type, s) for s in subj_summary.scans.itervalues())
            except KeyError:
                subj_datasets = {}
            try:
                proj_summary = project.subjects[
                    proj_summ_subject_name].experiments[proj_summ_session_name]
                proj_datasets = dict(
                    (s.type, s) for s in proj_summary.scans.itervalues())
            except KeyError:
                proj_datasets = {}
            for (name, data_format, mult, processed) in self.inputs.datasets:
                # Prepend study name if defined and processed input
                if processed and isdefined(self.inputs.study_name):
                    prefixed_name = self.inputs.study_name + '_' + name
                else:
                    prefixed_name = name
                if mult == 'per_session':
                    cache_dir = os.path.join(
                        base_cache_dir, self.inputs.subject_id,
                        self.inputs.session_id)
                    try:
                        if processed:
                            dataset = proc_datasets[prefixed_name]
                            cache_dir += XNATArchive.PROCESSED_SUFFIX
                        else:
                            dataset = datasets[prefixed_name]
                    except KeyError:
                        raise NiAnalysisError(
                            "Could not find '{}' dataset in acquired and "
                            "processed sessions ('{}' and '{}' respectively)"
                            .format(prefixed_name, "', '".join(datasets),
                                    "', '".join(proc_datasets)))
                    session_label = session.label
                elif mult == 'per_subject':
                    assert processed
                    dataset = subj_datasets[prefixed_name]
                    cache_dir = os.path.join(
                        base_cache_dir, self.inputs.subject_id,
                        subj_summ_session_name)
                    session_label = subj_summ_session_name
                elif mult == 'per_project':
                    assert processed
                    dataset = proj_datasets[prefixed_name]
                    cache_dir = os.path.join(
                        base_cache_dir, proj_summ_subject_name,
                        proj_summ_session_name)
                    session_label = proj_summ_session_name
                else:
                    assert False, "Unrecognised multiplicity '{}'".format(mult)
                if not os.path.exists(cache_dir):
                    os.makedirs(cache_dir)
                fname = prefixed_name
                if data_formats[data_format].extension is not None:
                    fname += data_formats[data_format].extension
                cache_path = os.path.join(cache_dir, fname)
                # FIXME: Should do a check to see if versions match
                if not os.path.exists(cache_path):
                    tmp_dir = cache_path + '.download'
                    try:
                        dataset.resources[data_format.upper()].download_dir(
                            tmp_dir)
                    except KeyError:
                        raise NiAnalysisError(
                            "'{}' dataset is not available in '{}' format, "
                            "available resources are '{}'"
                            .format(
                                name, data_format.upper(),
                                "', '".join(
                                    r.label
                                    for r in dataset.resources.itervalues())))
                    data_path = os.path.join(
                        tmp_dir, session_label, 'scans',
                        (dataset.id + '-' +
                         re.sub(r'[\.\-]', '_', dataset.type)), 'resources',
                        data_format.upper(), 'files')
                    if data_formats[data_format].extension is not None:
                        data_path = os.path.join(data_path, fname)
                    shutil.move(data_path, cache_path)
                    shutil.rmtree(tmp_dir)
                outputs[name + OUTPUT_SUFFIX] = cache_path
        return outputs


class XNATSinkInputSpecMixin(object):
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


class XNATSinkInputSpec(ArchiveSinkInputSpec, XNATSinkInputSpecMixin):
    pass


class XNATSubjectSinkInputSpec(ArchiveSubjectSinkInputSpec,
                               XNATSinkInputSpecMixin):
    pass


class XNATProjectSinkInputSpec(ArchiveProjectSinkInputSpec,
                               XNATSinkInputSpecMixin):
    pass


class XNATSink(ArchiveSink, XNATMixin):
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
            # Add session for processed scans if not present
            session, cache_dir = self._get_session(xnat_login)
            # Make session cache dir
            if not os.path.exists(cache_dir):
                os.makedirs(cache_dir, stat.S_IRWXU | stat.S_IRWXG)
            # Loop through datasets connected to the sink and copy them to the
            # cache directory and upload to daris.
            for name, format_name, mult, processed in self.inputs.datasets:
                assert mult in self.ACCEPTED_MULTIPLICITIES
                assert processed, ("{} (format: {}, mult: {}) isn't processed"
                                   .format(name, format_name, mult))
                filename = getattr(self.inputs, name + INPUT_SUFFIX)
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
                if isdefined(self.inputs.study_name):
                    prefixed_name = self.inputs.study_name + '_' + name
                else:
                    prefixed_name = name
                out_fname = prefixed_name + dataset_format.extension
                # Copy to local cache
                dst_path = os.path.join(cache_dir, out_fname)
                out_files.append(dst_path)
                shutil.copyfile(src_path, dst_path)
                # Upload to XNAT
                dataset = xnat_login.classes.MrScanData(
                    type=prefixed_name, parent=session)
                # Delete existing resource
                # TODO: probably should have check to see if we want to
                #       override it
                try:
                    resource = dataset.resources[format_name.upper()]
                    resource.delete()
                except KeyError:
                    pass
                resource = dataset.create_resource(format_name.upper())
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

    def _get_session(self, xnat_login):
        project = xnat_login.projects[self.inputs.project_id]
        subject = project.subjects[self.inputs.subject_id]
        assert self.full_session_id in subject.experiments
        session_name = self.full_session_id + XNATArchive.PROCESSED_SUFFIX
        try:
            session = subject.experiments[session_name]
        except KeyError:
            session = self._create_session(xnat_login, subject.id,
                                           session_name)
        # Get cache dir for session
        cache_dir = os.path.abspath(os.path.join(
            self.inputs.cache_dir, self.inputs.project_id,
            self.inputs.subject_id,
            self.inputs.session_id + XNATArchive.PROCESSED_SUFFIX))
        return session, cache_dir

    def _create_session(self, xnat_login, subject_id, session_id):
        """
        This creates a processed session in a way that respects whether
        the acquired session has been shared into another project or not.

        If we weren't worried about this we could just use

            session = xnat_login.classes.MrSessionData(label=proc_session_id,
                                                       parent=subject)
        """
        uri = ('/data/archive/projects/{}/subjects/{}/experiments/{}'
               .format(self.inputs.project_id, subject_id, session_id))
        query = {'xsiType': 'xnat:mrSessionData', 'label': session_id,
                 'req_format': 'qa'}
        response = xnat_login.put(uri, query=query)
        if response.status_code not in (200, 201):
            raise NiAnalysisError(
                "Could not create session '{}' in subject '{}' in project '{}'"
                " response code {}"
                .format(session_id, subject_id, self.inputs.project_id,
                        response))
        return xnat_login.classes.MrSessionData(uri=uri,
                                                xnat_session=xnat_login)


class XNATSubjectSink(XNATSink):

    input_spec = XNATSubjectSinkInputSpec

    ACCEPTED_MULTIPLICITIES = ('per_subject',)

    def _get_session(self, xnat_login):
        project = xnat_login.projects[self.inputs.project_id]
        subject = project.subjects[self.inputs.subject_id]
        session_name = XNATArchive.subject_summary_session_name(subject.label)
        try:
            session = subject.experiments[session_name]
        except KeyError:
            session = self._create_session(xnat_login, subject.id,
                                           session_name)
        # Get cache dir for session
        cache_dir = os.path.abspath(os.path.join(
            self.inputs.cache_dir, self.inputs.project_id,
            self.inputs.subject_id,
            self.inputs.subject_id + '_' + XNATArchive.SUMMARY_NAME))
        return session, cache_dir


class XNATProjectSink(XNATSink):

    input_spec = XNATProjectSinkInputSpec

    ACCEPTED_MULTIPLICITIES = ('per_project',)

    def _get_session(self, xnat_login):
        project = xnat_login.projects[self.inputs.project_id]
        subject_name = XNATArchive.project_summary_subject_name(project.id)
        try:
            subject = project.subjects[subject_name]
        except KeyError:
            subject = xnat_login.classes.SubjectData(
                label=subject_name, parent=project)
        session_name = XNATArchive.project_summary_session_name(project.id)
        try:
            session = subject.experiments[session_name]
        except KeyError:
            session = xnat_login.classes.MrSessionData(
                label=session_name, parent=subject)
        # Get cache dir for session
        cache_dir = os.path.abspath(os.path.join(
            self.inputs.cache_dir, self.inputs.project_id,
            self.inputs.project_id + '_' + XNATArchive.SUMMARY_NAME,
            self.inputs.project_id + '_' + XNATArchive.SUMMARY_NAME + '_' +
            XNATArchive.SUMMARY_NAME))
        return session, cache_dir


class XNATArchive(Archive):
    """
    An 'Archive' class for the DaRIS research management system.
    """

    type = 'xnat'
    Sink = XNATSink
    Source = XNATSource
    SubjectSink = XNATSubjectSink
    ProjectSink = XNATProjectSink

    SUMMARY_NAME = 'PROC'
    PROCESSED_SUFFIX = '_PROC'

    def __init__(self, user=None, password=None, cache_dir=None,
                 server='https://mbi-xnat.erc.monash.edu.au'):
        self._server = server
        self._user = user
        self._password = password
        if cache_dir is None:
            self._cache_dir = os.path.join(os.environ['HOME'], '.xnat')
        else:
            self._cache_dir = cache_dir
        try:
            # Attempt to make cache if it doesn't already exist
            os.makedirs(self._cache_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

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
        return xnat.connect(server=self._server, **sess_kwargs)

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
        # Convert subject ids to strings if they are integers
        if subject_ids is not None:
            subject_ids = [('{}_{0:03d}'.format(project_id, s)
                            if isinstance(s, int) else s) for s in subject_ids]
        subjects = []
        sessions = defaultdict(list)
        with self._login() as xnat_login:
            xproject = xnat_login.projects[project_id]
            for xsession in xproject.experiments.itervalues():
                xsubject = xsession.subject
                subj_id = xsubject.label
                sess_id = xsession.label.split('_')[2]
                if ((subject_ids is not None and subj_id not in subject_ids) or
                        (session_ids is not None and
                         sess_id not in session_ids)):
                    continue  # Skip session
                sessions[subj_id].append(Session(
                    sess_id,
                    datasets=self._get_datasets(xsession, 'per_session'),
                    processed=False))
            for xsubject in xproject.subjects.itervalues():
                subj_id = xsubject.label
                if subject_ids is not None and subj_id not in subject_ids:
                    continue
                subj_summary_name = self.subject_summary_session_name(subj_id)
                if subj_summary_name in xsubject.experiments:
                    subj_summary = self._get_datasets(
                        xsubject.experiments[subj_summary_name], 'per_subject')
                else:
                    subj_summary = []
                subjects.append(Subject(subj_id, sessions[subj_id],
                                        subj_summary))
            proj_summary_name = self.project_summary_session_name(
                project_id)
            if proj_summary_name in xproject.subjects:
                proj_summary = self._get_datasets(
                    xproject.subjects[
                        proj_summary_name].experiments[
                            self.project_summary_session_name(project_id)],
                    'per_project')
            else:
                proj_summary = []
            if not subjects:
                raise NiAnalysisError(
                    "Did not find any subjects matching the IDs '{}' in "
                    "project '{}' (found '{}')"
                    .format("', '".join(subject_ids), project_id,
                            "', '".join(s.label for s in xproject.subj)))
            if not sessions:
                raise NiAnalysisError(
                    "Did not find any sessions subjects matching the IDs '{}'"
                    "(in subjects '{}') for project '{}'"
                    .format("', '".join(session_ids),
                            "', '".join(s.label for s in xproject.subj),
                             project_id))
        return Project(project_id, subjects, proj_summary)

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
    def subject_summary_session_name(cls, subject_id):
        return '{}_{}'.format(subject_id, cls.SUMMARY_NAME)

    @classmethod
    def project_summary_session_name(cls, project_id):
        return '{}_{}_{}'.format(project_id, cls.SUMMARY_NAME,
                                 cls.SUMMARY_NAME)

    @classmethod
    def project_summary_subject_name(cls, project_id):
        return '{}_{}'.format(project_id, cls.SUMMARY_NAME)


def download_all_datasets(download_dir, server, user, password, session_id,
                          overwrite=True):
    with xnat.connect(server, user, password) as xnat_login:
        try:
            session = xnat_login.experiments[session_id]
        except KeyError:
            raise NiAnalysisError(
                "Didn't find session matching '{}' on {}".format(session_id,
                                                                 server))
        for dataset in session.scans.itervalues():
            data_format = _guess_data_format(dataset)
            download_path = os.path.join(
                download_dir,
                dataset.type + data_formats[data_format.lower()].extension)
            if overwrite or not os.path.exists(download_path):
                download_resource(download_path, dataset, data_format,
                                  session.label)


def download_dataset(download_path, server, user, password, session_id,
                     dataset_name, data_format=None):
    """
    Downloads a single dataset from an XNAT server
    """
    with xnat.connect(server, user=user, password=password) as xnat_login:
        try:
            session = xnat_login.experiments[session_id]
        except KeyError:
            raise NiAnalysisError(
                "Didn't find session matching '{}' on {}".format(session_id,
                                                                 server))
        try:
            dataset = session.scans[dataset_name]
        except KeyError:
            raise NiAnalysisError(
                "Didn't find dataset matching '{}' in {}".format(dataset_name,
                                                                 session_id))
        if data_format is None:
            data_format = _guess_data_format(dataset)
        download_resource(download_path, dataset, data_format, session.label)


def _guess_data_format(dataset):
    dataset_formats = [r for r in dataset.resources.itervalues()
                       if r.label.lower() in data_formats]
    if len(dataset_formats) > 1:
        raise NiAnalysisError(
            "Multiple valid resources for '{}' dataset, please pass "
            "'data_format' to 'download_dataset' method to speficy resource to"
            "download".format(dataset.type, "', '".join(dataset_formats)))
    return dataset_formats[0].label


def download_resource(download_path, dataset, data_format, session_label):

    ext = data_formats[data_format.lower()].extension
    try:
        resource = dataset.resources[data_format.upper()]
    except KeyError:
        raise NiAnalysisError(
            "Didn't find {} resource in {} dataset matching '{}' in {}"
            .format(data_format.upper(), dataset.type))
    tmp_dir = download_path + '.download'
    resource.download_dir(tmp_dir)
    dataset_label = dataset.id + '-' + sanitize_re.sub('_', dataset.type)
    src_path = os.path.join(tmp_dir, session_label, 'scans',
                            dataset_label, 'resources',
                            data_format.upper(), 'files')
    if data_format.lower() != 'dicom':
        src_path = os.path.join(src_path, dataset.type + ext)
    shutil.move(src_path, download_path)
    shutil.rmtree(tmp_dir)


def list_datasets(server, user, password, session_id):
    with xnat.connect(server, user=user, password=password) as xnat_login:
        session = xnat_login.experiments[session_id]
        return [s.type for s in session.scans.itervalues()]
