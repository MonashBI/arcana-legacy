from __future__ import absolute_import
from builtins import zip
oldstr = str
from builtins import str  # @IgnorePep8
from builtins import object  # @IgnorePep8
from future.utils import PY2  # @IgnorePep8
from abc import ABCMeta  # @IgnorePep8
import os.path  # @IgnorePep8
import tempfile  # @IgnorePep8
from itertools import repeat  # @IgnorePep8
import shutil  # @IgnorePep8
import hashlib  # @IgnorePep8
from future.utils import with_metaclass  # @IgnorePep8
from arcana.utils import JSON_ENCODING  # @IgnorePep8
import stat  # @IgnorePep8
import time  # @IgnorePep8
import logging  # @IgnorePep8
import errno  # @IgnorePep8
import json  # @IgnorePep8
from zipfile import ZipFile, BadZipfile  # @IgnorePep8
from collections import defaultdict  # @IgnorePep8
from nipype.interfaces.base import Directory, traits, isdefined  # @IgnorePep8
from arcana.dataset import Dataset, Field  # @IgnorePep8
from arcana.repository.base import (  # @IgnorePep8
    BaseRepository, RepositorySource, RepositorySink, RepositorySourceInputSpec,
    RepositorySinkInputSpec, RepositorySubjectSinkInputSpec,
    RepositoryVisitSinkInputSpec,
    RepositoryProjectSinkInputSpec,
    RepositorySubjectSink, RepositoryVisitSink, RepositoryProjectSink,
    MULTIPLICITIES)
from arcana.repository.tree import Session, Subject, Project, Visit  # @IgnorePep8
from arcana.file_format import FileFormat  # @IgnorePep8
from arcana.utils import split_extension  # @IgnorePep8
from arcana.exception import (  # @IgnorePep8
    ArcanaError, ArcanaFileFormatError, ArcanaMissingDataException)
from arcana.utils import dir_modtime, NoContextWrapper  # @IgnorePep8
import re  # @IgnorePep8
import xnat  # @IgnorePep8
from arcana.utils import PATH_SUFFIX, FIELD_SUFFIX  # @IgnorePep8

logger = logging.getLogger('arcana')

special_char_re = re.compile(r'[^a-zA-Z_0-9]')
tag_parse_re = re.compile(r'\((\d+),(\d+)\)')

RELEVANT_DICOM_TAG_TYPES = set(('UI', 'CS', 'DA', 'TM', 'SH', 'LO',
                                'PN', 'ST', 'AS'))

BUILTIN_XNAT_FIELDS = []


def lower(s):
    if s is None:
        return None
    return s.lower()


class XnatMixin(object):

    @property
    def session_id(self):
        return self.inputs.subject_id + '_' + self.inputs.visit_id


class XnatSourceInputSpec(RepositorySourceInputSpec):
    project_id = traits.Str(mandatory=True, desc='The project ID')
    server = traits.Str(mandatory=True,
                        desc="The address of the XNAT server")
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


class XnatSource(RepositorySource, XnatMixin):
    """
    A NiPype IO interface for grabbing datasets off DaRIS (analogous to
    DataGrabber)
    """

    input_spec = XnatSourceInputSpec

    def __init__(self, *args, **kwargs):
        self._check_md5 = kwargs.pop('check_md5', True)
        super(XnatSource, self).__init__(*args, **kwargs)

    @property
    def check_md5(self):
        return self._check_md5

    def _list_outputs(self):
        # FIXME: Should probably not prepend the project before this point
        subject_id = self.inputs.subject_id.split('_')[-1]
        visit_id = self.inputs.visit_id
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
            # Get primary session, derived and summary sessions and cache
            # dirs
            sessions = {}
            cache_dirs = {}
            for freq, derived in ([('per_session', False)] +
                                    list(zip(MULTIPLICITIES, repeat(True)))):
                subj_label, sess_label = XnatRepository.get_labels(
                    freq, self.inputs.project_id, subject_id, visit_id)
                if freq == 'per_session' and derived:
                    sess_label += XnatRepository.PROCESSED_SUFFIX
                cache_dirs[(freq, derived)] = os.path.join(
                    base_cache_dir, subj_label, sess_label)
                try:
                    subject = project.subjects[subj_label]
                    sessions[(freq, derived)] = subject.experiments[
                        sess_label]
                except KeyError:
                    continue
            outputs = {}
            for dataset in self.datasets:
                try:
                    session = sessions[(dataset.frequency,
                                        dataset.derived)]
                except KeyError:
                    raise ArcanaMissingDataException(
                        "Did not find{} session for frequency '{}', "
                        "it was expected to find {} in"
                        .format(
                            (' derived' if dataset.frequency else ''),
                            dataset.frequency, dataset))
                cache_dir = cache_dirs[(dataset.frequency,
                                        dataset.derived)]
                try:
                    xdataset = session.scans[
                        dataset.basename(subject_id=subject_id,
                                         visit_id=visit_id)]
                except KeyError:
                    raise ArcanaError(
                        "Could not find '{}' dataset in session '{}' "
                        "(found {})".format(
                            dataset.prefixed_name, session.label,
                            "', '".join(list(session.scans.keys()))))
                # Get filename
                fname = dataset.fname(subject_id=subject_id,
                                      visit_id=visit_id)
                # Get resource to check its MD5 digest
                xresource = self.get_resource(xdataset, dataset)
                need_to_download = True
                # FIXME: Should do a check to see if versions match
                if not os.path.exists(cache_dir):
                    os.makedirs(cache_dir)
                cache_path = os.path.join(cache_dir, fname)
                if os.path.exists(cache_path):
                    if self.check_md5:
                        md5_path = (cache_path +
                                    XnatRepository.MD5_SUFFIX)
                        try:
                            with open(md5_path, 'r') as f:
                                cached_digests = json.load(f)
                            digests = self.get_digests(xresource)
                            if cached_digests == digests:
                                need_to_download = False
                        except IOError:
                            pass
                    else:
                        need_to_download = False
                if need_to_download:
                    # The path to the directory which the files will be
                    # downloaded to.
                    tmp_dir = cache_path + '.download'
                    try:
                        # Attempt to make tmp download directory. This will
                        # fail if another process (or previous attempt) has
                        # already created it. In that case this process will
                        # wait to see if that download finishes successfully,
                        # and if so use the cached version.
                        os.mkdir(tmp_dir)
                    except OSError as e:
                        if e.errno == errno.EEXIST:
                            # Another process may be concurrently downloading
                            # the same file to the cache. Wait for
                            # 'race_cond_delay' seconds and then check that it
                            # has been completed or assume interrupted and
                            # redownload.
                            self.delayed_download(
                                tmp_dir, xresource, xdataset, dataset,
                                session.label, cache_path,
                                delay=self.inputs.race_cond_delay)
                        else:
                            raise
                    else:
                        self.download_dataset(
                            tmp_dir, xresource, xdataset, dataset,
                            session.label, cache_path)
                outputs[dataset.name + PATH_SUFFIX] = cache_path
            for field in self.fields:
                prefixed_name = field.prefixed_name
                session = sessions[(field.frequency,
                                    field.derived)]
                outputs[field.name + FIELD_SUFFIX] = field.dtype(
                    session.fields[prefixed_name])
        return outputs


class XnatSinkInputSpecMixin(object):
    project_id = traits.Str(mandatory=True, desc='The project ID')
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


class XnatSinkInputSpec(RepositorySinkInputSpec, XnatSinkInputSpecMixin):
    pass


class XnatSubjectSinkInputSpec(RepositorySubjectSinkInputSpec,
                               XnatSinkInputSpecMixin):
    pass


class XnatVisitSinkInputSpec(RepositoryVisitSinkInputSpec,
                                 XnatSinkInputSpecMixin):
    pass


class XnatProjectSinkInputSpec(RepositoryProjectSinkInputSpec,
                               XnatSinkInputSpecMixin):
    pass


class XnatSinkMixin(with_metaclass(ABCMeta, XnatMixin)):
    """
    A NiPype IO interface for putting derived datasets onto DaRIS (analogous
    to DataSink)
    """

    def _list_outputs(self):
        """Execute this module.
        """
        # Initiate output
        outputs = self._base_outputs()
        out_files = []
        missing_files = []
        # Open XNAT session
        sess_kwargs = {}
        if 'user' in self.inputs.trait_names():  # Because InputSpec is dynamic
            sess_kwargs['user'] = self.inputs.user
        if 'password' in self.inputs.trait_names():
            sess_kwargs['password'] = self.inputs.password
        logger.debug("Session kwargs: {}".format(sess_kwargs))
        with xnat.connect(server=self.inputs.server,
                          **sess_kwargs) as xnat_login:
            # Add session for derived scans if not present
            session, cache_dir = self._get_session(xnat_login)
            # Make session cache dir
            if not os.path.exists(cache_dir):
                os.makedirs(cache_dir, stat.S_IRWXU | stat.S_IRWXG)
            # Loop through datasets connected to the sink and copy them to the
            # cache directory and upload to daris.
            for dataset in self.datasets:
                assert dataset.frequency == self.frequency
                assert dataset.derived, (
                    "{} (format: {}, freq: {}) isn't derived"
                    .format(dataset.name, dataset.format_name,
                            dataset.frequency))
                filename = getattr(self.inputs,
                                   dataset.name + PATH_SUFFIX)
                if not isdefined(filename):
                    missing_files.append(dataset.name)
                    continue  # skip the upload for this file
                ext = dataset.format.extension
                assert split_extension(filename)[1] == ext, (
                    "Mismatching extension '{}' for format '{}' ('{}')"
                    .format(split_extension(filename)[1],
                            dataset.format.name,
                            dataset.format.extension))
                src_path = os.path.abspath(filename)
                out_fname = dataset.fname()
                # Copy to local cache
                dst_path = os.path.join(cache_dir, out_fname)
                out_files.append(dst_path)
                shutil.copyfile(src_path, dst_path)
                # Create md5 digest
                with open(dst_path, 'rb') as f:
                    digests = {out_fname: hashlib.md5(f.read()).hexdigest()}
                with open(dst_path + XnatRepository.MD5_SUFFIX, 'w',
                          **JSON_ENCODING) as f:
                    json.dump(digests, f)
                # Upload to XNAT
                xdataset = xnat_login.classes.MrScanData(
                    type=dataset.basename(), parent=session)
                # Delete existing resource
                # TODO: probably should have check to see if we want to
                #       override it
                try:
                    xresource = xdataset.resources[
                        dataset.format.name.upper()]
                    xresource.delete()
                except KeyError:
                    pass
                xresource = xdataset.create_resource(
                    dataset.format.name.upper())
                xresource.upload(dst_path, out_fname)
            for field in self.fields:
                assert field.frequency == self.frequency
                assert field.derived, ("{} isn't derived".format(
                    field))
                val = getattr(self.inputs, field.name + FIELD_SUFFIX)
                if PY2 and isinstance(val, basestring):  # @UndefinedVariable
                    val = oldstr(val)
                session.fields[field.prefixed_name] = val
        if missing_files:
            # FIXME: Not sure if this should be an exception or not,
            #        indicates a problem but stopping now would throw
            #        away the datasets that were created
            logger.warning(
                "Missing output datasets '{}' in XnatSink".format(
                    "', '".join(str(f) for f in missing_files)))
        # Return cache file paths
        outputs['out_files'] = out_files
        return outputs

    def _get_session(self, xnat_login):
        project = xnat_login.projects[self.inputs.project_id]
        # FIXME: Subject should probably be input without the project prefix
        try:
            subject_id = self.inputs.subject_id.split('_')[-1]
        except AttributeError:
            subject_id = None
        try:
            visit_id = self.inputs.visit_id
        except AttributeError:
            visit_id = None
        subj_label, sess_label = XnatRepository.get_labels(
            self.frequency, self.inputs.project_id, subject_id, visit_id)
        if self.frequency == 'per_session':
            sess_label += XnatRepository.PROCESSED_SUFFIX
            if visit_id is not None:
                visit_id += XnatRepository.PROCESSED_SUFFIX
        try:
            subject = project.subjects[subj_label]
        except KeyError:
            subject = xnat_login.classes.SubjectData(
                label=subj_label, parent=project)
        try:
            session = subject.experiments[sess_label]
        except KeyError:
            session = self._create_session(xnat_login, subj_label,
                                           sess_label)
        # Get cache dir for session
        cache_dir = os.path.abspath(os.path.join(
            self.inputs.cache_dir, self.inputs.project_id, subject.label,
            session.label))
        return session, cache_dir

    def _create_session(self, xnat_login, subject_id, visit_id):
        """
        This creates a derived session in a way that respects whether
        the acquired session has been shared into another project or not.

        If we weren't worried about this we could just use

            session = xnat_login.classes.MrSessionData(label=derived_session_id,
                                                       parent=subject)
        """
        uri = ('/data/archive/projects/{}/subjects/{}/experiments/{}'
               .format(self.inputs.project_id, subject_id, visit_id))
        query = {'xsiType': 'xnat:mrSessionData', 'label': visit_id,
                 'req_format': 'qa'}
        response = xnat_login.put(uri, query=query)
        if response.status_code not in (200, 201):
            raise ArcanaError(
                "Could not create session '{}' in subject '{}' in project '{}'"
                " response code {}"
                .format(visit_id, subject_id, self.inputs.project_id,
                        response))
        return xnat_login.classes.MrSessionData(uri=uri,
                                                xnat_session=xnat_login)


class XnatSink(XnatSinkMixin, RepositorySink):

    input_spec = XnatSinkInputSpec


class XnatSubjectSink(XnatSinkMixin, RepositorySubjectSink):

    input_spec = XnatSubjectSinkInputSpec


class XnatVisitSink(XnatSinkMixin, RepositoryVisitSink):

    input_spec = XnatVisitSinkInputSpec


class XnatProjectSink(XnatSinkMixin, RepositoryProjectSink):

    input_spec = XnatProjectSinkInputSpec


class XnatRepository(BaseRepository):
    """
    An 'Repository' class for XNAT repositories

    Parameters
    ----------
    project_id : str
        The ID of the project on XNAT
    user : str
        Username with which to connect to XNAT with
    password : str
        Password to connect to XNAt with
    cache_dir : str (path)
        Path to local directory to cache XNAT data in
    server : str (URI)
        URI of XNAT server to connect to
    check_md5 : bool
        Whether to check the MD5 digest of cached files before using. This
        checks for updates on the server since the file was cached
    subject_ids : list(str) | None
        A list of subject IDs to filter the project search for. Will
        reduce the time taken to initialise the repository but will also
        limit the subjects that can be analysed
    visit_ids : list(str) | None
        A list of subject IDs to filter the project search for. Will
        reduce the time taken to initialise the repository but will also
        limit the subjects that can be analysed
    race_cond_delay : int
        The amount of time to wait before checking that the required
        dataset has been downloaded to cache by another process has
        completed if they are attempting to download the same dataset
    """

    type = 'xnat'
    Sink = XnatSink
    Source = XnatSource
    SubjectSink = XnatSubjectSink
    VisitSink = XnatVisitSink
    ProjectSink = XnatProjectSink

    SUMMARY_NAME = 'ALL'
    PROCESSED_SUFFIX = '_PROC'
    MD5_SUFFIX = '.md5.json'

    def __init__(self, server, project_id, user=None, password=None,
                 cache_dir=None, check_md5=True, race_cond_delay=30):
        self._project_id = project_id
        self._server = server
        self._user = user
        self._password = password
        self.race_cond_delay = race_cond_delay
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
        self._check_md5 = check_md5

    def __repr__(self):
        return ("{}(server={}, project_id={}, cache_dir={})"
                .format(type(self).__name__,
                        self.server, self.project_id,
                        self._cache_dir))

    def __eq__(self, other):
        try:
            return (self.server == other.server and
                    self._cache_dir == other._cache_dir and
                    self.project_id == other.project_id)
        except AttributeError:
            return False  # For comparison with other types

    def connect(self):
        """
        Parameters
        ----------
        prev_login : xnat.XNATSession
            An XNAT login that has been opened in the code that calls
            the method that calls login. It is wrapped in a
            NoExitWrapper so the returned connection can be used
            in a "with" statement in the method.
        """
        sess_kwargs = {}
        if self._user is not None:
            sess_kwargs['user'] = self._user
        if self._password is not None:
            sess_kwargs['password'] = self._password
        self._login = xnat.connect(server=self._server, **sess_kwargs)

    def disconnect(self):
        self._login.disconnect()

    def get_dataset(self, dataset):
        """
        Caches a single dataset (if the 'path' attribute is accessed
        and it has not been previously cached for example

        Parameters
        ----------
        dataset : Dataset
            The dataset to cache
        prev_login : xnat.XNATSession
            An XNATSession object to use for the connection. A new
            one is created if one isn't provided
        """
        if dataset.repository is not self:
            raise ArcanaError(
                "{} is not from {}".format(dataset, self))
        assert dataset.uri is not None
        with self:  # Connect to the XNAT repository if haven't already
            sess_id, scan_id = re.match(
                r'/data/experiments/(\w+)/scans/(.*)',
                dataset.uri).groups()
            xsession = self.login.experiments[sess_id]
            xdataset = xsession.scans[scan_id]
            cache_path = self.cache_path(dataset)
            # Get resource to check its MD5 digest
            xresource = self.get_resource(xdataset, dataset)
            need_to_download = True
            if os.path.exists(cache_path):
                if self.check_md5:
                    md5_path = (cache_path +
                                XnatRepository.MD5_SUFFIX)
                    try:
                        with open(md5_path, 'r') as f:
                            cached_digests = json.load(f)
                        digests = self.get_digests(xresource)
                        if cached_digests == digests:
                            need_to_download = False
                    except IOError:
                        pass
                else:
                    need_to_download = False
            if need_to_download:
                # The path to the directory which the files will be
                # downloaded to.
                tmp_dir = cache_path + '.download'
                try:
                    # Attempt to make tmp download directory. This will
                    # fail if another process (or previous attempt) has
                    # already created it. In that case this process will
                    # wait to see if that download finishes successfully,
                    # and if so use the cached version.
                    os.mkdir(tmp_dir)
                except OSError as e:
                    if e.errno == errno.EEXIST:
                        # Another process may be concurrently downloading
                        # the same file to the cache. Wait for
                        # 'race_cond_delay' seconds and then check that it
                        # has been completed or assume interrupted and
                        # redownload.
                        self.delayed_download(
                            tmp_dir, xresource, xdataset, dataset,
                            xsession.label, cache_path,
                            delay=self.race_cond_delay)
                    else:
                        raise
                else:
                    self.download_dataset(
                        tmp_dir, xresource, xdataset, dataset,
                        xsession.label, cache_path)
        return cache_path

    def put_dataset(self, dataset):
        assert dataset.derived, (
            "{} (format: {}, freq: {}) isn't derived"
            .format(dataset.name, dataset.format_name,
                    dataset.frequency))
        filename = getattr(self.inputs,
                           dataset.name + PATH_SUFFIX)
        ext = dataset.format.extension
        assert split_extension(filename)[1] == ext, (
            "Mismatching extension '{}' for format '{}' ('{}')"
            .format(split_extension(filename)[1],
                    dataset.format.name,
                    dataset.format.extension))
        src_path = os.path.abspath(filename)
        out_fname = dataset.fname()
        # Copy to local cache
        dst_path = os.path.join(cache_dir, out_fname)
        shutil.copyfile(src_path, dst_path)
        # Create md5 digest
        with open(dst_path, 'rb') as f:
            digests = {out_fname: hashlib.md5(f.read()).hexdigest()}
        with open(dst_path + XnatRepository.MD5_SUFFIX, 'w',
                  **JSON_ENCODING) as f:
            json.dump(digests, f)
        # Upload to XNAT
        xdataset = self._login.classes.MrScanData(
            type=dataset.basename(), parent=session)
        # Delete existing resource
        # TODO: probably should have check to see if we want to
        #       override it
        try:
            xresource = xdataset.resources[
                dataset.format.name.upper()]
            xresource.delete()
        except KeyError:
            pass
        xresource = xdataset.create_resource(
            dataset.format.name.upper())
        xresource.upload(dst_path, out_fname)

    def put_field(self, field):
        for field in self.fields:
            assert field.frequency == self.frequency
            assert field.derived, ("{} isn't derived".format(
                field))
            val = getattr(self.inputs, field.name + FIELD_SUFFIX)
            if PY2 and isinstance(val, basestring):  # @UndefinedVariable
                val = oldstr(val)
            session.fields[field.prefixed_name] = val

    def cache_path(self, dataset):
        subj_dir, sess_dir = self.get_labels(
            dataset.frequency, self.project_id,
            dataset.subject_id, dataset.visit_id)
        return os.path.join(self._cache_dir, self.project_id,
                            subj_dir, sess_dir, dataset.fname())

    def all_session_ids(self, project_id):
        """
        Parameters
        ----------
        project_id : int
            The project id to return the sessions for
        repo_id : int
            The id of the repository (2 for monash daris)
        visit_ids: int|List[int]|None
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
                    project_id].experiments.values()]

    def tree(self, subject_ids=None, visit_ids=None):
        """
        Return the tree of subject and sessions information within a
        project in the XNAT repository

        Parameters
        ----------
        subject_ids : list(str)
            List of subject IDs with which to filter the tree with. If
            None all are returned
        visit_ids : list(str)
            List of visit IDs with which to filter the tree with. If
            None all are returned

        Returns
        -------
        project : arcana.repository.Project
            A hierarchical tree of subject, session and dataset
            information for the repository
        """
        # Convert subject ids to strings if they are integers
        if subject_ids is not None:
            subject_ids = [
                ('{:03d}'.format(s)
                 if isinstance(s, int) else s) for s in subject_ids]
        # Add derived visit IDs to list of visit ids to filter
        if visit_ids is not None:
            visit_ids = visit_ids + [i + self.PROCESSED_SUFFIX
                                     for i in visit_ids]
        subjects = []
        sessions = defaultdict(list)
        with self.login() as xnat_login:
            xproject = xnat_login.projects[self.project_id]
            visit_sessions = defaultdict(list)
            # Create list of subjects
            for xsubject in xproject.subjects.values():
                # This assumes that the subject ID is prepended with
                # the project ID
                subj_id = xsubject.label[(len(self.project_id) + 1):]
                if subj_id == XnatRepository.SUMMARY_NAME:
                    continue
                if (subject_ids is not None and
                        subj_id not in subject_ids):
                    continue
                logger.debug("Getting info for subject '{}'"
                             .format(subj_id))
                sessions = {}
                derived_sessions = []
                # Get per_session datasets
                for xsession in xsubject.experiments.values():
                    visit_id = '_'.join(xsession.label.split('_')[2:])
                    if visit_id == XnatRepository.SUMMARY_NAME:
                        continue
                    if not (visit_ids is None or visit_id in visit_ids):
                        continue
                    derived = xsession.label.endswith(
                        self.PROCESSED_SUFFIX)
                    session = Session(subj_id, visit_id,
                                      datasets=self._get_datasets(
                                          xsession, 'per_session',
                                          subject_id=subj_id,
                                          visit_id=visit_id,
                                          derived=derived),
                                      fields=self._get_fields(
                                          xsession, 'per_session',
                                          subject_id=subj_id,
                                          visit_id=visit_id,
                                          derived=derived),
                                      derived=None)
                    if derived:
                        derived_sessions.append(session)
                    else:
                        sessions[visit_id] = session
                        visit_sessions[visit_id].append(session)
                for derived_session in derived_sessions:
                    visit_id = derived_session.visit_id[:-len(
                        self.PROCESSED_SUFFIX)]
                    try:
                        sessions[visit_id].derived = derived_session
                    except KeyError:
                        raise ArcanaError(
                            "No matching acquired session for derived "
                            "session '{}_{}_{}'".format(
                                self.project_id,
                                derived_session.subject_id,
                                derived_session.visit_id))
                # Get per_subject datasets
                subj_summary_name = self.get_labels(
                    'per_subject', self.project_id, subj_id)[1]
                try:
                    xsubj_summary = xsubject.experiments[
                        subj_summary_name]
                except KeyError:
                    subj_datasets = []
                    subj_fields = []
                else:
                    subj_datasets = self._get_datasets(
                        xsubj_summary, 'per_subject',
                        subject_id=subj_id)
                    subj_fields = self._get_fields(
                        xsubj_summary, 'per_subject',
                        subject_id=subj_id)
                subjects.append(Subject(subj_id,
                                        sorted(sessions.values()),
                                        datasets=subj_datasets,
                                        fields=subj_fields))
            # Create list of visits
            visits = []
            for visit_id, v_sessions in visit_sessions.items():
                (_, visit_summary_sess_name) = self.get_labels(
                    'per_visit', self.project_id, visit_id=visit_id)
                # Get 'per_visit' datasets
                try:
                    xvisit_summary = xproject.experiments[
                        visit_summary_sess_name]
                except KeyError:
                    visit_datasets = []
                    visit_fields = {}
                else:
                    visit_datasets = self._get_datasets(
                        xvisit_summary, 'per_visit', visit_id=visit_id)
                    visit_fields = self._get_fields(
                        xvisit_summary, 'per_visit', visit_id=visit_id)
                visits.append(Visit(visit_id, sorted(v_sessions),
                                    datasets=visit_datasets,
                                    fields=visit_fields))
            # Get 'per_project' datasets
            (proj_summary_subj_name,
             proj_summary_sess_name) = self.get_labels('per_project',
                                                       self.project_id)
            try:
                xproj_summary = xproject.subjects[
                    proj_summary_subj_name].experiments[
                        proj_summary_sess_name]
            except KeyError:
                proj_datasets = []
                proj_fields = []
            else:
                proj_datasets = self._get_datasets(xproj_summary,
                                                   'per_project')
                proj_fields = self._get_fields(xproj_summary,
                                               'per_project')
            if not subjects:
                raise ArcanaError(
                    "Did not find any subjects matching the IDs '{}' in "
                    "project '{}' (found '{}')"
                    .format(
                        ("', '".join(subject_ids)
                         if subject_ids is not None else ''),
                        self.project_id,
                        "', '".join(
                            s.label[(len(self.project_id) + 1):]
                            for s in list(xproject.subjects.values()))))
            if not sessions:
                raise ArcanaError(
                    "Did not find any sessions matching the visit IDs "
                    "'{}' (in subjects '{}') for project '{}'"
                    .format(
                        ("', '".join(visit_ids)
                         if visit_ids is not None else ''),
                        "', '".join(
                            s.label.split('_')[1]
                            for s in list(xproject.experiments.values())),
                        self.project_id))
        return Project(sorted(subjects), sorted(visits),
                       datasets=proj_datasets, fields=proj_fields)

    def _get_datasets(self, xsession, freq, subject_id=None,
                      visit_id=None, derived=False):
        """
        Returns a list of datasets within an XNAT session

        Parameters
        ----------
        xsession : xnat.classes.MrSessionData
            The XNAT session to extract the datasets from
        freq : str
            The frequency of the returned datasets (either 'per_session',
            'per_subject', 'per_visit', or 'per_project')
        derived : bool
            Whether the session is derived or not

        Returns
        -------
        datasets : list(arcana.dataset.Dataset)
            List of datasets within an XNAT session
        """
        datasets = []
        for xdataset in xsession.scans.values():
            try:
                file_format = guess_file_format(xdataset)
            except ArcanaFileFormatError as e:
                logger.warning(
                    "Ignoring '{}' as couldn't guess its file format:\n{}"
                    .format(xdataset.type, e))
            datasets.append(Dataset(
                xdataset.type, format=file_format, derived=derived,  # @ReservedAssignment @IgnorePep8
                frequency=freq, path=None, id=xdataset.id,
                uri=xdataset.uri, subject_id=subject_id,
                visit_id=visit_id, repository=self))
        return sorted(datasets)

    def _get_fields(self, xsession, freq, subject_id=None,
                    visit_id=None, derived=False):
        """
        Returns a list of fields within an XNAT session

        Parameters
        ----------
        xsession : xnat.classes.MrSessionData
            The XNAT session to extract the fields from
        freq : str
            The frequency of the returned fields (either 'per_session',
            'per_subject', 'per_visit', or 'per_project')

        Returns
        -------
        fields : list(arcana.dataset.Field)
            List of fields within an XNAT session
        """
        fields = []
        for name, value in list(xsession.fields.items()):
            fields.append(Field(
                name=name, value=value, derived=derived,
                frequency=freq, subject_id=subject_id,
                visit_id=visit_id, repository=self))
        return sorted(fields)

    def dicom_header(self, dataset, prev_login=None):
        with self.login(prev_login) as xnat_login:
            response = xnat_login.get(
                '/REST/services/dicomdump?src=/archive/projects/{}'
                '{}&format=json'
                .format(self.project_id, dataset.uri[len('/data'):]))
        def convert(val, code):  # @IgnorePep8
            if code == 'TM':
                try:
                    val = float(val)
                except ValueError:
                    pass
            elif code == 'CS':
                val = val.split('\\')
            return val
        hdr = {tag_parse_re.match(t['tag1']).groups():
               convert(t['value'], t['vr'])
               for t in response.json()['ResultSet']['Result']
               if (tag_parse_re.match(t['tag1']) and
                   t['vr'] in RELEVANT_DICOM_TAG_TYPES)}
        return hdr

    @property
    def project_id(self):
        return self._project_id

    @property
    def server(self):
        return self._server

    @property
    def cache_dir(self):
        return self._cache_dir

    @classmethod
    def get_labels(cls, frequency, project_id, subject_id=None,
                   visit_id=None):
        """
        Returns the labels for the XNAT subject and sessions given
        the frequency and provided IDs.
        """
        if frequency == 'per_session':
            assert visit_id is not None
            assert subject_id is not None
            subj_label = '{}_{}'.format(project_id, subject_id)
            sess_label = '{}_{}_{}'.format(project_id, subject_id,
                                           visit_id)
        elif frequency == 'per_subject':
            assert subject_id is not None
            subj_label = '{}_{}'.format(project_id, subject_id)
            sess_label = '{}_{}_{}'.format(project_id, subject_id,
                                           cls.SUMMARY_NAME)
        elif frequency == 'per_visit':
            assert visit_id is not None
            subj_label = '{}_{}'.format(project_id, cls.SUMMARY_NAME)
            sess_label = '{}_{}_{}'.format(project_id, cls.SUMMARY_NAME,
                                           visit_id)
        elif frequency == 'per_project':
            subj_label = '{}_{}'.format(project_id, cls.SUMMARY_NAME)
            sess_label = '{}_{}_{}'.format(project_id, cls.SUMMARY_NAME,
                                           cls.SUMMARY_NAME)
        else:
            raise ArcanaError(
                "Unrecognised frequency '{}'".format(frequency))
        return (subj_label, sess_label)

    @classmethod
    def get_resource(cls, xdataset, dataset):
        for resource_name in dataset.format.xnat_resource_names:
            try:
                return xdataset.resources[resource_name]
            except KeyError:
                continue
        raise ArcanaError(
            "'{}' dataset is not available in '{}' format(s), "
            "available resources are '{}'"
            .format(
                dataset.name,
                "', '".join(dataset.format.xnat_resource_names),
                "', '".join(
                    r.label for r in list(dataset.resources.values()))))

    @classmethod
    def get_digests(cls, resource):
        """
        Downloads the MD5 digests associated with the files in a resource.
        These are saved with the downloaded files in the cache and used to
        check if the files have been updated on the server
        """
        result = resource.xnat_session.get(resource.uri + '/files')
        if result.status_code != 200:
            raise ArcanaError(
                "Could not download metadata for resource {}"
                .format(resource.id))
        return dict((r['Name'], r['digest'])
                    for r in result.json()['ResultSet']['Result'])

    @classmethod
    def download_dataset(cls, tmp_dir, xresource, xdataset, dataset,
                         session_label, cache_path):
        # Download resource to zip file
        zip_path = os.path.join(tmp_dir, 'download.zip')
        with open(zip_path, 'wb') as f:
            xresource.xnat_session.download_stream(
                xresource.uri + '/files', f, format='zip', verbose=True)
        digests = cls.get_digests(xresource)
        # Extract downloaded zip file
        expanded_dir = os.path.join(tmp_dir, 'expanded')
        try:
            with ZipFile(zip_path) as zip_file:
                zip_file.extractall(expanded_dir)
        except BadZipfile as e:
            raise ArcanaError(
                "Could not unzip file '{}' ({})"
                .format(xresource.id, e))
        data_path = os.path.join(
            expanded_dir, session_label, 'scans',
            (xdataset.id + '-' + special_char_re.sub('_', xdataset.type)),
            'resources', xresource.label, 'files')
        if not dataset.format.directory:
            # If the dataformat is not a directory (e.g. DICOM),
            # attempt to locate a single file within the resource
            # directory with the appropriate filename and add that
            # to be the complete data path.
            fnames = os.listdir(data_path)
            match_fnames = [
                f for f in fnames
                if (lower(split_extension(f)[-1]) ==
                    lower(dataset.format.extension))]
            if len(match_fnames) == 1:
                data_path = os.path.join(data_path, match_fnames[0])
            else:
                raise ArcanaMissingDataException(
                    "Did not find single file with extension '{}' "
                    "(found '{}') in resource '{}'"
                    .format(dataset.format.extension,
                            "', '".join(fnames), data_path))
        try:
            os.makedirs(os.path.dirname(cache_path))
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        shutil.move(data_path, cache_path)
        with open(cache_path + XnatRepository.MD5_SUFFIX, 'w',
                  **JSON_ENCODING) as f:
            json.dump(digests, f)
        shutil.rmtree(tmp_dir)

    @classmethod
    def delayed_download(cls, tmp_dir, xresource, xdataset, dataset,
                         session_label, cache_path, delay):
        logger.info("Waiting {} seconds for incomplete download of '{}' "
                    "initiated another process to finish"
                    .format(delay, cache_path))
        initial_mod_time = dir_modtime(tmp_dir)
        time.sleep(delay)
        if os.path.exists(cache_path):
            logger.info("The download of '{}' has completed "
                        "successfully in the other process, continuing"
                        .format(cache_path))
            return
        elif initial_mod_time != dir_modtime(tmp_dir):
            logger.info(
                "The download of '{}' hasn't completed yet, but it has"
                " been updated.  Waiting another {} seconds before "
                "checking again.".format(cache_path, delay))
            cls.delayed_download(tmp_dir, xresource, xdataset,
                                   dataset,
                                   session_label, cache_path, delay)
        else:
            logger.warning(
                "The download of '{}' hasn't updated in {} "
                "seconds, assuming that it was interrupted and "
                "restarting download".format(cache_path, delay))
            shutil.rmtree(tmp_dir)
            os.mkdir(tmp_dir)
            cls.download_dataset(
                tmp_dir, xresource, xdataset, dataset, session_label,
                cache_path)


def guess_file_format(xdataset):
    # Use a set here as in some cases there are multiple resources
    # the same format (e.g. DICOM + secondary)
    dataset_formats = set()
    for xresource in xdataset.resources.values():
        try:
            dataset_formats.add(FileFormat.by_names[
                xresource.label.lower()])
        except KeyError:
            logger.debug("Ignoring resource '{}' in dataset {}"
                         .format(xresource.label, xdataset.type))
    if not dataset_formats:
        raise ArcanaFileFormatError(
            "No recognised data formats for '{}' dataset (available "
            "resources are '{}')".format(
                xdataset.type, "', '".join(
                    r.label for r in xdataset.resources.values())))
    elif len(dataset_formats) > 1:
        raise ArcanaFileFormatError(
            "Multiple valid data-formats '{}' for '{}' dataset, please "
            "pass 'file_format' to 'download_dataset' method to speficy"
            " resource to download".format(
                "', '".join(f.label for f in dataset_formats),
                xdataset.type))
    return next(iter(dataset_formats))
