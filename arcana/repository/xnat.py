from __future__ import absolute_import
oldstr = str
import os  # IgnorePep8
from future.utils import PY2  # @IgnorePep8
from arcana.utils import makedirs  # @IgnorePep8
import os.path as op # @IgnorePep8
import shutil  # @IgnorePep8
import hashlib  # @IgnorePep8
from arcana.utils import JSON_ENCODING  # @IgnorePep8
import stat  # @IgnorePep8
import time  # @IgnorePep8
import logging  # @IgnorePep8
import errno  # @IgnorePep8
import json  # @IgnorePep8
from zipfile import ZipFile, BadZipfile  # @IgnorePep8
from collections import defaultdict  # @IgnorePep8
from arcana.data import Fileset, Field  # @IgnorePep8
from arcana.repository.base import BaseRepository  # @IgnorePep8
from arcana.repository.tree import Session, Subject, Tree, Visit  # @IgnorePep8
from arcana.data.file_format import FileFormat  # @IgnorePep8
from arcana.utils import split_extension  # @IgnorePep8
from arcana.exception import (  # @IgnorePep8
    ArcanaError, ArcanaFileFormatError, ArcanaMissingDataException)
from arcana.utils import dir_modtime, lower  # @IgnorePep8
import re  # @IgnorePep8
import xnat  # @IgnorePep8

logger = logging.getLogger('arcana')

special_char_re = re.compile(r'[^a-zA-Z_0-9]')
tag_parse_re = re.compile(r'\((\d+),(\d+)\)')

RELEVANT_DICOM_TAG_TYPES = set(('UI', 'CS', 'DA', 'TM', 'SH', 'LO',
                                'PN', 'ST', 'AS'))


class XnatRepository(BaseRepository):
    """
    An 'Repository' class for XNAT repositories

    Parameters
    ----------
    server : str (URI)
        URI of XNAT server to connect to
    project_id : str
        The ID of the project on XNAT
    cache_dir : str (path)
        Path to local directory to cache XNAT data in
    user : str
        Username with which to connect to XNAT with
    password : str
        Password to connect to XNAt with
    check_md5 : bool
        Whether to check the MD5 digest of cached files before using. This
        checks for updates on the server since the file was cached
    race_cond_delay : int
        The amount of time to wait before checking that the required
        fileset has been downloaded to cache by another process has
        completed if they are attempting to download the same fileset
    """

    type = 'xnat'

    SUMMARY_NAME = 'ALL'
    PROCESSED_SUFFIX = '_PROC'
    MD5_SUFFIX = '.md5.json'
    DERIVED_FROM_FIELD = '__derived_from__'

    def __init__(self, server, project_id, cache_dir, user=None,
                 password=None, check_md5=True, race_cond_delay=30):
        super(XnatRepository, self).__init__()
        self._project_id = project_id
        self._server = server
        self._cache_dir = cache_dir
        makedirs(self._cache_dir, exist_ok=True)
        self._user = user
        self._password = password
        self._race_cond_delay = race_cond_delay
        self._check_md5 = check_md5
        self._login = None

    def __hash__(self):
        return (hash(self.server) ^
                hash(self.project_id) ^
                hash(self.cache_dir) ^
                hash(self._race_cond_delay) ^
                hash(self._check_md5))

    def __repr__(self):
        return ("{}(server={}, project_id={}, cache_dir={})"
                .format(type(self).__name__,
                        self.server, self.project_id,
                        self._cache_dir))

    def __eq__(self, other):
        try:
            return (self.server == other.server and
                    self._cache_dir == other._cache_dir and
                    self.project_id == other.project_id and
                    self.cache_dir == other.cache_dir and
                    self._race_cond_delay == other._race_cond_delay and
                    self._check_md5 == other._check_md5)
        except AttributeError:
            return False  # For comparison with other types

    @property
    def login(self):
        return self._login

    @property
    def project_id(self):
        return self._project_id

    @property
    def server(self):
        return self._server

    @property
    def cache_dir(self):
        return self._cache_dir

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
        self._login = None

    def get_fileset(self, fileset):
        """
        Caches a single fileset (if the 'path' attribute is accessed
        and it has not been previously cached for example

        Parameters
        ----------
        fileset : Fileset
            The fileset to cache
        prev_login : xnat.XNATSession
            An XNATSession object to use for the connection. A new
            one is created if one isn't provided
        """
        if fileset.repository is not self:
            raise ArcanaError(
                "{} is not from {}".format(fileset, self))
        with self:  # Connect to the XNAT repository if haven't already
            xsession = self.get_xsession(fileset)
            scan_type = fileset.name
            xfileset = xsession.scans[scan_type]
            cache_path = self._cache_path(fileset)
            # Get resource to check its MD5 digest
            xresource = self._get_resource(xfileset, fileset)
            need_to_download = True
            if op.exists(cache_path):
                if self._check_md5:
                    md5_path = (cache_path +
                                XnatRepository.MD5_SUFFIX)
                    try:
                        with open(md5_path, 'r') as f:
                            cached_digests = json.load(f)
                        digests = self._get_digests(xresource)
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
                        self._delayed_download(
                            tmp_dir, xresource, xfileset, fileset,
                            xsession.label, cache_path,
                            delay=self._race_cond_delay)
                    else:
                        raise
                else:
                    self._download_fileset(
                        tmp_dir, xresource, xfileset, fileset,
                        xsession.label, cache_path)
        return cache_path

    def get_field(self, field):
        with self:
            xsession = self.get_xsession(field)
            val_str = xsession.fields[field.name]
            if field.array:
                val = [field.dtype(v) for v in val_str.split(',')]
            else:
                val = field.dtype(val_str)
        return val

    def put_fileset(self, fileset):
        """Execute this module.
        """
        # Open XNAT session
        with self:
            # Add session for derived scans if not present
            xsession = self.get_xsession(fileset)
            cache_path = self._cache_path(fileset)
            # Make session cache dir
            if not os.path.exists(op.dirname(cache_path)):
                os.makedirs(op.dirname(cache_path),
                            stat.S_IRWXU | stat.S_IRWXG)
            digests = {}
            if op.isfile(fileset.path):
                shutil.copyfile(fileset.path, cache_path)
                self._calculate_digest(cache_path, digests)
            elif op.isdir(fileset.path):
                shutil.copytree(fileset.path, cache_path)
                for fname in os.listdir(fileset.path):
                    self._calculate_digest(op.join(fileset.path, fname),
                                           digests)
            else:
                assert False
            with open(cache_path + XnatRepository.MD5_SUFFIX, 'w',
                      **JSON_ENCODING) as f:
                json.dump(digests, f)
            # Upload to XNAT
            xfileset = self._login.classes.MrScanData(
                type=fileset.name, parent=xsession)
            # Delete existing resource
            # TODO: probably should have check to see if we want to
            #       override it
            try:
                xresource = xfileset.resources[
                    fileset.format.name.upper()]
                xresource.delete()
            except KeyError:
                pass
            xresource = xfileset.create_resource(
                fileset.format.name.upper())
            xresource.upload(cache_path, fileset.fname)

    def put_field(self, field):
        val = field.value
        if field.array:
            val = ','.join(val)
        if PY2 and isinstance(val, basestring):  # @UndefinedVariable
            val = oldstr(val)
        with self:
            xsession = self.get_xsession(field)
            xsession.fields[field.name] = val

    def tree(self, subject_ids=None, visit_ids=None, **kwargs):
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
        project : arcana.repository.Tree
            A hierarchical tree of subject, session and fileset
            information for the repository
        """
        subject_ids = self.convert_subject_ids(subject_ids)
        # Add derived visit IDs to list of visit ids to filter
        if visit_ids is not None:
            visit_ids = visit_ids
        subjects = []
        sessions = defaultdict(list)
        with self:
            xproject = self._login.projects[self.project_id]
            visit_sessions = defaultdict(list)
            # Create list of subjects
            for xsubject in xproject.subjects.values():
                subj_id = self.extract_subject_id(xsubject.label)
                if subj_id == XnatRepository.SUMMARY_NAME:
                    continue
                if (subject_ids is not None and
                        subj_id not in subject_ids):
                    continue
                logger.debug("Getting info for subject '{}'"
                             .format(subj_id))
                # Store filesets and field for every session in the
                # subject, including summary
                data = defaultdict(lambda: ([], []))
                # Get per_session filesets
                for xsession in xsubject.experiments.values():
                    try:
                        session_label = xsession.fields[
                            self.DERIVED_FROM_FIELD]
                        from_study = xsession.label[
                            len(session_label) + 1:]
                    except KeyError:
                        session_label = xsession.label
                        from_study = None
                    visit_id = self.extract_visit_id(session_label)
                    if visit_id == XnatRepository.SUMMARY_NAME:
                        visit_id = None
                        frequency = 'per_subject'
                    elif not (visit_ids is None or visit_id in visit_ids):
                        continue
                    else:
                        frequency = 'per_session'
                    # Get filesets and fields previously loaded from
                    # base "acquired" xsession or alternative derivative
                    # xsessions
                    filesets, fields = data[visit_id]
                    filesets.extend(self._get_filesets(
                        xsession,
                        frequency=frequency,
                        subject_id=subj_id,
                        visit_id=visit_id,
                        from_study=from_study))
                    fields.extend(self._get_fields(
                        xsession,
                        frequency=frequency,
                        subject_id=subj_id,
                        visit_id=visit_id,
                        from_study=from_study))
                sessions = {}
                for visit_id, (filesets, fields) in data.items():
                    if visit_id is None:
                        continue  # Hold off on the summary data
                    sessions[visit_id] = session = Session(
                        subject_id=subj_id, visit_id=visit_id,
                        filesets=filesets, fields=fields)
                    visit_sessions[visit_id].append(session)
                subj_filesets, subj_fields = data[None]
                subjects.append(Subject(subj_id,
                                        sorted(sessions.values()),
                                        filesets=subj_filesets,
                                        fields=subj_fields))
            # Create list of visits
            visits = []
            for visit_id, v_sessions in visit_sessions.items():
                # Get 'per_visit' filesets
                try:
                    xvisit_summary = xproject.experiments[
                        self._get_labels(
                            'per_visit', self.project_id,
                            visit_id=visit_id)[1]]
                except KeyError:
                    visit_filesets = []
                    visit_fields = []
                else:
                    visit_filesets = self._get_filesets(
                        xvisit_summary,
                        frequency='per_visit',
                        visit_id=visit_id,
                        from_study=from_study)
                    visit_fields = self._get_fields(
                        xvisit_summary,
                        frequency='per_visit',
                        visit_id=visit_id,
                        from_study=from_study)
                visits.append(Visit(visit_id, sorted(v_sessions),
                                    filesets=visit_filesets,
                                    fields=visit_fields))
            # Get 'per_study' filesets
            (summary_subj_name,
             summary_sess_name) = self._get_labels('per_study')
            try:
                xproj_summary = xproject.subjects[
                    summary_subj_name].experiments[
                        summary_sess_name]
            except KeyError:
                proj_filesets = []
                proj_fields = []
            else:
                proj_filesets = self._get_filesets(
                    xproj_summary,
                    frequency='per_study',
                    from_study=from_study)
                proj_fields = self._get_fields(
                    xproj_summary,
                    frequency='per_study',
                    from_study=from_study)
        return Tree(sorted(subjects), sorted(visits),
                    filesets=proj_filesets, fields=proj_fields,
                    **kwargs)

    def convert_subject_ids(self, subject_ids):
        """
        Convert subject ids to strings if they are integers
        """
        # TODO: need to make this generalisable via a
        #       splitting+mapping function passed to the repository
        if subject_ids is not None:
            subject_ids = [
                ('{:03d}'.format(s)
                 if isinstance(s, int) else s) for s in subject_ids]
        return subject_ids

    def extract_subject_id(self, xsubject_label):
        """
        This assumes that the subject ID is prepended with
        the project ID.
        """
        return xsubject_label[(len(self.project_id) + 1):]

    def extract_visit_id(self, xsession_label):
        """
        This assumes that the session ID is preprended
        """
        return '_'.join(xsession_label.split('_')[2:])

    def _get_filesets(self, xsession, **kwargs):
        """
        Returns a list of filesets within an XNAT session

        Parameters
        ----------
        xsession : xnat.classes.MrSessionData
            The XNAT session to extract the filesets from
        freq : str
            The frequency of the returned filesets (either 'per_session',
            'per_subject', 'per_visit', or 'per_study')
        derived : bool
            Whether the session is derived or not

        Returns
        -------
        filesets : list(arcana.data.Fileset)
            List of filesets within an XNAT session
        """
        filesets = []
        for xfileset in xsession.scans.values():
            try:
                file_format = self._guess_file_format(xfileset)
            except ArcanaFileFormatError as e:
                logger.warning(
                    "Ignoring '{}' as couldn't guess its file format:\n{}"
                    .format(xfileset.type, e))
            filesets.append(Fileset(
                xfileset.type, format=file_format,  # @ReservedAssignment @IgnorePep8
                id=xfileset.id, uri=xfileset.uri, repository=self,
                **kwargs))
        return sorted(filesets)

    def _get_fields(self, xsession, **kwargs):
        """
        Returns a list of fields within an XNAT session

        Parameters
        ----------
        xsession : xnat.classes.MrSessionData
            The XNAT session to extract the fields from
        freq : str
            The frequency of the returned fields (either 'per_session',
            'per_subject', 'per_visit', or 'per_study')

        Returns
        -------
        fields : list(arcana.data.Field)
            List of fields within an XNAT session
        """
        fields = []
        for name, value in list(xsession.fields.items()):
            fields.append(Field(
                name=name, value=value, repository=self,
                **kwargs))
        return sorted(fields)

    def dicom_header(self, fileset):
        def convert(val, code):  # @IgnorePep8
            if code == 'TM':
                try:
                    val = float(val)
                except ValueError:
                    pass
            elif code == 'CS':
                val = val.split('\\')
            return val
        with self:
            response = self._login.get(
                '/REST/services/dicomdump?src={}'
                .format(fileset.uri[len('/data'):]))
        hdr = {tag_parse_re.match(t['tag1']).groups():
               convert(t['value'], t['vr'])
               for t in response.json()['ResultSet']['Result']
               if (tag_parse_re.match(t['tag1']) and
                   t['vr'] in RELEVANT_DICOM_TAG_TYPES)}
        return hdr

    @classmethod
    def _get_resource(cls, xfileset, fileset):
        for resource_name in fileset.format.xnat_resource_names:
            try:
                return xfileset.resources[resource_name]
            except KeyError:
                continue
        raise ArcanaError(
            "'{}' fileset is not available in '{}' format(s), "
            "available resources are '{}'"
            .format(
                fileset.name,
                "', '".join(fileset.format.xnat_resource_names),
                "', '".join(
                    r.label for r in list(fileset.resources.values()))))

    @classmethod
    def _get_digests(cls, resource):
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
    def _download_fileset(cls, tmp_dir, xresource, xfileset, fileset,
                          session_label, cache_path):
        # Download resource to zip file
        zip_path = op.join(tmp_dir, 'download.zip')
        with open(zip_path, 'wb') as f:
            xresource.xnat_session.download_stream(
                xresource.uri + '/files', f, format='zip', verbose=True)
        digests = cls._get_digests(xresource)
        # Extract downloaded zip file
        expanded_dir = op.join(tmp_dir, 'expanded')
        try:
            with ZipFile(zip_path) as zip_file:
                zip_file.extractall(expanded_dir)
        except BadZipfile as e:
            raise ArcanaError(
                "Could not unzip file '{}' ({})"
                .format(xresource.id, e))
        data_path = op.join(
            expanded_dir, session_label, 'scans',
            (xfileset.id + '-' + special_char_re.sub('_', xfileset.type)),
            'resources', xresource.label, 'files')
        if not fileset.format.directory:
            # If the dataformat is not a directory (e.g. DICOM),
            # attempt to locate a single file within the resource
            # directory with the appropriate filename and add that
            # to be the complete data path.
            fnames = os.listdir(data_path)
            match_fnames = [
                f for f in fnames
                if (lower(split_extension(f)[-1]) ==
                    lower(fileset.format.extension))]
            if len(match_fnames) == 1:
                data_path = op.join(data_path, match_fnames[0])
            else:
                raise ArcanaMissingDataException(
                    "Did not find single file with extension '{}' "
                    "(found '{}') in resource '{}'"
                    .format(fileset.format.extension,
                            "', '".join(fnames), data_path))
        shutil.move(data_path, cache_path)
        with open(cache_path + XnatRepository.MD5_SUFFIX, 'w',
                  **JSON_ENCODING) as f:
            json.dump(digests, f)
        shutil.rmtree(tmp_dir)

    @classmethod
    def _delayed_download(cls, tmp_dir, xresource, xfileset, fileset,
                          session_label, cache_path, delay):
        logger.info("Waiting {} seconds for incomplete download of '{}' "
                    "initiated another process to finish"
                    .format(delay, cache_path))
        initial_mod_time = dir_modtime(tmp_dir)
        time.sleep(delay)
        if op.exists(cache_path):
            logger.info("The download of '{}' has completed "
                        "successfully in the other process, continuing"
                        .format(cache_path))
            return
        elif initial_mod_time != dir_modtime(tmp_dir):
            logger.info(
                "The download of '{}' hasn't completed yet, but it has"
                " been updated.  Waiting another {} seconds before "
                "checking again.".format(cache_path, delay))
            cls._delayed_download(tmp_dir, xresource, xfileset,
                                   fileset,
                                   session_label, cache_path, delay)
        else:
            logger.warning(
                "The download of '{}' hasn't updated in {} "
                "seconds, assuming that it was interrupted and "
                "restarting download".format(cache_path, delay))
            shutil.rmtree(tmp_dir)
            os.mkdir(tmp_dir)
            cls._download_fileset(
                tmp_dir, xresource, xfileset, fileset, session_label,
                cache_path)

    def get_xsession(self, item):
        """
        Returns the XNAT session and cache dir corresponding to the
        item.
        """
        subj_label, sess_label = self._get_item_labels(item)
        with self:
            xproject = self._login.projects[self.project_id]
            try:
                xsubject = xproject.subjects[subj_label]
            except KeyError:
                xsubject = self._login.classes.SubjectData(
                    label=subj_label, parent=xproject)
            try:
                xsession = xsubject.experiments[sess_label]
            except KeyError:
                xsession = self._login.classes.MrSessionData(
                    label=sess_label, parent=xsubject)
                if item.derived:
                    xsession.fields[
                        self.DERIVED_FROM_FIELD] = self._get_item_labels(
                            item, no_from_study=True)[1]
        return xsession

    def _get_item_labels(self, item, no_from_study=False):
        """
        Returns the labels for the XNAT subject and sessions given
        the frequency and provided IDs.
        """
        subj_label, sess_label = self._get_labels(
            item.frequency, item.subject_id, item.visit_id)
        if not no_from_study and item.from_study is not None:
            sess_label += '_' + item.from_study
        return (subj_label, sess_label)

    def _get_labels(self, frequency, subject_id=None, visit_id=None):
        """
        Returns the labels for the XNAT subject and sessions given
        the frequency and provided IDs.
        """
        if frequency == 'per_session':
            subj_label = '{}_{}'.format(self.project_id,
                                        subject_id)
            sess_label = '{}_{}_{}'.format(self.project_id,
                                           subject_id,
                                           visit_id)
        elif frequency == 'per_subject':
            subj_label = '{}_{}'.format(self.project_id,
                                        subject_id)
            sess_label = '{}_{}_{}'.format(self.project_id,
                                           subject_id,
                                           self.SUMMARY_NAME)
        elif frequency == 'per_visit':
            subj_label = '{}_{}'.format(self.project_id,
                                        self.SUMMARY_NAME)
            sess_label = '{}_{}_{}'.format(self.project_id,
                                           self.SUMMARY_NAME,
                                           visit_id)
        elif frequency == 'per_study':
            subj_label = '{}_{}'.format(self.project_id,
                                        self.SUMMARY_NAME)
            sess_label = '{}_{}_{}'.format(self.project_id,
                                           self.SUMMARY_NAME,
                                           self.SUMMARY_NAME)
        else:
            assert False
        return (subj_label, sess_label)

    def _cache_path(self, fileset):
        subj_dir, sess_dir = self._get_item_labels(fileset)
        cache_dir = op.join(self._cache_dir, self.project_id,
                            subj_dir, sess_dir)
        makedirs(cache_dir, exist_ok=True)
        return op.join(cache_dir, fileset.fname)

    @classmethod
    def _guess_file_format(cls, xfileset):
        # Use a set here as in some cases there are multiple resources
        # the same format (e.g. DICOM + secondary)
        fileset_formats = set()
        for xresource in xfileset.resources.values():
            try:
                fileset_formats.add(FileFormat.by_names[
                    xresource.label.lower()])
            except KeyError:
                logger.debug("Ignoring resource '{}' in fileset {}"
                             .format(xresource.label, xfileset.type))
        if not fileset_formats:
            raise ArcanaFileFormatError(
                "No recognised data formats for '{}' fileset (available "
                "resources are '{}')".format(
                    xfileset.type, "', '".join(
                        r.label for r in xfileset.resources.values())))
        elif len(fileset_formats) > 1:
            raise ArcanaFileFormatError(
                "Multiple valid data-formats '{}' for '{}' fileset, please "
                "pass 'file_format' to 'download_fileset' method to speficy"
                " resource to download".format(
                    "', '".join(f.label for f in fileset_formats),
                    xfileset.type))
        return next(iter(fileset_formats))

    @classmethod
    def _calculate_digest(cls, path, digests):
        with open(path, 'rb') as f:
            digests[op.basename(path)] = hashlib.md5(
                f.read()).hexdigest()
