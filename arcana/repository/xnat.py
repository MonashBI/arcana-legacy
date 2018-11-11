from __future__ import absolute_import
from past.builtins import basestring
import os
import tempfile
from arcana.utils import makedirs
import os.path as op # @IgnorePep8
import shutil
from arcana.utils import JSON_ENCODING
import stat
import time
import logging
import errno
import json
from zipfile import ZipFile, BadZipfile
from arcana.data import Fileset, Field
from arcana.repository.base import BaseRepository
from arcana.data.file_format import FileFormat
from arcana.exceptions import (
    ArcanaError, ArcanaFileFormatError, ArcanaWrongRepositoryError)
from arcana.provenance import Record
from arcana.utils import dir_modtime
import re
import xnat

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
    MD5_SUFFIX = '.__md5__.json'
    DERIVED_FROM_FIELD = '__derived_from__'
    PROV_SCAN = '__prov__'

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
        self._check_repository(fileset)
        with self:  # Connect to the XNAT repository if haven't already
            xsession = self.get_xsession(fileset)
            scan_type = fileset.name
            xscan = xsession.scans[scan_type]
            cache_path = self._cache_path(fileset)
            need_to_download = True
            if op.exists(cache_path):
                if self._check_md5:
                    md5_path = cache_path + XnatRepository.MD5_SUFFIX
                    try:
                        with open(md5_path, 'r') as f:
                            cached_checksums = json.load(f)
                        if cached_checksums == fileset.checksums:
                            need_to_download = False
                    except IOError:
                        pass
                else:
                    need_to_download = False
            if need_to_download:
                xresource = self.get_resource(xscan, fileset.format)
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
                            tmp_dir, xresource, xscan, fileset,
                            xsession.label, cache_path,
                            delay=self._race_cond_delay)
                    else:
                        raise
                else:
                    self.download_fileset(
                        tmp_dir, xresource, xscan, fileset,
                        xsession.label, cache_path)
                    shutil.rmtree(tmp_dir)
        return cache_path

    def get_field(self, field):
        self._check_repository(field)
        with self:
            xsession = self.get_xsession(field)
            val_str = xsession.fields[field.name]
            if field.array:
                val = [field.dtype(v) for v in val_str.split(',')]
            else:
                val = field.dtype(val_str)
        return val

    def put_fileset(self, fileset):
        self._check_repository(fileset)
        # Open XNAT session
        with self:
            # Add session for derived scans if not present
            xsession = self.get_xsession(fileset)
            cache_path = self._cache_path(fileset)
            # Make session cache dir
            if not os.path.exists(op.dirname(cache_path)):
                os.makedirs(op.dirname(cache_path),
                            stat.S_IRWXU | stat.S_IRWXG)
            if fileset.format.directory:
                shutil.copytree(fileset.path, cache_path)
            else:
                shutil.copyfile(fileset.path, cache_path)
            with open(cache_path + XnatRepository.MD5_SUFFIX, 'w',
                      **JSON_ENCODING) as f:
                json.dump(fileset.checksums, f)
            # Upload to XNAT
            xscan = self._login.classes.MrScanData(
                type=fileset.name, parent=xsession)
            # Delete existing resource
            # TODO: probably should have check to see if we want to
            #       override it
            try:
                xresource = xscan.resources[
                    fileset.format.name.upper()]
            except KeyError:
                pass
            else:
                xresource.delete()
            xresource = xscan.create_resource(
                fileset.format.name.upper())
            xresource.upload(cache_path, fileset.fname)

    def put_field(self, field):
        self._check_repository(field)
        val = field.value
        if field.array:
            val = ','.join(val)
        if isinstance(val, basestring):
            val = str(val)
        with self:
            xsession = self.get_xsession(field)
            xsession.fields[field.name] = val

    def put_provenance(self, record):
        base_cache_path = self._cache_path(record, name=self.PROV_SCAN)
        if not op.exists(base_cache_path):
            os.mkdir(base_cache_path)
        else:
            if not op.isdir(base_cache_path):
                raise ArcanaError(
                    "Base provenance cache path ('{}') should be a directory"
                    .format(base_cache_path))
        cache_path = op.join(base_cache_path, record.pipeline_name + '.json')
        record.save(cache_path)
        # TODO: Should also save digest of prov.json to check to see if it
        #       has been altered remotely
        xsession = self.get_xsession(record)
        xprov = self._login.classes.MrScanData(
            type=self.PROV_SCAN, parent=xsession)
        # Delete existing provenance if present
        try:
            xresource = xprov.resources[record.pipeline_name]
        except KeyError:
            pass
        else:
            xresource.delete()
        xresource = xprov.create_resource(record.pipeline_name)
        xresource.upload(cache_path, op.basename(cache_path))

    def find_data(self, subject_ids=None, visit_ids=None, **kwargs):
        """
        Find all data within a repository, registering filesets, fields and
        provenance with the found_fileset, found_field and found_provenance
        methods, respectively

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
        filesets : list[Fileset]
            All the filesets found in the repository
        fields : list[Field]
            All the fields found in the repository
        records : list[Record]
            The provenance records found in the repository
        """
        subject_ids = self.convert_subject_ids(subject_ids)
        # Add derived visit IDs to list of visit ids to filter
        all_filesets = []
        all_fields = []
        all_records = []
        with self:
            xproject = self._login.projects[self.project_id]
            # Create list of subjects
            for xsubject in xproject.subjects.values():
                subj_id = self.extract_subject_id(xsubject.label)
                if subj_id == XnatRepository.SUMMARY_NAME:
                    subj_id = None
                elif (subject_ids is not None and subj_id not in subject_ids):
                    continue
                logger.debug("Getting info for subject '{}'".format(subj_id))
                # Store filesets and field for every session in the
                # subject, including summary
                # Get per_session filesets
                for xsession in xsubject.experiments.values():
                    try:
                        session_label = xsession.fields[
                            self.DERIVED_FROM_FIELD]
                        from_study = xsession.label[len(session_label) + 1:]
                    except KeyError:
                        session_label = xsession.label
                        from_study = None
                    visit_id = self.extract_visit_id(session_label)
                    if visit_id == XnatRepository.SUMMARY_NAME:
                        visit_id = None
                    elif not (visit_ids is None or visit_id in visit_ids):
                        continue
                    # Determine frequency
                    if (subj_id, visit_id) == (None, None):
                        frequency = 'per_study'
                    elif visit_id is None:
                        frequency = 'per_subject'
                    elif subj_id is None:
                        frequency = 'per_visit'
                    else:
                        frequency = 'per_session'
                    # Find filesets
                    for xscan in xsession.scans.values():
                        if xscan.type == self.PROV_SCAN:
                            # Download provenance JSON files and parse into
                            # records
                            temp_dir = tempfile.mkdtemp()
                            try:
                                xscan.download_dir(op.join(temp_dir, 'PROV'))
                                resources_dir = op.join(
                                    temp_dir, 'PROV', xsession.label, 'scans',
                                    xscan.id + '-' + xscan.type, 'resources')
                                for pipeline_name in os.listdir(resources_dir):
                                    json_path = op.join(
                                        resources_dir, pipeline_name, 'files',
                                        pipeline_name + '.json')
                                    all_records.append(
                                        Record.load(json_path,
                                                    frequency, subj_id,
                                                    visit_id, from_study))
                            finally:
                                shutil.rmtree(temp_dir)
                        else:
                            try:
                                file_format = self._guess_file_format(xscan)
                            except ArcanaFileFormatError as e:
                                logger.warning(
                                    "Ignoring '{}' as couldn't guess its file "
                                    "format:\n{}".format(xscan.type, e))
                            all_filesets.append(Fileset(
                                xscan.type, format=file_format, id=xscan.id,
                                uri=xscan.uri, repository=self,
                                frequency=frequency, subject_id=subj_id,
                                visit_id=visit_id, from_study=from_study,
                                checksums=self.get_checksums(self.get_resource(
                                    xscan, file_format), file_format),
                                **kwargs))
                    # Find fields
                    for name, value in list(xsession.fields.items()):
                        all_fields.append(Field(
                            name=name, value=value, repository=self,
                            frequency=frequency,
                            subject_id=subj_id,
                            visit_id=visit_id,
                            from_study=from_study,
                            **kwargs))
                    # Find records
        return all_filesets, all_fields, all_records

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
    def get_resource(cls, xscan, file_format):
        for resource_name in file_format.xnat_resource_names:
            try:
                return xscan.resources[resource_name]
            except KeyError:
                continue
        raise ArcanaError(
            "Could not find matching resource for {} ('{}') in {}, "
            "available resources are '{}'"
            .format(
                file_format,
                "', '".join(file_format.xnat_resource_names),
                xscan.uri,
                "', '".join(
                    r.label for r in list(xscan.resources.values()))))

    @classmethod
    def get_checksums(cls, resource, file_format):
        """
        Downloads the MD5 digests associated with the files in a resource.
        These are saved with the downloaded files in the cache and used to
        check if the files have been updated on the server

        Parameters
        ----------
        resource : xnat.ResourceCatalog
            The xnat resource
        file_format : FileFormat
            The format of the fileset to get the checksums for. Used to
            determine the primary file within the resource and change the
            corresponding key in the checksums dictionary to '.' to match
            the way it is generated locally by Arcana.
        """
        result = resource.xnat_session.get(resource.uri + '/files')
        if result.status_code != 200:
            raise ArcanaError(
                "Could not download metadata for resource {}"
                .format(resource.id))
        checksums = dict((r['Name'], r['digest'])
                         for r in result.json()['ResultSet']['Result'])
        if not file_format.directory:
            # Replace the key corresponding to the primary file with '.' to
            # match the way that checksums are created by Arcana
            primary = file_format.primary_file(checksums.keys())
            checksums['.'] = checksums.pop(primary)
        return checksums

    @classmethod
    def download_fileset(cls, tmp_dir, xresource, xscan, fileset,
                          session_label, cache_path):
        # Download resource to zip file
        zip_path = op.join(tmp_dir, 'download.zip')
        with open(zip_path, 'wb') as f:
            xresource.xnat_session.download_stream(
                xresource.uri + '/files', f, format='zip', verbose=True)
        checksums = cls.get_checksums(xresource, fileset.format)
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
            (xscan.id + '-' + special_char_re.sub('_', xscan.type)),
            'resources', xresource.label, 'files')
        if not fileset.format.directory:
            # If the dataformat is not a directory (e.g. DICOM),
            # attempt to locate a single file within the resource
            # directory with the appropriate filename and add that
            # to be the complete data path.
            data_path = op.join(
                data_path, fileset.format.primary_file(os.listdir(data_path)))

        shutil.move(data_path, cache_path)
        with open(cache_path + XnatRepository.MD5_SUFFIX, 'w',
                  **JSON_ENCODING) as f:
            json.dump(checksums, f)

    @classmethod
    def _delayed_download(cls, tmp_dir, xresource, xscan, fileset,
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
            cls._delayed_download(tmp_dir, xresource, xscan,
                                   fileset,
                                   session_label, cache_path, delay)
        else:
            logger.warning(
                "The download of '{}' hasn't updated in {} "
                "seconds, assuming that it was interrupted and "
                "restarting download".format(cache_path, delay))
            shutil.rmtree(tmp_dir)
            os.mkdir(tmp_dir)
            cls.download_fileset(
                tmp_dir, xresource, xscan, fileset, session_label,
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

    def _cache_path(self, fileset, name=None):
        subj_dir, sess_dir = self._get_item_labels(fileset)
        cache_dir = op.join(self._cache_dir, self.project_id,
                            subj_dir, sess_dir)
        makedirs(cache_dir, exist_ok=True)
        return op.join(cache_dir, fileset.fname if name is None else name)

    @classmethod
    def _guess_file_format(cls, xscan):
        # Use a set here as in some cases there are multiple resources
        # the same format (e.g. DICOM + secondary)
        fileset_formats = set()
        for xresource in xscan.resources.values():
            try:
                fileset_formats.add(FileFormat.by_names[
                    xresource.label.lower()])
            except KeyError:
                logger.debug("Ignoring resource '{}' in fileset {}"
                             .format(xresource.label, xscan.type))
        if not fileset_formats:
            raise ArcanaFileFormatError(
                "No recognised data formats for '{}' fileset (available "
                "resources are '{}')".format(
                    xscan.type, "', '".join(
                        r.label for r in xscan.resources.values())))
        elif len(fileset_formats) > 1:
            raise ArcanaFileFormatError(
                "Multiple valid data-formats '{}' for '{}' fileset, please "
                "pass 'file_format' to 'download_fileset' method to speficy"
                " resource to download".format(
                    "', '".join(f.label for f in fileset_formats),
                    xscan.type))
        return next(iter(fileset_formats))

    def _check_repository(self, item):
        if item.repository is not self:
            raise ArcanaWrongRepositoryError(
                "{} is from {} instead of {}".format(item, item.repository,
                                                     self))
