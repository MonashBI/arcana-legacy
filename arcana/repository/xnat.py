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
from arcana.repository.base import Repository
from arcana.exceptions import (
    ArcanaError, ArcanaUsageError, ArcanaFileFormatError,
    ArcanaWrongRepositoryError)
from arcana.pipeline.provenance import Record
from arcana.utils import dir_modtime, get_class_info, parse_value
import re
import xnat

logger = logging.getLogger('arcana')

special_char_re = re.compile(r'[^a-zA-Z_0-9]')
tag_parse_re = re.compile(r'\((\d+),(\d+)\)')

RELEVANT_DICOM_TAG_TYPES = set(('UI', 'CS', 'DA', 'TM', 'SH', 'LO',
                                'PN', 'ST', 'AS'))


class XnatRepo(Repository):
    """
    An 'Repository' class for XNAT repositories

    Parameters
    ----------
    server : str (URI)
        URI of XNAT server to connect to
    project_id : str
        The ID of the project in the XNAT repository
    cache_dir : str (path)
        Path to local directory to cache remote data in
    user : str
        Username with which to connect to XNAT with
    password : str
        Password to connect to the XNAT repository with
    check_md5 : bool
        Whether to check the MD5 digest of cached files before using. This
        checks for updates on the server since the file was cached
    race_cond_delay : int
        The amount of time to wait before checking that the required
        fileset has been downloaded to cache by another process has
        completed if they are attempting to download the same fileset
    session_filter : str
        A regular expression that is used to prefilter the discovered sessions
        to avoid having to retrieve metadata for them, and potentially speeding
        up the initialisation of the Study. Note that if the processing relies
        on summary derivatives (i.e. of 'per_visit/subject/study' frequency)
        then the filter should match all sessions in the Study's subject_ids
        and visit_ids.
    """

    type = 'xnat'

    SUMMARY_NAME = 'ALL'
    MD5_SUFFIX = '.__md5__.json'
    DERIVED_FROM_FIELD = '__derived_from__'
    PROV_SCAN = '__prov__'
    PROV_RESOURCE = 'PROV'

    def __init__(self, server, project_id, cache_dir, user=None,
                 password=None, check_md5=True, race_cond_delay=30,
                 session_filter=None, **kwargs):
        super(XnatRepo, self).__init__(**kwargs)
        if not isinstance(server, basestring):
            raise ArcanaUsageError(
                "Invalid server url {}".format(server))
        self._project_id = project_id
        self._server = server
        self._cache_dir = cache_dir
        makedirs(self._cache_dir, exist_ok=True)
        self._user = user
        self._password = password
        self._race_cond_delay = race_cond_delay
        self._check_md5 = check_md5
        self._session_filter = session_filter
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
    def prov(self):
        return {
            'type': get_class_info(type(self)),
            'server': self.server,
            'project': self.project_id}

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

    @property
    def check_md5(self):
        return self._check_md5

    @property
    def session_filter(self):
        return (re.compile(self._session_filter)
                if self._session_filter is not None else None)

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

        Returns
        -------
        primary_path : str
            The path of the primary file once it has been cached
        aux_paths : dict[str, str]
            A dictionary containing a mapping of auxiliary file names to
            paths
        """
        if fileset.format is None:
            raise ArcanaUsageError(
                "Attempting to download {}, which has not been assigned a "
                "file format (see Fileset.formatted)".format(fileset))
        self._check_repository(fileset)
        with self:  # Connect to the XNAT repository if haven't already
            xsession = self.get_xsession(fileset)
            xscan = xsession.scans[fileset.name]
            # Set URI so we can retrieve checksums if required
            fileset.uri = xscan.uri
            fileset.id = xscan.id
            cache_path = self._cache_path(fileset)
            need_to_download = True
            if op.exists(cache_path):
                if self._check_md5:
                    md5_path = cache_path + XnatRepo.MD5_SUFFIX
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
                xresource = xscan.resources[fileset._resource_name]
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
        if not fileset.format.directory:
            (primary_path, aux_paths) = fileset.format.assort_files(
                op.join(cache_path, f) for f in os.listdir(cache_path))
        else:
            primary_path = cache_path
            aux_paths = None
        return primary_path, aux_paths

    def get_field(self, field):
        self._check_repository(field)
        with self:
            xsession = self.get_xsession(field)
            val = xsession.fields[field.name]
            val = val.replace('&quot;', '"')
            val = parse_value(val)
        return val

    def put_fileset(self, fileset):
        if fileset.format is None:
            raise ArcanaFileFormatError(
                "Format of {} needs to be set before it is uploaded to {}"
                .format(fileset, self))
        self._check_repository(fileset)
        # Open XNAT session
        with self:
            # Add session for derived scans if not present
            xsession = self.get_xsession(fileset)
            cache_path = self._cache_path(fileset)
            # Make session cache dir
            cache_path_dir = (op.dirname(cache_path)
                              if fileset.format.directory else cache_path)
            if os.path.exists(cache_path_dir):
                shutil.rmtree(cache_path_dir)
            os.makedirs(cache_path_dir, stat.S_IRWXU | stat.S_IRWXG)
            if fileset.format.directory:
                shutil.copytree(fileset.path, cache_path)
            else:
                # Copy primary file
                shutil.copyfile(fileset.path,
                                op.join(cache_path, fileset.fname))
                # Copy auxiliaries
                for sc_fname, sc_path in fileset.aux_file_fnames_and_paths:
                    shutil.copyfile(sc_path, op.join(cache_path, sc_fname))
            with open(cache_path + XnatRepo.MD5_SUFFIX, 'w',
                      **JSON_ENCODING) as f:
                json.dump(fileset.calculate_checksums(), f, indent=2)
            # Upload to XNAT
            xscan = self._login.classes.MrScanData(
                id=fileset.id, type=fileset.basename, parent=xsession)
            fileset.uri = xscan.uri
            # Select the first xnat_resource name to use to upload the data to
            resource_name = fileset.format.resource_names(self.type)[0]
            try:
                xresource = xscan.resources[resource_name]
            except KeyError:
                pass
            else:
                # Delete existing resource
                # TODO: probably should have check to see if we want to
                #       override it
                xresource.delete()
            xresource = xscan.create_resource(resource_name)
            if fileset.format.directory:
                for fname in os.listdir(fileset.path):
                    xresource.upload(op.join(fileset.path, fname), fname)
            else:
                xresource.upload(fileset.path, fileset.fname)
                for sc_fname, sc_path in fileset.aux_file_fnames_and_paths:
                    xresource.upload(sc_path, sc_fname)

    def put_field(self, field):
        self._check_repository(field)
        val = field.value
        if field.array:
            if field.dtype is str:
                val = ['"{}"'.format(v) for v in val]
            val = '[' + ','.join(str(v) for v in val) + ']'
        if field.dtype is str:
            val = '"{}"'.format(val)
        with self:
            xsession = self.get_xsession(field)
            xsession.fields[field.name] = val

    def put_record(self, record):
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
            id=self.PROV_SCAN, type=self.PROV_SCAN, parent=xsession)
        # Delete existing provenance if present
        try:
            xresource = xprov.resources[record.pipeline_name]
        except KeyError:
            pass
        else:
            xresource.delete()
        # FIXME: should reuse the same resource for all provenance jsons
        xresource = xprov.create_resource(record.pipeline_name)
        xresource.upload(cache_path, op.basename(cache_path))

    def get_checksums(self, fileset):
        """
        Downloads the MD5 digests associated with the files in the file-set.
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
        if fileset.uri is None:
            raise ArcanaUsageError(
                "Can't retrieve checksums as URI has not been set for {}"
                .format(fileset))
        with self:
            checksums = {r['Name']: r['digest']
                         for r in self._login.get_json(fileset.uri + '/files')[
                             'ResultSet']['Result']}
        if not fileset.format.directory:
            # Replace the key corresponding to the primary file with '.' to
            # match the way that checksums are created by Arcana
            primary = fileset.format.assort_files(checksums.keys())[0]
            checksums['.'] = checksums.pop(primary)
        return checksums

    def find_data(self, subject_ids=None, visit_ids=None, **kwargs):
        """
        Find all filesets, fields and provenance records within an XNAT project

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
        # Note we prefer the use of raw REST API calls here for performance
        # reasons over using XnatPy's data structures.
        with self:
            # Get map of internal subject IDs to subject labels in project
            subject_xids_to_labels = {
                s['ID']: s['label'] for s in self._login.get_json(
                    '/data/projects/{}/subjects'.format(self.project_id))[
                        'ResultSet']['Result']}
            # Get list of all sessions within project
            session_xids = [
                s['ID'] for s in self._login.get_json(
                    '/data/projects/{}/experiments'.format(self.project_id))[
                        'ResultSet']['Result']
                if (self.session_filter is None or
                    self.session_filter.match(s['label']))]
            for session_xid in session_xids:
                session_json = self._login.get_json(
                    '/data/projects/{}/experiments/{}'.format(
                        self.project_id, session_xid))['items'][0]
                subject_xid = session_json['data_fields']['subject_ID']
                subject_id = subject_xids_to_labels[subject_xid]
                session_label = session_json['data_fields']['label']
                session_uri = (
                    '/data/archive/projects/{}/subjects/{}/experiments/{}'
                    .format(self.project_id, subject_xid, session_xid))
                # Get field values. We do this first so we can check for the
                # DERIVED_FROM_FIELD to determine the correct session label and
                # study name
                field_values = {}
                try:
                    fields_json = next(
                        c['items'] for c in session_json['children']
                        if c['field'] == 'fields/field')
                except StopIteration:
                    pass
                else:
                    for js in fields_json:
                        try:
                            value = js['data_fields']['field']
                        except KeyError:
                            pass
                        else:
                            field_values[js['data_fields']['name']] = value
                # Extract study name and derived-from session
                if self.DERIVED_FROM_FIELD in field_values:
                    df_sess_label = field_values.pop(self.DERIVED_FROM_FIELD)
                    from_study = session_label[len(df_sess_label) + 1:]
                    session_label = df_sess_label
                else:
                    from_study = None
                # Strip subject ID from session label if required
                if session_label.startswith(subject_id + '_'):
                    visit_id = session_label[len(subject_id) + 1:]
                else:
                    visit_id = session_label
                # Strip project ID from subject ID if required
                if subject_id.startswith(self.project_id + '_'):
                    subject_id = subject_id[len(self.project_id) + 1:]
                # Check subject is summary or not and whether it is to be
                # filtered
                if subject_id == XnatRepo.SUMMARY_NAME:
                    subject_id = None
                elif not (subject_ids is None or subject_id in subject_ids):
                    continue
                # Check visit is summary or not and whether it is to be
                # filtered
                if visit_id == XnatRepo.SUMMARY_NAME:
                    visit_id = None
                elif not (visit_ids is None or visit_id in visit_ids):
                    continue
                # Determine frequency
                if (subject_id, visit_id) == (None, None):
                    frequency = 'per_study'
                elif visit_id is None:
                    frequency = 'per_subject'
                elif subject_id is None:
                    frequency = 'per_visit'
                else:
                    frequency = 'per_session'
                # Append fields
                for name, value in field_values.items():
                    value = value.replace('&quot;', '"')
                    all_fields.append(Field(
                        name=name, value=value, repository=self,
                        frequency=frequency,
                        subject_id=subject_id,
                        visit_id=visit_id,
                        from_study=from_study,
                        **kwargs))
                # Extract part of JSON relating to files
                try:
                    scans_json = next(
                        c['items'] for c in session_json['children']
                        if c['field'] == 'scans/scan')
                except StopIteration:
                    scans_json = []
                for scan_json in scans_json:
                    scan_id = scan_json['data_fields']['ID']
                    scan_type = scan_json['data_fields'].get('type', '')
                    scan_quality = scan_json['data_fields'].get('quality',
                                                                None)
                    scan_uri = '{}/scans/{}'.format(session_uri, scan_id)
                    try:
                        resources_json = next(
                            c['items'] for c in scan_json['children']
                            if c['field'] == 'file')
                    except StopIteration:
                        resources = {}
                    else:
                        resources = {js['data_fields']['label']:
                                     js['data_fields'].get('format', None)
                                     for js in resources_json}
                    # Remove auto-generated snapshots directory
                    resources.pop('SNAPSHOTS', None)
                    if scan_type == self.PROV_SCAN:
                        # Download provenance JSON files and parse into
                        # records
                        temp_dir = tempfile.mkdtemp()
                        try:
                            with tempfile.TemporaryFile() as temp_zip:
                                self._login.download_stream(
                                    scan_uri + '/files', temp_zip,
                                    format='zip')
                                with ZipFile(temp_zip) as zip_file:
                                    zip_file.extractall(temp_dir)
                            for base_dir, _, fnames in os.walk(temp_dir):
                                for fname in fnames:
                                    if fname.endswith('.json'):
                                        pipeline_name = fname[:-len('.json')]
                                        json_path = op.join(base_dir, fname)
                                        all_records.append(
                                            Record.load(
                                                pipeline_name, frequency,
                                                subject_id, visit_id,
                                                from_study, json_path))
                        finally:
                            shutil.rmtree(temp_dir, ignore_errors=True)
                    else:
                        for resource in resources:
                            all_filesets.append(Fileset(
                                scan_type, id=scan_id, uri=scan_uri,
                                repository=self, frequency=frequency,
                                subject_id=subject_id, visit_id=visit_id,
                                from_study=from_study, quality=scan_quality,
                                resource_name=resource, **kwargs))
                logger.debug("Found node {}:{} on {}:{}".format(
                    subject_id, visit_id, self.server, self.project_id))
        return all_filesets, all_fields, all_records

    def convert_subject_ids(self, subject_ids):
        """
        Convert subject ids to strings if they are integers
        """
        # TODO: need to make this generalisable via a
        #       splitting+mapping function passed to the repository
        if subject_ids is not None:
            subject_ids = set(
                ('{:03d}'.format(s)
                 if isinstance(s, int) else s) for s in subject_ids)
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
                '/REST/services/dicomdump?src=' +
                fileset.uri[len('/data'):]).json()['ResultSet']['Result']
        hdr = {tag_parse_re.match(t['tag1']).groups(): convert(t['value'],
                                                               t['vr'])
               for t in response if (tag_parse_re.match(t['tag1']) and
                                     t['vr'] in RELEVANT_DICOM_TAG_TYPES)}
        return hdr

    def download_fileset(self, tmp_dir, xresource, xscan, fileset,
                          session_label, cache_path):
        # Download resource to zip file
        zip_path = op.join(tmp_dir, 'download.zip')
        with open(zip_path, 'wb') as f:
            xresource.xnat_session.download_stream(
                xresource.uri + '/files', f, format='zip', verbose=True)
        checksums = self.get_checksums(fileset)
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
        # Remove existing cache if present
        try:
            shutil.rmtree(cache_path)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise e
        shutil.move(data_path, cache_path)
        with open(cache_path + XnatRepo.MD5_SUFFIX, 'w',
                  **JSON_ENCODING) as f:
            json.dump(checksums, f, indent=2)

    def _delayed_download(self, tmp_dir, xresource, xscan, fileset,
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
            self._delayed_download(tmp_dir, xresource, xscan,
                                   fileset,
                                   session_label, cache_path, delay)
        else:
            logger.warning(
                "The download of '{}' hasn't updated in {} "
                "seconds, assuming that it was interrupted and "
                "restarting download".format(cache_path, delay))
            shutil.rmtree(tmp_dir)
            os.mkdir(tmp_dir)
            self.download_fileset(
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
        subject_id = self.inv_map_subject_id(item.subject_id)
        visit_id = self.inv_map_visit_id(item.visit_id)
        subj_label, sess_label = self._get_labels(
            item.frequency, subject_id, visit_id)
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
        return op.join(cache_dir, fileset.name if name is None else name)

    def _check_repository(self, item):
        if item.repository is not self:
            raise ArcanaWrongRepositoryError(
                "{} is from {} instead of {}".format(item, item.repository,
                                                     self))
