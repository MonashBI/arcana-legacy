from __future__ import absolute_import
from builtins import range
from builtins import object
import string
import random
import os
import os.path as op
import shutil
import re
import xnat
from arcana.testing import BaseTestCase
from arcana.repository.xnat import XnatRepository
from arcana.repository.directory import DirectoryRepository
from arcana.study import Study, StudyMetaClass
from arcana.data import AcquiredFilesetSpec, FilesetSpec, FieldSpec
from arcana.exception import ArcanaError
from arcana.data.file_format.standard import text_format
import sys
import logging
from future.utils import with_metaclass

# Import unittests as package so we can inherit from unittests in other modules
sys.path.insert(0, op.join(op.dirname(__file__), '..', '..', '..'))
import unittest as arcana_unittests  # @IgnorePep8 @NoMove @UnusedImport
sys.path.pop(0)


logger = logging.getLogger('arcana')

try:
    SERVER = os.environ['ARCANA_TEST_XNAT']
except KeyError:
    SERVER = None

SKIP_ARGS = (SERVER is None,
             "Skipping as ARCANA_TEST_XNAT env var not set")


class DummyStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        AcquiredFilesetSpec('source1', text_format),
        AcquiredFilesetSpec('source2', text_format, optional=True),
        AcquiredFilesetSpec('source3', text_format, optional=True),
        AcquiredFilesetSpec('source4', text_format, optional=True),
        FilesetSpec('sink1', text_format, 'dummy_pipeline'),
        FilesetSpec('sink3', text_format, 'dummy_pipeline'),
        FilesetSpec('sink4', text_format, 'dummy_pipeline'),
        FilesetSpec('subject_sink', text_format, 'dummy_pipeline',
                    frequency='per_subject'),
        FilesetSpec('visit_sink', text_format, 'dummy_pipeline',
                    frequency='per_visit'),
        FilesetSpec('study_sink', text_format, 'dummy_pipeline',
                    frequency='per_study'),
        FilesetSpec('resink1', text_format, 'dummy_pipeline'),
        FilesetSpec('resink2', text_format, 'dummy_pipeline'),
        FilesetSpec('resink3', text_format, 'dummy_pipeline'),
        FieldSpec('field1', int, 'dummy_pipeline'),
        FieldSpec('field2', float, 'dummy_pipeline'),
        FieldSpec('field3', str, 'dummy_pipeline')]

    def dummy_pipeline(self, **name_maps):
        return self.pipeline('dummy_pipeline', name_maps=name_maps)


class TestStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        AcquiredFilesetSpec('fileset1', text_format),
        AcquiredFilesetSpec('fileset2', text_format, optional=True),
        AcquiredFilesetSpec('fileset3', text_format),
        AcquiredFilesetSpec('fileset5', text_format, optional=True)]


class CreateXnatProjectMixin(object):

    PROJECT_NAME_LEN = 12

    @property
    def project(self):
        """
        Creates a random string of letters and numbers to be the
        project ID
        """
        try:
            return self._project
        except AttributeError:
            self._project = ''.join(
                random.choice(string.ascii_uppercase + string.digits)
                for _ in range(self.PROJECT_NAME_LEN))
            return self._project

    def _create_project(self, project_name=None):
        if project_name is None:
            project_name = self.project
        if SERVER == 'https://mbi-xnat.erc.monash.edu.au':
            raise ArcanaError(
                "Shouldn't be creating projects on the production "
                "server")
        with xnat.connect(SERVER) as login:
            uri = '/data/archive/projects/{}'.format(project_name)
            query = {'xsiType': 'xnat:projectData', 'req_format': 'qa'}
            response = login.put(uri, query=query)
            if response.ok:
                logger.info("Created test project '{}'"
                            .format(project_name))

    def _delete_project(self, project_name=None):
        if project_name is None:
            project_name = self.project
        with xnat.connect(SERVER) as login:
            login.projects[project_name].delete()


class TestOnXnatMixin(CreateXnatProjectMixin):

    def session_label(self, project=None, subject=None, visit=None,
                      from_study=None):
        if project is None:
            project = self.project
        if subject is None:
            subject = self.SUBJECT
        if visit is None:
            visit = self.VISIT
        label = '_'.join((project, subject, visit))
        if from_study is not None:
            label += '_' + from_study
        return label

    def session_cache(self, base_dir=None, project=None, subject=None,
                      visit=None, from_study=None):
        if base_dir is None:
            base_dir = self.cache_dir
        if project is None:
            project = self.project
        if subject is None:
            subject = self.SUBJECT
        return op.join(
            base_dir, project, '{}_{}'.format(project, subject),
            self.session_label(project=project, subject=subject,
                               visit=visit, from_study=from_study))

    def setUp(self):
        BaseTestCase.setUp(self)
        shutil.rmtree(self.cache_dir, ignore_errors=True)
        os.makedirs(self.cache_dir)
        self._create_project()
        with self._connect() as login:
            xproject = login.projects[self.project]
            xsubject = login.classes.SubjectData(
                label='{}_{}'.format(self.project, self.SUBJECT),
                parent=xproject)
            xsession = login.classes.MrSessionData(
                label=self.session_label(),
                parent=xsubject)
            for fileset in self.session.filesets:
                # Create fileset with an integer ID instead of the
                # label
                query = {'xsiType': 'xnat:mrScanData',
                         'req_format': 'qa',
                         'type': fileset.name}
                uri = '{}/scans/{}'.format(xsession.fulluri, fileset.id)
                login.put(uri, query=query)
                # Get XnatPy object for newly created fileset
                xfileset = login.classes.MrScanData(uri,
                                                    xnat_session=login)
                resource = xfileset.create_resource(
                    fileset.format.name.upper())
                if fileset.format.directory:
                    for fname in os.listdir(fileset.path):
                        resource.upload(
                            op.join(fileset.path, fname), fname)
                else:
                    resource.upload(fileset.path, fileset.fname)
            for field in self.session.fields:
                xsession.fields[field.name] = field.value

    def tearDown(self):
        # Clean up working dirs
        shutil.rmtree(self.cache_dir, ignore_errors=True)
        # Clean up session created for unit-test
        self._delete_project()

    def _connect(self):
        return xnat.connect(SERVER)


class TestMultiSubjectOnXnatMixin(CreateXnatProjectMixin):

    sanitize_id_re = re.compile(r'[^a-zA-Z_0-9]')

    def setUp(self):
        self._clean_up()
        self._repository = XnatRepository(project_id=self.project,
                                          server=SERVER,
                                          cache_dir=self.cache_dir)
        self.BASE_CLASS.setUp(self)
        local_repository = DirectoryRepository(self.project_dir)
        tree = local_repository.tree()
        self._create_project()
        repo = XnatRepository(SERVER, self.project, '/tmp')
        with repo:
            for node in tree:
                for fileset in node.filesets:
                    repo.put_fileset(fileset)
                for field in node.fields:
                    repo.put_field(field)

    def _upload_datset(self, xnat_login, fileset, xsession):
        if self._is_derived(fileset):
            type_name = self._derived_name(fileset)
        else:
            type_name = fileset.name
        xfileset = xnat_login.classes.MrScanData(
            type=type_name, parent=xsession)
        xresource = xfileset.create_resource(
            fileset.format.name.upper())
        if fileset.format.directory:
            for fname in os.listdir(fileset.path):
                fpath = op.join(fileset.path, fname)
                xresource.upload(fpath, fname)
        else:
            if not op.exists(fileset.path):
                raise ArcanaError(
                    "Cannot upload fileset {} as path ({}) does "
                    "not exist".format(fileset, fpath))
            xresource.upload(
                fileset.path,
                op.basename(fileset.path))

    @classmethod
    def _is_derived(cls, fileset):
        # return fileset.name.endswith(self.DERIVED_SUFFIX
        return '_' in fileset.name

    @classmethod
    def _derived_name(cls, fileset):
        # return name[:-len(self.DERIVED_SUFFIX)]
        return fileset.name

    def tearDown(self):
        self._clean_up()
        self._delete_project()

    def _clean_up(self):
        # Clean up working dirs
        shutil.rmtree(self.cache_dir, ignore_errors=True)

    @property
    def repository(self):
        return self._repository

    @property
    def xnat_session_name(self):
        return '{}_{}'.format(self.project, self.base_name)

    @property
    def project_dir(self):
        return op.join(self.repository_path, self.base_name)

    @property
    def output_cache_dir(self):
        return self._output_cache_dir

    @property
    def base_name(self):
        return self.BASE_CLASS._get_name()

    def _full_subject_id(self, subject):
        return self.project + '_' + subject

    def get_session_dir(self, subject=None, visit=None,
                        frequency='per_session'):
        if subject is None and frequency in ('per_session', 'per_subject'):
            subject = self.SUBJECT
        if visit is None and frequency in ('per_session', 'per_visit'):
            visit = self.VISIT
        if frequency == 'per_session':
            assert subject is not None
            assert visit is not None
            parts = [self.project, subject, visit]
        elif frequency == 'per_subject':
            assert subject is not None
            assert visit is None
            parts = [self.project, subject, XnatRepository.SUMMARY_NAME]
        elif frequency == 'per_visit':
            assert visit is not None
            assert subject is None
            parts = [self.project, XnatRepository.SUMMARY_NAME, visit]
        elif frequency == 'per_study':
            assert subject is None
            assert visit is None
            parts = [self.project, XnatRepository.SUMMARY_NAME,
                     XnatRepository.SUMMARY_NAME]
        else:
            assert False
        session_id = '_'.join(parts)
        session_path = op.join(self.output_cache_dir, session_id)
        if not op.exists(session_path):
            raise ArcanaError(
                "Session path '{}' does not exist".format(session_path))
        return session_path

    def output_file_path(self, fname, from_study, subject=None, visit=None,
                         frequency='per_session'):
        try:
            acq_path = self.BASE_CLASS.output_file_path(
                self, fname, from_study, subject=subject, visit=visit,
                frequency=frequency, derived=False)
        except KeyError:
            acq_path = None
        try:
            proc_path = self.BASE_CLASS.output_file_path(
                self, fname, from_study, subject=subject, visit=visit,
                frequency=frequency, derived=True)
        except KeyError:
            proc_path = None
        if acq_path is not None and op.exists(acq_path):
            if op.exists(proc_path):
                raise ArcanaError(
                    "Both acquired and derived paths were found for "
                    "'{}_{}' ({} and {})".format(from_study, fname, acq_path,
                                                 proc_path))
            path = acq_path
        else:
            path = proc_path
        return path


def filter_scans(names):
    return sorted(f for f in sorted(names)
                  if (f != XnatRepository.PROV_SCAN and
                      not f.endswith(XnatRepository.MD5_SUFFIX)))
