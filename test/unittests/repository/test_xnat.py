from __future__ import absolute_import
from builtins import str
from builtins import next
from builtins import range
from builtins import object
import string
import random
import os.path
import shutil
import tempfile
import re
import json
import time
import unittest
from multiprocessing import Process
from unittest import TestCase
import xnat
from arcana.testing import (
    BaseTestCase, BaseMultiSubjectTestCase)
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from arcana.repository.xnat import XnatRepository
from arcana.repository.local import LocalRepository
from arcana.study import Study, StudyMetaClass
from arcana.runner import LinearRunner
from arcana.dataset import (
    DatasetMatch, DatasetSpec, FieldSpec)
from arcana.file_format import FileFormat
from arcana.utils import PATH_SUFFIX, JSON_ENCODING
from arcana.exception import ArcanaError
from arcana.file_format.standard import text_format
from arcana.repository.tree import Project, Subject, Session, Visit
from arcana.dataset import Dataset
import sys
import logging
from future.utils import with_metaclass
# Import TestExistingPrereqs study to test it on XNAT
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import test_dataset  # @UnresolvedImport @IgnorePep8
sys.path.pop(0)

# Import TestExistingPrereqs study to test it on XNAT
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..',
                                'study'))
import test_study  # @UnresolvedImport @IgnorePep8
sys.path.pop(0)

# Import test_local to run TestProjectInfo on XNAT using TestOnXnat mixin
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
import test_local  # @UnresolvedImport @IgnorePep8
sys.path.pop(0)


logger = logging.getLogger('arcana')

dicom_format = FileFormat(name='dicom', extension=None,
                          directory=True, within_dir_exts=['.dcm'])

try:
    SERVER = os.environ['ARCANA_TEST_XNAT']
except KeyError:
    SERVER = None

SKIP_ARGS = (SERVER is None,
             "Skipping as ARCANA_TEST_XNAT env var not set")


class DummyStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        DatasetSpec('source1', text_format),
        DatasetSpec('source2', text_format, optional=True),
        DatasetSpec('source3', text_format, optional=True),
        DatasetSpec('source4', text_format, optional=True),
        DatasetSpec('sink1', text_format, 'dummy_pipeline'),
        DatasetSpec('sink3', text_format, 'dummy_pipeline'),
        DatasetSpec('sink4', text_format, 'dummy_pipeline'),
        DatasetSpec('subject_sink', text_format, 'dummy_pipeline',
                    frequency='per_subject'),
        DatasetSpec('visit_sink', text_format, 'dummy_pipeline',
                    frequency='per_visit'),
        DatasetSpec('project_sink', text_format, 'dummy_pipeline',
                    frequency='per_project'),
        DatasetSpec('resink1', text_format, 'dummy_pipeline'),
        DatasetSpec('resink2', text_format, 'dummy_pipeline'),
        DatasetSpec('resink3', text_format, 'dummy_pipeline'),
        FieldSpec('field1', int, 'dummy_pipeline'),
        FieldSpec('field2', float, 'dummy_pipeline'),
        FieldSpec('field3', str, 'dummy_pipeline')]

    def dummy_pipeline(self):
        pass


class TestStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        DatasetSpec('dataset1', text_format),
        DatasetSpec('dataset2', text_format, optional=True),
        DatasetSpec('dataset3', text_format),
        DatasetSpec('dataset5', text_format, optional=True)]


def ls_with_md5_filter(path):
    return [f for f in sorted(os.listdir(path))
            if not f.endswith(XnatRepository.MD5_SUFFIX)]


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

    def session_label(self, project=None, subject=None, visit=None):
        if project is None:
            project = self.project
        if subject is None:
            subject = self.SUBJECT
        if visit is None:
            visit = self.VISIT
        return '_'.join((project, subject, visit))

    def session_cache(self, base_dir=None, project=None, subject=None,
                      visit=None):
        if base_dir is None:
            base_dir = self.cache_dir
        if project is None:
            project = self.project
        if subject is None:
            subject = self.SUBJECT
        return os.path.join(
            base_dir, project, '{}_{}'.format(project, subject),
            self.session_label(project=project, subject=subject,
                               visit=visit))

    def proc_session_cache(self, *args, **kwargs):
        return self.session_cache(
            *args, **kwargs) + XnatRepository.PROCESSED_SUFFIX

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
            for dataset in self.session.datasets:
                # Create dataset with an integer ID instead of the
                # label
                query = {'xsiType': 'xnat:mrScanData',
                         'req_format': 'qa',
                         'type': dataset.name}
                uri = '{}/scans/{}'.format(xsession.fulluri, dataset.id)
                login.put(uri, query=query)
                # Get XnatPy object for newly created dataset
                xdataset = login.classes.MrScanData(uri,
                                                    xnat_session=login)
                resource = xdataset.create_resource(
                    dataset.format.name.upper())
                if dataset.format.directory:
                    for fname in os.listdir(dataset.path):
                        resource.upload(
                            os.path.join(dataset.path, fname), fname)
                else:
                    resource.upload(dataset.path, dataset.fname())
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
        local_repository = LocalRepository(self.project_dir)
        project = local_repository.get_tree()
        self._create_project()
        with xnat.connect(SERVER) as xnat_login:
            # Copy local repository to XNAT
            xproject = xnat_login.projects[self.project]
            for subject in project.subjects:
                subj_id = self.project + '_' + subject.id
                xsubject = xnat_login.classes.SubjectData(
                    label=subj_id, parent=xproject)
                for session in subject.sessions:
                    sess_id = subj_id + '_' + session.visit_id
                    xsession = xnat_login.classes.MrSessionData(
                        label=sess_id,
                        parent=xsubject)
                    if any(self._is_derived(d) for d in session.datasets):
                        xsession_proc = xnat_login.classes.MrSessionData(
                            label=sess_id + XnatRepository.PROCESSED_SUFFIX,
                            parent=xsubject)
                    for dataset in session.datasets:
                        if self._is_derived(dataset):
                            xsess = xsession_proc
                        else:
                            xsess = xsession
                        self._upload_datset(xnat_login, dataset, xsess)
                    for field in session.fields:
                        xsession.fields[field.name] = field.value
                if subject.datasets or subject.fields:
                    xsubj_summary = xnat_login.classes.MrSessionData(
                        label=XnatRepository.get_labels(
                            'per_subject', self.project,
                            subject_id=subject.id)[1],
                        parent=xsubject)
                    for dataset in subject.datasets:
                        self._upload_datset(xnat_login, dataset,
                                            xsubj_summary)
                    for field in subject.fields:
                        xsubj_summary.fields[field.name] = field.value
            for visit in project.visits:
                if visit.datasets or visit.fields:
                    (summ_subj_name,
                     summ_sess_name) = XnatRepository.get_labels(
                        'per_visit', self.project,
                        visit_id=visit.id)
                    xvisit_summary = xnat_login.classes.MrSessionData(
                        label=summ_sess_name,
                        parent=xnat_login.classes.SubjectData(
                            label=summ_subj_name, parent=xproject))
                    for dataset in visit.datasets:
                        self._upload_datset(xnat_login, dataset,
                                            xvisit_summary)
                    for field in visit.fields:
                        xvisit_summary.fields[field.name] = field.value
            if project.datasets or project.fields:
                (summ_subj_name,
                 summ_sess_name) = XnatRepository.get_labels(
                    'per_project', self.project)
                xproj_summary = xnat_login.classes.MrSessionData(
                    label=summ_sess_name,
                    parent=xnat_login.classes.SubjectData(
                        label=summ_subj_name, parent=xproject))
                for dataset in project.datasets:
                    self._upload_datset(xnat_login, dataset,
                                        xproj_summary)
                for field in project.fields:
                    xproj_summary.fields[field.name] = field.value
        self._output_cache_dir = tempfile.mkdtemp()

    def _upload_datset(self, xnat_login, dataset, xsession):
        if self._is_derived(dataset):
            type_name = self._derived_name(dataset)
        else:
            type_name = dataset.name
        xdataset = xnat_login.classes.MrScanData(
            type=type_name, parent=xsession)
        xresource = xdataset.create_resource(
            dataset.format.name.upper())
        if dataset.format.directory:
            for fname in os.listdir(dataset.path):
                fpath = os.path.join(dataset.path, fname)
                xresource.upload(fpath, fname)
        else:
            xresource.upload(
                dataset.path,
                os.path.basename(dataset.path))

    @classmethod
    def _is_derived(cls, dataset):
        # return dataset.name.endswith(self.DERIVED_SUFFIX
        return '_' in dataset.name

    @classmethod
    def _derived_name(cls, dataset):
        # return name[:-len(self.DERIVED_SUFFIX)]
        return dataset.name

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
        return os.path.join(self.repository_path, self.base_name)

    @property
    def output_cache_dir(self):
        return self._output_cache_dir

    @property
    def base_name(self):
        return self.BASE_CLASS._get_name()

    def _full_subject_id(self, subject):
        return self.project + '_' + subject

    def _proc_sess_id(self, session):
        return session + XnatRepository.PROCESSED_SUFFIX

    def get_session_dir(self, subject=None, visit=None,
                        frequency='per_session', derived=False):
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
        elif frequency == 'per_project':
            assert subject is None
            assert visit is None
            parts = [self.project, XnatRepository.SUMMARY_NAME,
                     XnatRepository.SUMMARY_NAME]
        else:
            assert False
        session_id = '_'.join(parts)
        if derived:
            session_id += XnatRepository.PROCESSED_SUFFIX
        session_path = os.path.join(self.output_cache_dir, session_id)
        if not os.path.exists(session_path):
            raise ArcanaError(
                "Session path '{}' does not exist".format(session_path))
        return session_path

    def output_file_path(self, fname, study_name, subject=None, visit=None,
                         frequency='per_session'):
        try:
            acq_path = self.BASE_CLASS.output_file_path(
                self, fname, study_name, subject=subject, visit=visit,
                frequency=frequency, derived=False)
        except KeyError:
            acq_path = None
        try:
            proc_path = self.BASE_CLASS.output_file_path(
                self, fname, study_name, subject=subject, visit=visit,
                frequency=frequency, derived=True)
        except KeyError:
            proc_path = None
        if acq_path is not None and os.path.exists(acq_path):
            if os.path.exists(proc_path):
                raise ArcanaError(
                    "Both acquired and derived paths were found for "
                    "'{}_{}' ({} and {})".format(study_name, fname, acq_path,
                                                 proc_path))
            path = acq_path
        else:
            path = proc_path
        return path


class TestXnatSourceAndSinkBase(TestOnXnatMixin, BaseTestCase):

    SUBJECT = 'SUBJECT'
    VISIT = 'VISIT'
    STUDY_NAME = 'astudy'
    SUMMARY_STUDY_NAME = 'asummary'

    INPUT_DATASETS = {'source1': 'foo', 'source2': 'bar',
                      'source3': 'wee', 'source4': 'wa'}
    INPUT_FIELDS = {'field1': 1, 'field2': 0.5, 'field3': 'boo'}

    def setUp(self):
        BaseTestCase.setUp(self)
        TestOnXnatMixin.setUp(self)

    def tearDown(self):
        TestOnXnatMixin.tearDown(self)
        BaseTestCase.tearDown(self)


class TestXnatSourceAndSink(TestXnatSourceAndSinkBase):

    @property
    def digest_sink_project(self):
        return self.project + 'SINK'

    def setUp(self):
        TestXnatSourceAndSinkBase.setUp(self)
        self._create_project(self.digest_sink_project)

    def tearDown(self):
        TestXnatSourceAndSinkBase.tearDown(self)
        self._delete_project(self.digest_sink_project)

    @unittest.skipIf(*SKIP_ARGS)
    def test_repository_roundtrip(self):

        # Create working dirs
        # Create DarisSource node
        repository = XnatRepository(
            project_id=self.project,
            server=SERVER, cache_dir=self.cache_dir)
        study = DummyStudy(
            self.STUDY_NAME, repository, runner=LinearRunner('a_dir'),
            inputs=[DatasetMatch('source1', text_format, 'source1'),
                    DatasetMatch('source2', text_format, 'source2'),
                    DatasetMatch('source3', text_format, 'source3'),
                    DatasetMatch('source4', text_format, 'source4')])
        # TODO: Should test out other file formats as well.
        source_files = [study.input(n)
                        for n in ('source1', 'source2', 'source3',
                                  'source4')]
        sink_files = [study.spec(n)
                      for n in ('sink1', 'sink3', 'sink4')]
        inputnode = pe.Node(IdentityInterface(['subject_id',
                                               'visit_id']),
                            'inputnode')
        inputnode.inputs.subject_id = str(self.SUBJECT)
        inputnode.inputs.visit_id = str(self.VISIT)
        source = repository.source(source_files,
                                   study_name=self.STUDY_NAME)
        sink = repository.sink(sink_files,
                               study_name=self.STUDY_NAME)
        sink.inputs.name = 'repository-roundtrip-unittest'
        sink.inputs.desc = (
            "A test session created by repository roundtrip unittest")
        # Create workflow connecting them together
        workflow = pe.Workflow('source-sink-unit-test',
                               base_dir=self.work_dir)
        workflow.add_nodes((source, sink))
        workflow.connect(inputnode, 'subject_id', source, 'subject_id')
        workflow.connect(inputnode, 'visit_id', source, 'visit_id')
        workflow.connect(inputnode, 'subject_id', sink, 'subject_id')
        workflow.connect(inputnode, 'visit_id', sink, 'visit_id')
        for source_file in source_files:
            if source_file.name != 'source2':
                sink_name = source_file.name.replace('source', 'sink')
                workflow.connect(
                    source, source_file.name + PATH_SUFFIX,
                    sink, sink_name + PATH_SUFFIX)
        workflow.run()
        # Check cache was created properly
        self.assertEqual(ls_with_md5_filter(self.session_cache()),
                         ['source1.txt', 'source2.txt',
                          'source3.txt', 'source4.txt'])
        expected_sink_datasets = [self.STUDY_NAME + '_sink1',
                                  self.STUDY_NAME + '_sink3',
                                  self.STUDY_NAME + '_sink4']
        self.assertEqual(
            ls_with_md5_filter(self.proc_session_cache()),
            [d + text_format.extension
             for d in expected_sink_datasets])
        with self._connect() as mbi_xnat:
            dataset_names = list(mbi_xnat.experiments[
                self.session_label() +
                XnatRepository.PROCESSED_SUFFIX].scans.keys())
        self.assertEqual(sorted(dataset_names), expected_sink_datasets)

    @unittest.skipIf(*SKIP_ARGS)
    def test_fields_roundtrip(self):
        repository = XnatRepository(
            server=SERVER, cache_dir=self.cache_dir,
            project_id=self.project)
        study = DummyStudy(
            self.STUDY_NAME, repository, runner=LinearRunner('a_dir'),
            inputs=[DatasetMatch('source1', text_format, 'source1')])
        fields = [study.spec('field{}'.format(i)) for i in range(1, 4)]
        sink = repository.sink(
            outputs=fields,
            name='fields_sink',
            study_name='test')
        sink.inputs.field1_field = field1 = 1
        sink.inputs.field2_field = field2 = 2.0
        sink.inputs.field3_field = field3 = str('3')
        sink.inputs.subject_id = self.SUBJECT
        sink.inputs.visit_id = self.VISIT
        sink.inputs.desc = "Test sink of fields"
        sink.inputs.name = 'test_sink'
        sink.run()
        source = repository.source(
            inputs=fields,
            name='fields_source',
            study_name='test')
        source.inputs.visit_id = self.VISIT
        source.inputs.subject_id = self.SUBJECT
        source.inputs.desc = "Test source of fields"
        source.inputs.name = 'test_source'
        results = source.run()
        self.assertEqual(results.outputs.field1_field, field1)
        self.assertEqual(results.outputs.field2_field, field2)
        self.assertEqual(results.outputs.field3_field, field3)

    @unittest.skipIf(*SKIP_ARGS)
    def test_delayed_download(self):
        """
        Tests handling of race conditions where separate processes attempt to
        cache the same dataset
        """
        cache_dir = os.path.join(self.work_dir,
                                 'cache-delayed-download')
        DATASET_NAME = 'source1'
        target_path = os.path.join(self.session_cache(cache_dir),
                                   DATASET_NAME + text_format.extension)
        tmp_dir = target_path + '.download'
        shutil.rmtree(cache_dir, ignore_errors=True)
        os.makedirs(cache_dir)
        repository = XnatRepository(server=SERVER, cache_dir=cache_dir,
                                    project_id=self.project)
        study = DummyStudy(
            self.STUDY_NAME, repository, LinearRunner('ad'),
            inputs=[DatasetMatch(DATASET_NAME, text_format,
                                 DATASET_NAME)])
        source = repository.source([study.input(DATASET_NAME)],
                                   name='delayed_source',
                                   study_name='delayed_study')
        source.inputs.subject_id = self.SUBJECT
        source.inputs.visit_id = self.VISIT
        result1 = source.run()
        source1_path = result1.outputs.source1_path
        self.assertTrue(os.path.exists(source1_path))
        self.assertEqual(source1_path, target_path,
                         "Output file path '{}' not equal to target path '{}'"
                         .format(source1_path, target_path))
        # Clear cache to start again
        shutil.rmtree(cache_dir, ignore_errors=True)
        # Create tmp_dir before running interface, this time should wait for 1
        # second, check to see that the session hasn't been created and then
        # clear it and redownload the dataset.
        os.makedirs(tmp_dir)
        source.inputs.race_cond_delay = 1
        result2 = source.run()
        source1_path = result2.outputs.source1_path
        # Clear cache to start again
        shutil.rmtree(cache_dir, ignore_errors=True)
        # Create tmp_dir before running interface, this time should wait for 1
        # second, check to see that the session hasn't been created and then
        # clear it and redownload the dataset.
        internal_dir = os.path.join(tmp_dir, 'internal')
        deleted_tmp_dir = tmp_dir + '.deleted'

        def simulate_download():
            "Simulates a download in a separate process"
            os.makedirs(internal_dir)
            time.sleep(5)
            # Modify a file in the temp dir to make the source download keep
            # waiting
            logger.info('Updating simulated download directory')
            with open(os.path.join(internal_dir, 'download'), 'a') as f:
                f.write('downloading')
            time.sleep(10)
            # Simulate the finalising of the download by copying the previously
            # downloaded file into place and deleting the temp dir.
            logger.info('Finalising simulated download')
            with open(target_path, 'a') as f:
                f.write('simulated')
            shutil.move(tmp_dir, deleted_tmp_dir)

        source.inputs.race_cond_delay = 10
        p = Process(target=simulate_download)
        p.start()  # Start the simulated download in separate process
        time.sleep(1)
        source.run()  # Run the local download
        p.join()
        with open(os.path.join(deleted_tmp_dir, 'internal', 'download')) as f:
            d = f.read()
        self.assertEqual(d, 'downloading')
        with open(target_path) as f:
            d = f.read()
        self.assertEqual(d, 'simulated')

    @unittest.skipIf(*SKIP_ARGS)
    def test_digest_check(self):
        """
        Tests check of downloaded digests to see if file needs to be
        redownloaded
        """
        cache_dir = os.path.join(self.work_dir, 'cache-digest-check')
        DATASET_NAME = 'source1'
        STUDY_NAME = 'digest_check_study'
        dataset_fpath = DATASET_NAME + text_format.extension
        source_target_path = os.path.join(self.session_cache(cache_dir),
                                          dataset_fpath)
        md5_path = source_target_path + XnatRepository.MD5_SUFFIX
        shutil.rmtree(cache_dir, ignore_errors=True)
        os.makedirs(cache_dir)
        repository = XnatRepository(
            project_id=self.project,
            server=SERVER, cache_dir=cache_dir)
        study = DummyStudy(
            STUDY_NAME, repository, LinearRunner('ad'),
            inputs=[DatasetMatch(DATASET_NAME, text_format,
                                 DATASET_NAME)])
        source = repository.source([study.input(DATASET_NAME)],
                                   name='digest_check_source',
                                   study_name=STUDY_NAME)
        source.inputs.subject_id = self.SUBJECT
        source.inputs.visit_id = self.VISIT
        source.run()
        self.assertTrue(os.path.exists(md5_path))
        self.assertTrue(os.path.exists(source_target_path))
        with open(md5_path) as f:
            digests = json.load(f)
        # Stash the downloaded file in a new location and create a dummy
        # file instead
        stash_path = source_target_path + '.stash'
        shutil.move(source_target_path, stash_path)
        with open(source_target_path, 'w') as f:
            f.write('dummy')
        # Run the download, which shouldn't download as the digests are the
        # same
        source.run()
        with open(source_target_path) as f:
            d = f.read()
        self.assertEqual(d, 'dummy')
        # Replace the digest with a dummy
        os.remove(md5_path)
        digests[dataset_fpath] = 'dummy_digest'
        with open(md5_path, 'w', **JSON_ENCODING) as f:
            json.dump(digests, f)
        # Retry the download, which should now download since the digests
        # differ
        source.run()
        with open(source_target_path) as f:
            d = f.read()
        with open(stash_path) as f:
            e = f.read()
        self.assertEqual(d, e)
        # Resink the source file and check that the generated MD5 digest is
        # stored in identical format
        sink_repository = XnatRepository(
            project_id=self.digest_sink_project, server=SERVER,
            cache_dir=cache_dir)
        DATASET_NAME = 'sink1'
        sink = sink_repository.sink(
            [study.spec(DATASET_NAME)],
            name='digest_check_sink',
            study_name=STUDY_NAME)
        sink.inputs.name = 'digest_check_sink'
        sink.inputs.desc = "Tests the generation of MD5 digests"
        sink.inputs.subject_id = self.SUBJECT
        sink.inputs.visit_id = self.VISIT
        sink.inputs.sink1_path = source_target_path
        sink_fpath = (STUDY_NAME + '_' + DATASET_NAME +
                      text_format.extension)
        sink_target_path = os.path.join(
            (self.session_cache(
                cache_dir, project=self.digest_sink_project,
                subject=(self.SUBJECT)) +
             XnatRepository.PROCESSED_SUFFIX),
            sink_fpath)
        sink_md5_path = sink_target_path + XnatRepository.MD5_SUFFIX
        sink.run()
        with open(md5_path) as f:
            source_digests = json.load(f)
        with open(sink_md5_path) as f:
            sink_digests = json.load(f)
        self.assertEqual(
            source_digests[dataset_fpath],
            sink_digests[sink_fpath],
            ("Source digest ({}) did not equal sink digest ({})"
             .format(source_digests[dataset_fpath],
                     sink_digests[sink_fpath])))


class TestXnatSummarySourceAndSink(TestXnatSourceAndSinkBase):

    @unittest.skipIf(*SKIP_ARGS)
    def test_summary(self):
        # Create working dirs
        # Create XnatSource node
        repository = XnatRepository(
            server=SERVER, cache_dir=self.cache_dir,
            project_id=self.project)
        study = DummyStudy(
            self.SUMMARY_STUDY_NAME, repository, LinearRunner('ad'),
            inputs=[
                DatasetMatch('source1', text_format, 'source1'),
                DatasetMatch('source2', text_format, 'source2'),
                DatasetMatch('source3', text_format, 'source3')])
        # TODO: Should test out other file formats as well.
        source_files = [study.input(n)
                        for n in ('source1', 'source2', 'source3')]
        inputnode = pe.Node(IdentityInterface(['subject_id', 'visit_id']),
                            'inputnode')
        inputnode.inputs.subject_id = self.SUBJECT
        inputnode.inputs.visit_id = self.VISIT
        source = repository.source(source_files)
        subject_sink_files = [
            study.spec('subject_sink')]
        subject_sink = repository.sink(
            subject_sink_files,
            frequency='per_subject',
            study_name=self.SUMMARY_STUDY_NAME)
        subject_sink.inputs.name = 'subject_summary'
        subject_sink.inputs.desc = (
            "Tests the sinking of subject-wide datasets")
        # Test visit sink
        visit_sink_files = [study.spec('visit_sink')]
        visit_sink = repository.sink(
            visit_sink_files,
            frequency='per_visit',
            study_name=self.SUMMARY_STUDY_NAME)
        visit_sink.inputs.name = 'visit_summary'
        visit_sink.inputs.desc = (
            "Tests the sinking of visit-wide datasets")
        # Test project sink
        project_sink_files = [
            study.spec('project_sink')]
        project_sink = repository.sink(
            project_sink_files,
            frequency='per_project',
            study_name=self.SUMMARY_STUDY_NAME)

        project_sink.inputs.name = 'project_summary'
        project_sink.inputs.desc = (
            "Tests the sinking of project-wide datasets")
        # Create workflow connecting them together
        workflow = pe.Workflow('summary_unittest',
                               base_dir=self.work_dir)
        workflow.add_nodes((source, subject_sink, visit_sink,
                            project_sink))
        workflow.connect(inputnode, 'subject_id', source, 'subject_id')
        workflow.connect(inputnode, 'visit_id', source, 'visit_id')
        workflow.connect(inputnode, 'subject_id', subject_sink, 'subject_id')
        workflow.connect(inputnode, 'visit_id', visit_sink, 'visit_id')
        workflow.connect(
            source, 'source1' + PATH_SUFFIX,
            subject_sink, 'subject_sink' + PATH_SUFFIX)
        workflow.connect(
            source, 'source2' + PATH_SUFFIX,
            visit_sink, 'visit_sink' + PATH_SUFFIX)
        workflow.connect(
            source, 'source3' + PATH_SUFFIX,
            project_sink, 'project_sink' + PATH_SUFFIX)
        workflow.run()
        with self._connect() as mbi_xnat:
            # Check subject summary directories were created properly in cache
            expected_subj_datasets = [self.SUMMARY_STUDY_NAME +
                                      '_subject_sink']
            subject_dir = os.path.join(
                self.cache_dir, self.project,
                '_'.join((self.project, self.SUBJECT)),
                '_'.join((self.project, self.SUBJECT,
                         XnatRepository.SUMMARY_NAME)))
            self.assertEqual(ls_with_md5_filter(subject_dir),
                             [d + text_format.extension
                              for d in expected_subj_datasets])
            # and on XNAT
            subject_dataset_names = list(mbi_xnat.projects[
                self.project].experiments[
                    '_'.join((self.project, self.SUBJECT,
                              XnatRepository.SUMMARY_NAME))].scans.keys())
            self.assertEqual(expected_subj_datasets, subject_dataset_names)
            # Check visit summary directories were created properly in
            # cache
            expected_visit_datasets = [self.SUMMARY_STUDY_NAME +
                                       '_visit_sink']
            visit_dir = os.path.join(
                self.cache_dir, self.project,
                self.project + '_' + XnatRepository.SUMMARY_NAME,
                (self.project + '_' + XnatRepository.SUMMARY_NAME +
                 '_' + self.VISIT))
            self.assertEqual(ls_with_md5_filter(visit_dir),
                             [d + text_format.extension
                              for d in expected_visit_datasets])
            # and on XNAT
            visit_dataset_names = list(mbi_xnat.projects[
                self.project].experiments[
                    '{}_{}_{}'.format(
                        self.project, XnatRepository.SUMMARY_NAME,
                        self.VISIT)].scans.keys())
            self.assertEqual(expected_visit_datasets, visit_dataset_names)
            # Check project summary directories were created properly in cache
            expected_proj_datasets = [self.SUMMARY_STUDY_NAME +
                                      '_project_sink']
            project_dir = os.path.join(
                self.cache_dir, self.project,
                self.project + '_' + XnatRepository.SUMMARY_NAME,
                self.project + '_' + XnatRepository.SUMMARY_NAME + '_' +
                XnatRepository.SUMMARY_NAME)
            self.assertEqual(ls_with_md5_filter(project_dir),
                             [d + text_format.extension
                              for d in expected_proj_datasets])
            # and on XNAT
            project_dataset_names = list(mbi_xnat.projects[
                self.project].experiments[
                    '{}_{sum}_{sum}'.format(
                        self.project,
                        sum=XnatRepository.SUMMARY_NAME)].scans.keys())
            self.assertEqual(expected_proj_datasets, project_dataset_names)
        # Reload the data from the summary directories
        reloadinputnode = pe.Node(IdentityInterface(['subject_id',
                                                     'visit_id']),
                                  'reload_inputnode')
        reloadinputnode.inputs.subject_id = self.SUBJECT
        reloadinputnode.inputs.visit_id = self.VISIT
        reloadsource = repository.source(
            (source_files + subject_sink_files + visit_sink_files +
             project_sink_files),
            name='reload_source',
            study_name=self.SUMMARY_STUDY_NAME)
        reloadsink = repository.sink(
            [study.spec(n)
             for n in ('resink1', 'resink2', 'resink3')],
            study_name=self.SUMMARY_STUDY_NAME)
        reloadsink.inputs.name = 'reload_summary'
        reloadsink.inputs.desc = (
            "Tests the reloading of subject and project summary datasets")
        reloadworkflow = pe.Workflow('reload_summary_unittest',
                                     base_dir=self.work_dir)
        reloadworkflow.connect(reloadinputnode, 'subject_id',
                               reloadsource, 'subject_id')
        reloadworkflow.connect(reloadinputnode, 'visit_id',
                               reloadsource, 'visit_id')
        reloadworkflow.connect(reloadinputnode, 'subject_id',
                               reloadsink, 'subject_id')
        reloadworkflow.connect(reloadinputnode, 'visit_id',
                               reloadsink, 'visit_id')
        reloadworkflow.connect(reloadsource,
                               'subject_sink' + PATH_SUFFIX,
                               reloadsink,
                               'resink1' + PATH_SUFFIX)
        reloadworkflow.connect(reloadsource,
                               'visit_sink' + PATH_SUFFIX,
                               reloadsink,
                               'resink2' + PATH_SUFFIX)
        reloadworkflow.connect(reloadsource,
                               'project_sink' + PATH_SUFFIX,
                               reloadsink,
                               'resink3' + PATH_SUFFIX)
        reloadworkflow.run()
        # Check that the datasets
        self.assertEqual(
            ls_with_md5_filter(self.proc_session_cache()),
            [self.SUMMARY_STUDY_NAME + '_resink1.txt',
             self.SUMMARY_STUDY_NAME + '_resink2.txt',
             self.SUMMARY_STUDY_NAME + '_resink3.txt'])
        # and on XNAT
        with self._connect() as mbi_xnat:
            resinked_dataset_names = list(mbi_xnat.projects[
                self.project].experiments[
                    self.session_label() +
                    XnatRepository.PROCESSED_SUFFIX].scans.keys())
            self.assertEqual(sorted(resinked_dataset_names),
                             [self.SUMMARY_STUDY_NAME + '_resink1',
                              self.SUMMARY_STUDY_NAME + '_resink2',
                              self.SUMMARY_STUDY_NAME + '_resink3'])


class TestDicomTagMatchAndIDOnXnat(TestOnXnatMixin,
                                   test_dataset.TestDicomTagMatch):

    BASE_CLASS = test_dataset.TestDicomTagMatch

    @property
    def ref_dir(self):
        return os.path.join(
            self.ref_path, self._get_name(self.BASE_CLASS))

    def setUp(self):
        test_dataset.TestDicomTagMatch.setUp(self)
        TestOnXnatMixin.setUp(self)
        # Set up DICOM headers
        with xnat.connect(SERVER) as login:
            xsess = login.projects[self.project].experiments[
                '_'.join((self.project, self.SUBJECT, self.VISIT))]
            login.put('/data/experiments/{}?pullDataFromHeaders=true'
                      .format(xsess.id))

    def tearDown(self):
        TestOnXnatMixin.tearDown(self)
        test_dataset.TestDicomTagMatch.tearDown(self)

    @unittest.skipIf(*SKIP_ARGS)
    def test_dicom_match(self):
        study = test_dataset.TestMatchStudy(
            name='test_dicom',
            repository=XnatRepository(
                project_id=self.project,
                server=SERVER, cache_dir=tempfile.mkdtemp()),
            runner=LinearRunner(self.work_dir),
            inputs=test_dataset.TestDicomTagMatch.DICOM_MATCH)
        phase = list(study.data('gre_phase'))[0]
        mag = list(study.data('gre_mag'))[0]
        self.assertEqual(phase.name, 'gre_field_mapping_3mm_phase')
        self.assertEqual(mag.name, 'gre_field_mapping_3mm_mag')

    @unittest.skipIf(*SKIP_ARGS)
    def test_id_match(self):
        study = test_dataset.TestMatchStudy(
            name='test_dicom',
            repository=XnatRepository(
                project_id=self.project,
                server=SERVER, cache_dir=tempfile.mkdtemp()),
            runner=LinearRunner(self.work_dir),
            inputs=[
                DatasetMatch('gre_phase', dicom_format, id=7),
                DatasetMatch('gre_mag', dicom_format, id=6)])
        phase = list(study.data('gre_phase'))[0]
        mag = list(study.data('gre_mag'))[0]
        self.assertEqual(phase.name, 'gre_field_mapping_3mm_phase')
        self.assertEqual(mag.name, 'gre_field_mapping_3mm_mag')

    @unittest.skipIf(*SKIP_ARGS)
    def test_order_match(self):
        test_dataset.TestDicomTagMatch.test_order_match(self)


class TestDatasetCacheOnPathAccess(TestOnXnatMixin,
                                   BaseTestCase):

    INPUT_DATASETS = {'dataset': '1'}

    @unittest.skipIf(*SKIP_ARGS)
    def test_cache_on_path_access(self):
        tmp_dir = tempfile.mkdtemp()
        repository = XnatRepository(
            project_id=self.project,
            server=SERVER, cache_dir=tmp_dir)
        tree = repository.get_tree(
            subject_ids=[self.SUBJECT],
            visit_ids=[self.VISIT])
        # Get a dataset
        dataset = next(next(next(tree.subjects).sessions).datasets)
        self.assertEqual(dataset._path, None)
        target_path = os.path.join(
            tmp_dir, self.project,
            '{}_{}'.format(self.project, self.SUBJECT),
            '{}_{}_{}'.format(self.project, self.SUBJECT, self.VISIT),
            dataset.fname())
        # This should implicitly download the dataset
        self.assertEqual(dataset.path, target_path)
        with open(target_path) as f:
            self.assertEqual(f.read(),
                             self.INPUT_DATASETS[dataset.name])


class TestExistingPrereqsOnXnat(TestMultiSubjectOnXnatMixin,
                                test_study.TestExistingPrereqs):

    BASE_CLASS = test_study.TestExistingPrereqs

    @unittest.skipIf(*SKIP_ARGS)
    def test_per_session_prereqs(self):
        super(TestExistingPrereqsOnXnat, self).test_per_session_prereqs()


class TestXnatCache(TestMultiSubjectOnXnatMixin,
                    BaseMultiSubjectTestCase):

    BASE_CLASS = BaseMultiSubjectTestCase
    STRUCTURE = {
        'subject1': {
            'visit1': ['dataset1', 'dataset2', 'dataset3'],
            'visit2': ['dataset1', 'dataset2', 'dataset3']},
        'subject2': {
            'visit1': ['dataset1', 'dataset2', 'dataset3'],
            'visit2': ['dataset1', 'dataset2', 'dataset3']}}

    DATASET_CONTENTS = {'dataset1': 1,
                        'dataset2': 2,
                        'dataset3': 3}

    @property
    def input_tree(self):
        sessions = []
        visit_ids = set()
        for subj_id, visits in list(self.STRUCTURE.items()):
            for visit_id, datasets in list(visits.items()):
                sessions.append(Session(subj_id, visit_id, datasets=[
                    Dataset(d, text_format, subject_id=subj_id,
                            visit_id=visit_id) for d in datasets]))
                visit_ids.add(visit_id)
        subjects = [Subject(i, sessions=[s for s in sessions
                                         if s.subject_id == i])
                    for i in self.STRUCTURE]
        visits = [Visit(i, sessions=[s for s in sessions
                                     if s.visit == i])
                  for i in visit_ids]
        return Project(subjects=subjects, visits=visits)

    @unittest.skipIf(*SKIP_ARGS)
    def test_cache_download(self):
        repository = XnatRepository(
            project_id=self.project,
            server=SERVER,
            cache_dir=tempfile.mkdtemp())
        study = self.create_study(
            TestStudy, 'cache_download',
            inputs=[
                DatasetMatch('dataset1', text_format, 'dataset1'),
                DatasetMatch('dataset3', text_format, 'dataset3')],
            repository=repository)
        study.cache_inputs()
        for subject_id, visits in list(self.STRUCTURE.items()):
            subj_dir = os.path.join(
                repository.cache_dir, self.project,
                '{}_{}'.format(self.project, subject_id))
            for visit_id in visits:
                sess_dir = os.path.join(
                    subj_dir,
                    '{}_{}_{}'.format(self.project, subject_id,
                                      visit_id))
                for inpt in study.inputs:
                    self.assertTrue(os.path.exists(os.path.join(
                        sess_dir, inpt.fname())))

    @property
    def base_name(self):
        return self.name


class TestProjectInfo(TestMultiSubjectOnXnatMixin,
                      test_local.TestProjectInfo):

    BASE_CLASS = test_local.TestProjectInfo

    @unittest.skipIf(*SKIP_ARGS)
    def test_project_info(self):
        tree = self.repository.get_tree()
        ref_tree = self.get_tree(self.repository, set_ids=True)
        self.assertEqual(
            tree, ref_tree,
            "Generated project doesn't match reference:{}"
            .format(tree.find_mismatch(ref_tree)))
