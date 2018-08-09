from __future__ import absolute_import
from builtins import str
from builtins import next
from builtins import range
from builtins import object
import string
import random
import os
import os.path as op
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
from arcana.repository.simple import SimpleRepository
from arcana.study import Study, StudyMetaClass
from arcana.processor import LinearProcessor
from arcana.data import (
    FilesetMatch, FilesetSpec, FieldSpec)
from arcana.data.file_format import FileFormat
from arcana.utils import PATH_SUFFIX, JSON_ENCODING
from arcana.exception import ArcanaError
from arcana.data.file_format.standard import text_format
from arcana.repository.tree import Tree, Subject, Session, Visit
from arcana.data import Fileset
import sys
import logging
from future.utils import with_metaclass
# Import TestExistingPrereqs study to test it on XNAT
sys.path.insert(0, op.join(op.dirname(__file__), '..'))
import test_fileset  # @UnresolvedImport @IgnorePep8
sys.path.pop(0)

# Import TestExistingPrereqs study to test it on XNAT
sys.path.insert(0, op.join(op.dirname(__file__), '..',
                                'study'))
import test_study  # @UnresolvedImport @IgnorePep8
sys.path.pop(0)

# Import test_local to run TestProjectInfo on XNAT using TestOnXnat mixin
sys.path.insert(0, op.join(op.dirname(__file__)))
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
        FilesetSpec('source1', text_format),
        FilesetSpec('source2', text_format, optional=True),
        FilesetSpec('source3', text_format, optional=True),
        FilesetSpec('source4', text_format, optional=True),
        FilesetSpec('sink1', text_format, 'dummy_pipeline'),
        FilesetSpec('sink3', text_format, 'dummy_pipeline'),
        FilesetSpec('sink4', text_format, 'dummy_pipeline'),
        FilesetSpec('subject_sink', text_format, 'dummy_pipeline',
                    frequency='per_subject'),
        FilesetSpec('visit_sink', text_format, 'dummy_pipeline',
                    frequency='per_visit'),
        FilesetSpec('project_sink', text_format, 'dummy_pipeline',
                    frequency='per_study'),
        FilesetSpec('resink1', text_format, 'dummy_pipeline'),
        FilesetSpec('resink2', text_format, 'dummy_pipeline'),
        FilesetSpec('resink3', text_format, 'dummy_pipeline'),
        FieldSpec('field1', int, 'dummy_pipeline'),
        FieldSpec('field2', float, 'dummy_pipeline'),
        FieldSpec('field3', str, 'dummy_pipeline')]

    def dummy_pipeline(self):
        pass


class TestStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        FilesetSpec('fileset1', text_format),
        FilesetSpec('fileset2', text_format, optional=True),
        FilesetSpec('fileset3', text_format),
        FilesetSpec('fileset5', text_format, optional=True)]


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
        simple_repository = SimpleRepository(self.project_dir)
        tree = simple_repository.tree()
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
        elif frequency == 'per_study':
            assert subject is None
            assert visit is None
            parts = [self.project, XnatRepository.SUMMARY_NAME,
                     XnatRepository.SUMMARY_NAME]
        else:
            assert False
        session_id = '_'.join(parts)
        if derived:
            session_id += XnatRepository.PROCESSED_SUFFIX
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
            self.STUDY_NAME, repository, processor=LinearProcessor('a_dir'),
            inputs=[FilesetMatch('source1', text_format, 'source1'),
                    FilesetMatch('source2', text_format, 'source2'),
                    FilesetMatch('source3', text_format, 'source3'),
                    FilesetMatch('source4', text_format, 'source4')])
        # TODO: Should test out other file formats as well.
        source_files = ['source1', 'source2', 'source3', 'source4']
        sink_files = ['sink1', 'sink3', 'sink4']
        inputnode = pe.Node(IdentityInterface(['subject_id',
                                               'visit_id']),
                            'inputnode')
        inputnode.inputs.subject_id = str(self.SUBJECT)
        inputnode.inputs.visit_id = str(self.VISIT)
        source = study.source(source_files)
        sink = study.sink(sink_files)
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
        for source_name in source_files:
            if source_name != 'source2':
                sink_name = source_name.replace('source', 'sink')
                workflow.connect(
                    source, source_name + PATH_SUFFIX,
                    sink, sink_name + PATH_SUFFIX)
        workflow.run()
        # Check cache was created properly
        self.assertEqual(ls_with_md5_filter(self.session_cache()),
                         ['source1.txt', 'source2.txt',
                          'source3.txt', 'source4.txt'])
        expected_sink_filesets = ['sink1', 'sink3', 'sink4']
        self.assertEqual(
            ls_with_md5_filter(self.session_cache(
                from_study=self.STUDY_NAME)),
            [d + text_format.extension
             for d in expected_sink_filesets])
        with self._connect() as login:
            fileset_names = list(login.experiments[self.session_label(
                from_study=self.STUDY_NAME)].scans.keys())
        self.assertEqual(sorted(fileset_names), expected_sink_filesets)

    @unittest.skipIf(*SKIP_ARGS)
    def test_fields_roundtrip(self):
        repository = XnatRepository(
            server=SERVER, cache_dir=self.cache_dir,
            project_id=self.project)
        study = DummyStudy(
            self.STUDY_NAME, repository, processor=LinearProcessor('a_dir'),
            inputs=[FilesetMatch('source1', text_format, 'source1')])
        fields = ['field{}'.format(i) for i in range(1, 4)]
        sink = study.sink(
            outputs=fields,
            name='fields_sink')
        sink.inputs.field1_field = field1 = 1
        sink.inputs.field2_field = field2 = 2.0
        sink.inputs.field3_field = field3 = str('3')
        sink.inputs.subject_id = self.SUBJECT
        sink.inputs.visit_id = self.VISIT
        sink.inputs.desc = "Test sink of fields"
        sink.inputs.name = 'test_sink'
        sink.run()
        source = study.source(
            inputs=fields,
            name='fields_source')
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
        cache the same fileset
        """
        cache_dir = op.join(self.work_dir,
                                 'cache-delayed-download')
        DATASET_NAME = 'source1'
        target_path = op.join(self.session_cache(cache_dir),
                                   DATASET_NAME + text_format.extension)
        tmp_dir = target_path + '.download'
        shutil.rmtree(cache_dir, ignore_errors=True)
        os.makedirs(cache_dir)
        repository = XnatRepository(server=SERVER, cache_dir=cache_dir,
                                    project_id=self.project)
        study = DummyStudy(
            self.STUDY_NAME, repository, LinearProcessor('ad'),
            inputs=[FilesetMatch(DATASET_NAME, text_format,
                                 DATASET_NAME)])
        source = study.source([study.input(DATASET_NAME)],
                                   name='delayed_source')
        source.inputs.subject_id = self.SUBJECT
        source.inputs.visit_id = self.VISIT
        result1 = source.run()
        source1_path = result1.outputs.source1_path
        self.assertTrue(op.exists(source1_path))
        self.assertEqual(source1_path, target_path,
                         "Output file path '{}' not equal to target path '{}'"
                         .format(source1_path, target_path))
        # Clear cache to start again
        shutil.rmtree(cache_dir, ignore_errors=True)
        # Create tmp_dir before running interface, this time should wait for 1
        # second, check to see that the session hasn't been created and then
        # clear it and redownload the fileset.
        os.makedirs(tmp_dir)
        source.inputs.race_cond_delay = 1
        result2 = source.run()
        source1_path = result2.outputs.source1_path
        # Clear cache to start again
        shutil.rmtree(cache_dir, ignore_errors=True)
        # Create tmp_dir before running interface, this time should wait for 1
        # second, check to see that the session hasn't been created and then
        # clear it and redownload the fileset.
        internal_dir = op.join(tmp_dir, 'internal')
        deleted_tmp_dir = tmp_dir + '.deleted'

        def simulate_download():
            "Simulates a download in a separate process"
            os.makedirs(internal_dir)
            time.sleep(5)
            # Modify a file in the temp dir to make the source download keep
            # waiting
            logger.info('Updating simulated download directory')
            with open(op.join(internal_dir, 'download'), 'a') as f:
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
        with open(op.join(deleted_tmp_dir, 'internal', 'download')) as f:
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
        cache_dir = op.join(self.work_dir, 'cache-digest-check')
        DATASET_NAME = 'source1'
        STUDY_NAME = 'digest_check_study'
        fileset_fpath = DATASET_NAME + text_format.extension
        source_target_path = op.join(self.session_cache(cache_dir),
                                          fileset_fpath)
        md5_path = source_target_path + XnatRepository.MD5_SUFFIX
        shutil.rmtree(cache_dir, ignore_errors=True)
        os.makedirs(cache_dir)
        source_repository = XnatRepository(
            project_id=self.project,
            server=SERVER, cache_dir=cache_dir)
        sink_repository = XnatRepository(
            project_id=self.digest_sink_project, server=SERVER,
            cache_dir=cache_dir)
        study = DummyStudy(
            STUDY_NAME, sink_repository, LinearProcessor('ad'),
            inputs=[FilesetMatch(DATASET_NAME, text_format,
                                 DATASET_NAME,
                                 repository=source_repository)],
            subject_ids=['SUBJECT'], visit_ids=['VISIT'],
            fill_tree=True)
        source = study.source([DATASET_NAME],
                              name='digest_check_source')
        source.inputs.subject_id = self.SUBJECT
        source.inputs.visit_id = self.VISIT
        source.run()
        self.assertTrue(op.exists(md5_path))
        self.assertTrue(op.exists(source_target_path))
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
        digests[fileset_fpath] = 'dummy_digest'
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
        DATASET_NAME = 'sink1'
        sink = study.sink(
            [DATASET_NAME],
            name='digest_check_sink')
        sink.inputs.name = 'digest_check_sink'
        sink.inputs.desc = "Tests the generation of MD5 digests"
        sink.inputs.subject_id = self.SUBJECT
        sink.inputs.visit_id = self.VISIT
        sink.inputs.sink1_path = source_target_path
        sink_fpath = DATASET_NAME + text_format.extension
        sink_target_path = op.join(
            self.session_cache(
                cache_dir, project=self.digest_sink_project,
                subject=(self.SUBJECT), from_study=STUDY_NAME),
            sink_fpath)
        sink_md5_path = sink_target_path + XnatRepository.MD5_SUFFIX
        sink.run()
        with open(md5_path) as f:
            source_digests = json.load(f)
        with open(sink_md5_path) as f:
            sink_digests = json.load(f)
        self.assertEqual(
            source_digests[fileset_fpath],
            sink_digests[sink_fpath],
            ("Source digest ({}) did not equal sink digest ({})"
             .format(source_digests[fileset_fpath],
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
            self.SUMMARY_STUDY_NAME, repository, LinearProcessor('ad'),
            inputs=[
                FilesetMatch('source1', text_format, 'source1'),
                FilesetMatch('source2', text_format, 'source2'),
                FilesetMatch('source3', text_format, 'source3')])
        # TODO: Should test out other file formats as well.
        source_files = ['source1', 'source2', 'source3']
        inputnode = pe.Node(IdentityInterface(['subject_id', 'visit_id']),
                            'inputnode')
        inputnode.inputs.subject_id = self.SUBJECT
        inputnode.inputs.visit_id = self.VISIT
        source = study.source(source_files)
        subject_sink_files = ['subject_sink']
        subject_sink = study.sink(
            subject_sink_files,
            frequency='per_subject')
        subject_sink.inputs.name = 'subject_summary'
        subject_sink.inputs.desc = (
            "Tests the sinking of subject-wide filesets")
        # Test visit sink
        visit_sink_files = ['visit_sink']
        visit_sink = study.sink(
            visit_sink_files,
            frequency='per_visit')
        visit_sink.inputs.name = 'visit_summary'
        visit_sink.inputs.desc = (
            "Tests the sinking of visit-wide filesets")
        # Test project sink
        project_sink_files = ['project_sink']
        project_sink = study.sink(
            project_sink_files,
            frequency='per_study')
        project_sink.inputs.name = 'project_summary'
        project_sink.inputs.desc = (
            "Tests the sinking of project-wide filesets")
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
        with self._connect() as login:
            # Check subject summary directories were created properly in cache
            expected_subj_filesets = ['subject_sink']
            subject_dir = self.session_cache(
                visit=XnatRepository.SUMMARY_NAME,
                from_study=self.SUMMARY_STUDY_NAME)
            self.assertEqual(ls_with_md5_filter(subject_dir),
                             [d + text_format.extension
                              for d in expected_subj_filesets])
            # and on XNAT
            subject_fileset_names = list(login.projects[
                self.project].experiments[
                    self.session_label(
                        visit=XnatRepository.SUMMARY_NAME,
                        from_study=self.SUMMARY_STUDY_NAME)].scans.keys())
            self.assertEqual(expected_subj_filesets, subject_fileset_names)
            # Check visit summary directories were created properly in
            # cache
            expected_visit_filesets = ['visit_sink']
            visit_dir = self.session_cache(
                subject=XnatRepository.SUMMARY_NAME,
                from_study=self.SUMMARY_STUDY_NAME)
            self.assertEqual(ls_with_md5_filter(visit_dir),
                             [d + text_format.extension
                              for d in expected_visit_filesets])
            # and on XNAT
            visit_fileset_names = list(login.projects[
                self.project].experiments[
                    self.session_label(
                        subject=XnatRepository.SUMMARY_NAME,
                        from_study=self.SUMMARY_STUDY_NAME)].scans.keys())
            self.assertEqual(expected_visit_filesets, visit_fileset_names)
            # Check project summary directories were created properly in cache
            expected_proj_filesets = ['project_sink']
            project_dir = self.session_cache(
                subject=XnatRepository.SUMMARY_NAME,
                visit=XnatRepository.SUMMARY_NAME,
                from_study=self.SUMMARY_STUDY_NAME)
            self.assertEqual(ls_with_md5_filter(project_dir),
                             [d + text_format.extension
                              for d in expected_proj_filesets])
            # and on XNAT
            project_fileset_names = list(login.projects[
                self.project].experiments[
                    self.session_label(
                        subject=XnatRepository.SUMMARY_NAME,
                        visit=XnatRepository.SUMMARY_NAME,
                        from_study=self.SUMMARY_STUDY_NAME)].scans.keys())
            self.assertEqual(expected_proj_filesets, project_fileset_names)
        # Reload the data from the summary directories
        reloadinputnode = pe.Node(IdentityInterface(['subject_id',
                                                     'visit_id']),
                                  'reload_inputnode')
        reloadinputnode.inputs.subject_id = self.SUBJECT
        reloadinputnode.inputs.visit_id = self.VISIT
        reloadsource = study.source(
            (source_files + subject_sink_files + visit_sink_files +
             project_sink_files),
            name='reload_source')
        reloadsink = study.sink(
            ['resink1', 'resink2', 'resink3'])
        reloadsink.inputs.name = 'reload_summary'
        reloadsink.inputs.desc = (
            "Tests the reloading of subject and project summary filesets")
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
        # Check that the filesets
        self.assertEqual(
            ls_with_md5_filter(self.session_cache(
                from_study=self.SUMMARY_STUDY_NAME)),
            ['resink1.txt', 'resink2.txt', 'resink3.txt'])
        # and on XNAT
        with self._connect() as login:
            resinked_fileset_names = list(login.projects[
                self.project].experiments[
                    self.session_label(
                        from_study=self.SUMMARY_STUDY_NAME)].scans.keys())
            self.assertEqual(sorted(resinked_fileset_names),
                             ['resink1', 'resink2', 'resink3'])


class TestDicomTagMatchAndIDOnXnat(TestOnXnatMixin,
                                   test_fileset.TestDicomTagMatch):

    BASE_CLASS = test_fileset.TestDicomTagMatch

    @property
    def ref_dir(self):
        return op.join(
            self.ref_path, self._get_name(self.BASE_CLASS))

    def setUp(self):
        test_fileset.TestDicomTagMatch.setUp(self)
        TestOnXnatMixin.setUp(self)
        # Set up DICOM headers
        with xnat.connect(SERVER) as login:
            xsess = login.projects[self.project].experiments[
                '_'.join((self.project, self.SUBJECT, self.VISIT))]
            login.put('/data/experiments/{}?pullDataFromHeaders=true'
                      .format(xsess.id))

    def tearDown(self):
        TestOnXnatMixin.tearDown(self)
        test_fileset.TestDicomTagMatch.tearDown(self)

    @unittest.skipIf(*SKIP_ARGS)
    def test_dicom_match(self):
        study = test_fileset.TestMatchStudy(
            name='test_dicom',
            repository=XnatRepository(
                project_id=self.project,
                server=SERVER, cache_dir=tempfile.mkdtemp()),
            processor=LinearProcessor(self.work_dir),
            inputs=test_fileset.TestDicomTagMatch.DICOM_MATCH)
        phase = list(study.data('gre_phase'))[0]
        mag = list(study.data('gre_mag'))[0]
        self.assertEqual(phase.name, 'gre_field_mapping_3mm_phase')
        self.assertEqual(mag.name, 'gre_field_mapping_3mm_mag')

    @unittest.skipIf(*SKIP_ARGS)
    def test_id_match(self):
        study = test_fileset.TestMatchStudy(
            name='test_dicom',
            repository=XnatRepository(
                project_id=self.project,
                server=SERVER, cache_dir=tempfile.mkdtemp()),
            processor=LinearProcessor(self.work_dir),
            inputs=[
                FilesetMatch('gre_phase', dicom_format, id=7),
                FilesetMatch('gre_mag', dicom_format, id=6)])
        phase = list(study.data('gre_phase'))[0]
        mag = list(study.data('gre_mag'))[0]
        self.assertEqual(phase.name, 'gre_field_mapping_3mm_phase')
        self.assertEqual(mag.name, 'gre_field_mapping_3mm_mag')

    @unittest.skipIf(*SKIP_ARGS)
    def test_order_match(self):
        test_fileset.TestDicomTagMatch.test_order_match(self)


class TestFilesetCacheOnPathAccess(TestOnXnatMixin,
                                   BaseTestCase):

    INPUT_DATASETS = {'fileset': '1'}

    @unittest.skipIf(*SKIP_ARGS)
    def test_cache_on_path_access(self):
        tmp_dir = tempfile.mkdtemp()
        repository = XnatRepository(
            project_id=self.project,
            server=SERVER, cache_dir=tmp_dir)
        tree = repository.tree(
            subject_ids=[self.SUBJECT],
            visit_ids=[self.VISIT])
        # Get a fileset
        fileset = next(next(next(tree.subjects).sessions).filesets)
        self.assertEqual(fileset._path, None)
        target_path = op.join(
            tmp_dir, self.project,
            '{}_{}'.format(self.project, self.SUBJECT),
            '{}_{}_{}'.format(self.project, self.SUBJECT, self.VISIT),
            fileset.fname)
        # This should implicitly download the fileset
        self.assertEqual(fileset.path, target_path)
        with open(target_path) as f:
            self.assertEqual(f.read(),
                             self.INPUT_DATASETS[fileset.name])


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
            'visit1': ['fileset1', 'fileset2', 'fileset3'],
            'visit2': ['fileset1', 'fileset2', 'fileset3']},
        'subject2': {
            'visit1': ['fileset1', 'fileset2', 'fileset3'],
            'visit2': ['fileset1', 'fileset2', 'fileset3']}}

    DATASET_CONTENTS = {'fileset1': 1,
                        'fileset2': 2,
                        'fileset3': 3}

    @property
    def input_tree(self):
        sessions = []
        visit_ids = set()
        for subj_id, visits in list(self.STRUCTURE.items()):
            for visit_id, filesets in list(visits.items()):
                sessions.append(Session(subj_id, visit_id, filesets=[
                    Fileset(d, text_format, subject_id=subj_id,
                            visit_id=visit_id) for d in filesets]))
                visit_ids.add(visit_id)
        subjects = [Subject(i, sessions=[s for s in sessions
                                         if s.subject_id == i])
                    for i in self.STRUCTURE]
        visits = [Visit(i, sessions=[s for s in sessions
                                     if s.visit == i])
                  for i in visit_ids]
        return Tree(subjects=subjects, visits=visits)

    @unittest.skipIf(*SKIP_ARGS)
    def test_cache_download(self):
        repository = XnatRepository(
            project_id=self.project,
            server=SERVER,
            cache_dir=tempfile.mkdtemp())
        study = self.create_study(
            TestStudy, 'cache_download',
            inputs=[
                FilesetMatch('fileset1', text_format, 'fileset1'),
                FilesetMatch('fileset3', text_format, 'fileset3')],
            repository=repository)
        study.cache_inputs()
        for subject_id, visits in list(self.STRUCTURE.items()):
            subj_dir = op.join(
                repository.cache_dir, self.project,
                '{}_{}'.format(self.project, subject_id))
            for visit_id in visits:
                sess_dir = op.join(
                    subj_dir,
                    '{}_{}_{}'.format(self.project, subject_id,
                                      visit_id))
                for inpt in study.inputs:
                    self.assertTrue(op.exists(op.join(
                        sess_dir, inpt.name + inpt.format.extension)))

    @property
    def base_name(self):
        return self.name


class TestProjectInfo(TestMultiSubjectOnXnatMixin,
                      test_local.TestLocalProjectInfo):

    BASE_CLASS = test_local.TestLocalProjectInfo

    @unittest.skipIf(*SKIP_ARGS)
    def test_project_info(self):
        tree = self.repository.tree()
        ref_tree = self.get_tree(self.repository, set_ids=True)
        self.assertEqual(
            tree, ref_tree,
            "Generated project doesn't match reference:{}"
            .format(tree.find_mismatch(ref_tree)))


class TestConnectDisconnect(TestCase):

    @unittest.skipIf(*SKIP_ARGS)
    def test_connect_disconnect(self):
        repository = XnatRepository(project_id='dummy',
                                    server=SERVER,
                                    cache_dir=tempfile.mkdtemp())
        with repository:
            self._test_open(repository)
        self._test_closed(repository)

        with repository:
            self._test_open(repository)
            with repository:
                self._test_open(repository)
            self._test_open(repository)
        self._test_closed(repository)

    def _test_open(self, repository):
        repository._login.classes  # check connection

    def _test_closed(self, repository):
        self.assertRaises(
            AttributeError,
            getattr,
            repository._login,
            'classes')
