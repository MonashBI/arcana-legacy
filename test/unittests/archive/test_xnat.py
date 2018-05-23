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
from arcana.archive.xnat import (
    XnatArchive, download_all_datasets)
from arcana.archive.local import LocalArchive
from arcana.study import Study, StudyMetaClass
from arcana.runner import LinearRunner
from arcana.dataset import (
    DatasetMatch, DatasetSpec, FieldSpec)
from arcana.data_format import DataFormat
from arcana.utils import PATH_SUFFIX
from arcana.exception import ArcanaError
from arcana.data_format import text_format
import sys
import logging
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


logger = logging.getLogger('Arcana')

dicom_format = DataFormat(name='dicom', extension=None,
                          directory=True, within_dir_exts=['.dcm'])

try:
    SERVER = os.environ['ARCANA_TEST_XNAT']
except KeyError:
    SERVER = None
SERVER = 'https://mbi-xnat.erc.monash.edu.au'


class DummyStudy(Study):

    __metaclass__ = StudyMetaClass

    add_data_specs = [
        DatasetSpec('source1', text_format),
        DatasetSpec('source2', text_format),
        DatasetSpec('source3', text_format),
        DatasetSpec('source4', text_format),
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
        DatasetSpec('resink3', text_format, 'dummy_pipeline')]

    def dummy_pipeline(self):
        pass


def filter_md5_fnames(fnames):
    return [f for f in sorted(fnames)
            if not f.endswith(XnatArchive.MD5_SUFFIX)]


class LockXnatProjectTestCase(BaseTestCase):
    """
    Provides methods to lock the use of an XNAT project used for testing
    the XnatArchive, to avoid conflicts between multiple runs of the
    same unittests
    """

    def setUp(self):
        super(LockXnatProjectTestCase, self).setUp()

    def tearDown(self):
        super(LockXnatProjectTestCase, self).tearDown()


class TestXnatArchive(BaseTestCase):

    PROJECT = 'TEST002'
    SUBJECT = '001'
    DIGEST_SINK_PROJECT = 'TEST009'
    DIGEST_SINK_SUBJECT = '001'
    VISIT = 'MR01'
    STUDY_NAME = 'astudy'
    SUMMARY_STUDY_NAME = 'asummary'

    INPUT_DATASETS = {'source1': 'foo', 'source2': 'bar',
                      'source3': 'wee', 'source4': 'wa'}

    def session_label(self, project=None, subject=None, visit=None):
        if project is None:
            project = self.PROJECT
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
            project = self.PROJECT
        if subject is None:
            subject = self.SUBJECT
        return os.path.join(
            base_dir, project, '{}_{}'.format(project, subject),
            self.session_label(project=project, subject=subject,
                               visit=visit))

    def proc_session_cache(self, *args, **kwargs):
        return self.session_cache(
            *args, **kwargs) + XnatArchive.PROCESSED_SUFFIX

    def setUp(self):
        super(TestXnatArchive, self).setUp()
        shutil.rmtree(self.cache_dir, ignore_errors=True)
        os.makedirs(self.cache_dir)
        self._delete_test_subjects()
        with self._connect() as mbi_xnat:
            xproject = mbi_xnat.projects[self.PROJECT]
            xsubject = mbi_xnat.classes.SubjectData(
                label='{}_{}'.format(self.PROJECT, self.SUBJECT),
                parent=xproject)
            xsession = mbi_xnat.classes.MrSessionData(
                label=self.session_label(),
                parent=xsubject)
            for dataset in self.session.datasets:
                xdataset = mbi_xnat.classes.MrScanData(
                    type=dataset.name, parent=xsession)
                resource = xdataset.create_resource(
                    dataset.format.name.upper())
                resource.upload(dataset.path, dataset.fname())

    def tearDown(self):
        # Clean up working dirs
        shutil.rmtree(self.cache_dir, ignore_errors=True)
        # Clean up session created for unit-test
        self._delete_test_subjects()

    def _delete_test_subjects(self):
        with self._connect() as mbi_xnat:
            project = mbi_xnat.projects[self.PROJECT]
            subject = '{}_{}'.format(self.PROJECT, self.SUBJECT)
            if subject in project.subjects:
                project.subjects[subject].delete()
            project_summary_name = (self.PROJECT + '_' +
                                    XnatArchive.SUMMARY_NAME)
            if project_summary_name in project.subjects:
                project.subjects[project_summary_name].delete()

    def _connect(self):
        return xnat.connect(SERVER)

    @unittest.skipIf(SERVER is None, "ARCANA_TEST_XNAT env var not set")
    def test_archive_roundtrip(self):

        # Create working dirs
        # Create DarisSource node
        archive = XnatArchive(
            project_id=self.PROJECT,
            server=SERVER, cache_dir=self.cache_dir)
        study = DummyStudy(
            self.STUDY_NAME, archive, runner=LinearRunner('a_dir'),
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
        source = archive.source(source_files,
                                study_name=self.STUDY_NAME)
        sink = archive.sink(sink_files,
                            study_name=self.STUDY_NAME)
        sink.inputs.name = 'archive-roundtrip-unittest'
        sink.inputs.desc = (
            "A test session created by archive roundtrip unittest")
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
        self.assertEqual(filter_md5_fnames(os.listdir(self.session_cache())),
                         ['source1.txt', 'source2.txt',
                          'source3.txt', 'source4.txt'])
        expected_sink_datasets = [self.STUDY_NAME + '_sink1',
                                  self.STUDY_NAME + '_sink3',
                                  self.STUDY_NAME + '_sink4']
        self.assertEqual(
            filter_md5_fnames(os.listdir(self.proc_session_cache())),
            [d + text_format.extension
             for d in expected_sink_datasets])
        with self._connect() as mbi_xnat:
            dataset_names = mbi_xnat.experiments[
                self.session_label() +
                XnatArchive.PROCESSED_SUFFIX].scans.keys()
        self.assertEqual(sorted(dataset_names), expected_sink_datasets)

    @unittest.skipIf(SERVER is None, "ARCANA_TEST_XNAT env var not set")
    def test_fields_roundtrip(self):
        archive = XnatArchive(
            server=SERVER, cache_dir=self.archive_cache_dir,
            project_id=self.PROJECT)
        sink = archive.sink(
            outputs=[
                FieldSpec('field1', int, 'pipeline'),
                FieldSpec('field2', float, 'pipeline'),
                FieldSpec('field3', str, 'pipeline')],
            name='fields_sink',
            study_name='test')
        sink.inputs.field1_field = field1 = 1
        sink.inputs.field2_field = field2 = 2.0
        sink.inputs.field3_field = field3 = '3'
        sink.inputs.subject_id = self.SUBJECT
        sink.inputs.visit_id = self.VISIT
        sink.inputs.desc = "Test sink of fields"
        sink.inputs.name = 'test_sink'
        sink.run()
        source = archive.source(
            inputs=[
                FieldSpec('field1', int, 'dummy_pipeline'),
                FieldSpec('field2', float, 'dummy_pipeline'),
                FieldSpec('field3', str, 'dummy_pipeline')],
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

    @unittest.skipIf(SERVER is None, "ARCANA_TEST_XNAT env var not set")
    def test_summary(self):
        # Create working dirs
        # Create XnatSource node
        archive = XnatArchive(
            server=SERVER, cache_dir=self.archive_cache_dir,
            project_id=self.PROJECT)
        study = DummyStudy(
            self.SUMMARY_STUDY_NAME, archive, LinearRunner('ad'),
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
        source = archive.source(source_files)
        subject_sink_files = [
            study.spec('subject_sink')]
        subject_sink = archive.sink(subject_sink_files,
                                    frequency='per_subject',
                                    study_name=self.SUMMARY_STUDY_NAME)
        subject_sink.inputs.name = 'subject_summary'
        subject_sink.inputs.desc = (
            "Tests the sinking of subject-wide datasets")
        # Test visit sink
        visit_sink_files = [study.spec('visit_sink')]
        visit_sink = archive.sink(visit_sink_files,
                                  frequency='per_visit',
                                  study_name=self.SUMMARY_STUDY_NAME)
        visit_sink.inputs.name = 'visit_summary'
        visit_sink.inputs.desc = (
            "Tests the sinking of visit-wide datasets")
        # Test project sink
        project_sink_files = [
            study.spec('project_sink')]
        project_sink = archive.sink(project_sink_files,
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
                self.archive_cache_dir, self.PROJECT,
                '_'.join((self.PROJECT, self.SUBJECT)),
                '_'.join((self.PROJECT, self.SUBJECT,
                         XnatArchive.SUMMARY_NAME)))
            self.assertEqual(filter_md5_fnames(os.listdir(subject_dir)),
                             [d + text_format.extension
                              for d in expected_subj_datasets])
            # and on XNAT
            subject_dataset_names = mbi_xnat.projects[
                self.PROJECT].experiments[
                    '_'.join((self.PROJECT, self.SUBJECT,
                              XnatArchive.SUMMARY_NAME))].scans.keys()
            self.assertEqual(expected_subj_datasets, subject_dataset_names)
            # Check visit summary directories were created properly in
            # cache
            expected_visit_datasets = [self.SUMMARY_STUDY_NAME +
                                       '_visit_sink']
            visit_dir = os.path.join(
                self.archive_cache_dir, self.PROJECT,
                self.PROJECT + '_' + XnatArchive.SUMMARY_NAME,
                (self.PROJECT + '_' + XnatArchive.SUMMARY_NAME +
                 '_' + self.VISIT))
            self.assertEqual(filter_md5_fnames(os.listdir(visit_dir)),
                             [d + text_format.extension
                              for d in expected_visit_datasets])
            # and on XNAT
            visit_dataset_names = mbi_xnat.projects[
                self.PROJECT].experiments[
                    '{}_{}_{}'.format(
                        self.PROJECT, XnatArchive.SUMMARY_NAME,
                        self.VISIT)].scans.keys()
            self.assertEqual(expected_visit_datasets, visit_dataset_names)
            # Check project summary directories were created properly in cache
            expected_proj_datasets = [self.SUMMARY_STUDY_NAME +
                                      '_project_sink']
            project_dir = os.path.join(
                self.archive_cache_dir, self.PROJECT,
                self.PROJECT + '_' + XnatArchive.SUMMARY_NAME,
                self.PROJECT + '_' + XnatArchive.SUMMARY_NAME + '_' +
                XnatArchive.SUMMARY_NAME)
            self.assertEqual(filter_md5_fnames(os.listdir(project_dir)),
                             [d + text_format.extension
                              for d in expected_proj_datasets])
            # and on XNAT
            project_dataset_names = mbi_xnat.projects[
                self.PROJECT].experiments[
                    '{}_{sum}_{sum}'.format(
                        self.PROJECT,
                        sum=XnatArchive.SUMMARY_NAME)].scans.keys()
            self.assertEqual(expected_proj_datasets, project_dataset_names)
        # Reload the data from the summary directories
        reloadinputnode = pe.Node(IdentityInterface(['subject_id',
                                                     'visit_id']),
                                  'reload_inputnode')
        reloadinputnode.inputs.subject_id = self.SUBJECT
        reloadinputnode.inputs.visit_id = self.VISIT
        reloadsource = archive.source(
            (source_files + subject_sink_files + visit_sink_files +
             project_sink_files),
            name='reload_source',
            study_name=self.SUMMARY_STUDY_NAME)
        reloadsink = archive.sink(
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
            filter_md5_fnames(os.listdir(self.proc_session_cache())),
            [self.SUMMARY_STUDY_NAME + '_resink1.txt',
             self.SUMMARY_STUDY_NAME + '_resink2.txt',
             self.SUMMARY_STUDY_NAME + '_resink3.txt'])
        # and on XNAT
        with self._connect() as mbi_xnat:
            resinked_dataset_names = mbi_xnat.projects[
                self.PROJECT].experiments[
                    self.session_label() +
                    XnatArchive.PROCESSED_SUFFIX].scans.keys()
            self.assertEqual(sorted(resinked_dataset_names),
                             [self.SUMMARY_STUDY_NAME + '_resink1',
                              self.SUMMARY_STUDY_NAME + '_resink2',
                              self.SUMMARY_STUDY_NAME + '_resink3'])

    @unittest.skipIf(SERVER is None, "ARCANA_TEST_XNAT env var not set")
    def test_delayed_download(self):
        """
        Tests handling of race conditions where separate processes attempt to
        cache the same dataset
        """
        cache_dir = os.path.join(self.base_cache_path,
                                 'delayed-download-cache')
        DATASET_NAME = 'source1'
        target_path = os.path.join(self.session_cache(cache_dir),
                                   DATASET_NAME + text_format.extension)
        tmp_dir = target_path + '.download'
        shutil.rmtree(cache_dir, ignore_errors=True)
        os.makedirs(cache_dir)
        archive = XnatArchive(server=SERVER, cache_dir=cache_dir,
                              project_id=self.PROJECT)
        study = DummyStudy(
            self.STUDY_NAME, archive, LinearRunner('ad'),
            inputs=[DatasetMatch(DATASET_NAME, text_format,
                                 DATASET_NAME)])
        source = archive.source([study.input(DATASET_NAME)],
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

    @unittest.skipIf(SERVER is None, "ARCANA_TEST_XNAT env var not set")
    def test_digest_check(self):
        """
        Tests check of downloaded digests to see if file needs to be
        redownloaded
        """
        cache_dir = os.path.join(self.base_cache_path,
                                 'digest-check-cache')
        DATASET_NAME = 'source1'
        STUDY_NAME = 'digest_check_study'
        dataset_fpath = DATASET_NAME + text_format.extension
        source_target_path = os.path.join(self.session_cache(cache_dir),
                                          dataset_fpath)
        md5_path = source_target_path + XnatArchive.MD5_SUFFIX
        shutil.rmtree(cache_dir, ignore_errors=True)
        os.makedirs(cache_dir)
        archive = XnatArchive(
            project_id=self.PROJECT,
            server=SERVER, cache_dir=cache_dir)
        study = DummyStudy(
            STUDY_NAME, archive, LinearRunner('ad'),
            inputs=[DatasetMatch(DATASET_NAME, text_format,
                                 DATASET_NAME)])
        source = archive.source([study.input(DATASET_NAME)],
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
        with open(md5_path, 'w') as f:
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
        sink_archive = XnatArchive(
            project_id=self.DIGEST_SINK_PROJECT, server=SERVER,
            cache_dir=cache_dir)
        DATASET_NAME = 'sink1'
        sink = sink_archive.sink(
            [study.spec(DATASET_NAME)],
            name='digest_check_sink',
            study_name=STUDY_NAME)
        sink.inputs.name = 'digest_check_sink'
        sink.inputs.desc = "Tests the generation of MD5 digests"
        sink.inputs.subject_id = self.DIGEST_SINK_SUBJECT
        sink.inputs.visit_id = self.VISIT
        sink.inputs.sink1_path = source_target_path
        sink_fpath = (STUDY_NAME + '_' + DATASET_NAME +
                      text_format.extension)
        sink_target_path = os.path.join(
            (self.session_cache(
                cache_dir, project=self.DIGEST_SINK_PROJECT,
                subject=(self.DIGEST_SINK_SUBJECT)) +
             XnatArchive.PROCESSED_SUFFIX),
            sink_fpath)
        sink_md5_path = sink_target_path + XnatArchive.MD5_SUFFIX
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


class TestXnatArchiveSpecialCharInScanName(TestCase):

    PROJECT = 'MRH033'
    SUBJECT = '001'
    VISIT = 'MR01'
    TEST_NAME = 'special_char_in_scan_name'
    DATASETS = ['localizer 3 PLANES (Left)',
                'PosDisp: [3] cv_t1rho_3D_2_TR450 (Left)']
    work_path = os.path.join(BaseTestCase.test_data_dir, 'work',
                             TEST_NAME)

    @unittest.skipIf(SERVER is None, "ARCANA_TEST_XNAT env var not set")
    def test_special_char_in_scan_name(self):
        """
        Tests whether XNAT source can download files with spaces in their names
        """
        cache_dir = tempfile.mkdtemp()
        archive = XnatArchive(
            server=SERVER, cache_dir=cache_dir,
            project_id=self.PROJECT)
        study = DummyStudy(
            'study', archive, LinearRunner('ad'),
            inputs=[DatasetMatch('source{}'.format(i), text_format, d)
                    for i, d in enumerate(self.DATASETS, start=1)],
            subject_ids=[self.SUBJECT], visit_ids=[self.VISIT])
        source = archive.source(
            [study.input('source{}'.format(i))
             for i in range(1, len(self.DATASETS) + 1)])
        source.inputs.subject_id = self.SUBJECT
        source.inputs.visit_id = self.VISIT
        workflow = pe.Workflow(self.TEST_NAME, base_dir=self.work_path)
        workflow.add_nodes([source])
        graph = workflow.run()
        result = next(n.result for n in graph.nodes() if n.name == source.name)
        for i, dname in enumerate(self.DATASETS, start=1):
            path = getattr(result.outputs,
                           'source{}{}'.format(i, PATH_SUFFIX))
            self.assertEqual(os.path.basename(path), dname)
            self.assertTrue(os.path.exists(path))


class TestOnXnatMixin(object):

    sanitize_id_re = re.compile(r'[^a-zA-Z_0-9]')
    # Used to specify datasets that should be put in the derived
    # session
#     DERIVED_SUFFIX = '_derived'

    def setUp(self):
        self._clean_up()
        self._archive = XnatArchive(project_id=self.project_id,
                                    server=SERVER,
                                    cache_dir=self.cache_dir)
        self.BASE_CLASS.setUp(self)
        local_archive = LocalArchive(self.project_dir)
        project = local_archive.get_tree()
        with xnat.connect(SERVER) as xnat_login:
            # Copy local archive to XNAT
            xproject = xnat_login.projects[self.PROJECT]
            for subject in project.subjects:
                subj_id = self.PROJECT + '_' + subject.id
                xsubject = xnat_login.classes.SubjectData(
                    label=subj_id, parent=xproject)
                for session in subject.sessions:
                    sess_id = subj_id + '_' + session.visit_id
                    xsession = xnat_login.classes.MrSessionData(
                        label=sess_id,
                        parent=xsubject)
                    if any(self._is_derived(d) for d in session.datasets):
                        xsession_proc = xnat_login.classes.MrSessionData(
                            label=sess_id + XnatArchive.PROCESSED_SUFFIX,
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
                        label=XnatArchive.get_labels(
                            'per_subject', self.PROJECT,
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
                     summ_sess_name) = XnatArchive.get_labels(
                        'per_visit', self.PROJECT,
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
                 summ_sess_name) = XnatArchive.get_labels(
                    'per_project', self.PROJECT)
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

    def _clean_up(self):
        # Clean up working dirs
        shutil.rmtree(self.cache_dir, ignore_errors=True)
        # Clean up session created for unit-test
        with xnat.connect(SERVER) as mbi_xnat:
            xproject = mbi_xnat.projects[self.PROJECT]
            for xsubject in list(xproject.subjects.itervalues()):
                xsubject.delete()

    @property
    def archive(self):
        return self._archive

    @property
    def xnat_session_name(self):
        return '{}_{}'.format(self.XNAT_TEST_PROJECT, self.base_name)

    @property
    def project_dir(self):
        return os.path.join(self.archive_path, self.base_name)

    @property
    def output_cache_dir(self):
        return self._output_cache_dir

    @property
    def base_name(self):
        return self._get_name(self.BASE_CLASS)

    @property
    def project_id(self):
        return self.PROJECT

    def _full_subject_id(self, subject):
        return self.PROJECT + '_' + subject

    def _proc_sess_id(self, session):
        return session + XnatArchive.PROCESSED_SUFFIX

    def get_session_dir(self, subject=None, visit=None,
                        frequency='per_session', derived=False):
        if subject is None and frequency in ('per_session', 'per_subject'):
            subject = self.SUBJECT
        if visit is None and frequency in ('per_session', 'per_visit'):
            visit = self.VISIT
        if frequency == 'per_session':
            assert subject is not None
            assert visit is not None
            parts = [self.PROJECT, subject, visit]
        elif frequency == 'per_subject':
            assert subject is not None
            assert visit is None
            parts = [self.PROJECT, subject, XnatArchive.SUMMARY_NAME]
        elif frequency == 'per_visit':
            assert visit is not None
            assert subject is None
            parts = [self.PROJECT, XnatArchive.SUMMARY_NAME, visit]
        elif frequency == 'per_project':
            assert subject is None
            assert visit is None
            parts = [self.PROJECT, XnatArchive.SUMMARY_NAME,
                     XnatArchive.SUMMARY_NAME]
        else:
            assert False
        session_id = '_'.join(parts)
        if derived:
            session_id += XnatArchive.PROCESSED_SUFFIX
        session_path = os.path.join(self.output_cache_dir, session_id)
        if not os.path.exists(session_path):
            download_all_datasets(session_path, SERVER, session_id)
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


class TestExistingPrereqsOnXnat(TestOnXnatMixin,
                                test_study.TestExistingPrereqs):

    PROJECT = 'TEST007'
    BASE_CLASS = test_study.TestExistingPrereqs

    @unittest.skipIf(SERVER is None, "ARCANA_TEST_XNAT env var not set")
    def test_per_session_prereqs(self):
        super(TestExistingPrereqsOnXnat, self).test_per_session_prereqs()


class TestStudy(Study):

    __metaclass__ = StudyMetaClass

    add_data_specs = [
        DatasetSpec('dataset1', text_format),
        DatasetSpec('dataset2', text_format),
        DatasetSpec('dataset3', text_format),
        DatasetSpec('dataset5', text_format)]


class TestXnatCache(TestOnXnatMixin, BaseMultiSubjectTestCase):

    PROJECT = 'TEST011'
    BASE_CLASS = BaseMultiSubjectTestCase
    SUBJECTS = ['subject1', 'subject3', 'subject4']
    VISITS = ['visit1']

    @unittest.skipIf(SERVER is None, "ARCANA_TEST_XNAT env var not set")
    def test_cache_download(self):
        archive = XnatArchive(project_id=self.project_id,
                              server=SERVER,
                              cache_dir=tempfile.mkdtemp())
        study = self.create_study(
            TestStudy, 'cache_download',
            inputs=[
                DatasetMatch('dataset1', text_format, 'dataset1'),
                DatasetMatch('dataset2', text_format, 'dataset2'),
                DatasetMatch('dataset3', text_format, 'dataset3'),
                DatasetMatch('dataset5', text_format, 'dataset5')],
            archive=archive)
        study.cache_inputs()
        for subject_id in self.SUBJECTS:
            for inpt in study.inputs:
                self.assertTrue(
                    os.path.exists(os.path.join(
                        archive.cache_dir, self.PROJECT,
                        '{}_{}'.format(self.PROJECT, subject_id),
                        '{}_{}_{}'.format(self.PROJECT, subject_id,
                                          self.VISITS[0]),
                        inpt.fname())))

    @property
    def base_name(self):
        return self.name


class TestProjectInfo(TestOnXnatMixin,
                      test_local.TestProjectInfo):

    PROJECT = 'TEST013'
    BASE_CLASS = test_local.TestProjectInfo

    @unittest.skipIf(SERVER is None, "ARCANA_TEST_XNAT env var not set")
    def test_project_info(self):
        tree = self.archive.get_tree()
        ref_tree = self.get_tree(self.archive, set_ids=True)
        self.assertEqual(
            tree, ref_tree,
            "Generated project doesn't match reference:{}"
            .format(tree.find_mismatch(ref_tree)))


class TestDicomTagMatchAndIDOnXnat(BaseTestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @unittest.skipIf(SERVER is None, "ARCANA_TEST_XNAT env var not set")
    def test_dicom_match(self):
        study = test_dataset.TestMatchStudy(
            name='test_dicom',
            archive=XnatArchive(
                project_id='TEST001',
                server=SERVER, cache_dir=tempfile.mkdtemp()),
            runner=LinearRunner(self.work_dir),
            inputs=test_dataset.TestDicomTagMatch.DICOM_MATCH,
            subject_ids=['DATASET'], visit_ids=['DICOMTAGMATCH'])
        phase = study.data('gre_phase')[0]
        mag = study.data('gre_mag')[0]
        self.assertEqual(phase.name, 'gre_field_mapping_3mm_phase')
        self.assertEqual(mag.name, 'gre_field_mapping_3mm_mag')

    @unittest.skipIf(SERVER is None, "ARCANA_TEST_XNAT env var not set")
    def test_id_match(self):
        study = test_dataset.TestMatchStudy(
            name='test_dicom',
            archive=XnatArchive(
                project_id='TEST001',
                server=SERVER, cache_dir=tempfile.mkdtemp()),
            runner=LinearRunner(self.work_dir),
            inputs=[
                DatasetMatch('gre_phase', dicom_format, id=8),
                DatasetMatch('gre_mag', dicom_format, id=7)],
            subject_ids=['DATASET'], visit_ids=['DICOMTAGMATCH'])
        phase = study.data('gre_phase')[0]
        mag = study.data('gre_mag')[0]
        self.assertEqual(phase.name, 'gre_field_mapping_3mm_phase')
        self.assertEqual(mag.name, 'gre_field_mapping_3mm_mag')


class TestDatasetCacheOnPathAccess(TestCase):

    PROJECT = 'TEST001'
    DATASET_NAME = 'localizer'
    SUBJECT = 'ARCHIVEXNAT'
    VISIT = 'DATASETCACHEONPATHACCESS'

    @unittest.skipIf(SERVER is None, "ARCANA_TEST_XNAT env var not set")
    def test_cache_on_path_access(self):
        tmp_dir = tempfile.mkdtemp()
        archive = XnatArchive(
            project_id=self.PROJECT,
            server=SERVER, cache_dir=tmp_dir)
        tree = archive.get_tree(
            subject_ids=[self.SUBJECT],
            visit_ids=[self.VISIT])
        # Get a dataset
        dataset = next(next(next(tree.subjects).sessions).datasets)
        self.assertEqual(dataset._path, None)
        target_path = os.path.join(
            tmp_dir, self.PROJECT,
            '{}_{}'.format(self.PROJECT, self.SUBJECT),
            '{}_{}_{}'.format(self.PROJECT, self.SUBJECT, self.VISIT),
            self.DATASET_NAME)
        # This should implicitly download the dataset
        self.assertEqual(dataset.path, target_path)
        self.assertTrue(os.path.exists(target_path))
