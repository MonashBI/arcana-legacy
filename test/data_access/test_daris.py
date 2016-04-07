import os.path
import shutil
import errno
import hashlib
from unittest import TestCase
from nipype.pipeline import engine as pe
from mbi_pipelines.data_access.daris import (
    DarisSession, DarisSource, DarisSink)

SERVER = 'mf-erc.its.monash.edu.au'

# The projects/subjects/studies to alter on DaRIS
REPO_ID = 2
PROJECT_ID = 4
SUBJECT_ID = 12
STUDY_ID = 1
TEST_IMAGE = os.path.abspath(os.path.join(
    os.path.dirname(__file__), 'test_data', 'test_upload.nii.gz'))
CACHE_DIR = os.path.abspath(os.path.join(
    os.path.dirname(__file__), 'test_data', 'cache_dir'))
WORKFLOW_DIR = os.path.abspath(os.path.join(
    os.path.dirname(__file__), 'test_data', 'workflow_dir'))


class TestDarisSession(TestCase):

    def setUp(self):
        self._daris = DarisSession(user='test123', password='GaryEgan1',
                                   domain='mon-daris', server=SERVER)
        self._daris.open()

    def tearDown(self):
        self._daris.close()

    def test_get_projects(self):
        projects = self._daris.get_projects(repo_id=REPO_ID)
        self.assertEqual(
            len(projects), 4,
            "'test123' account only has access to 4 projects")
        self.assertEqual(projects[4].name, 'Barnes_Test_Area_01')
        self.assertEqual(projects[4].description, "Barnes test area 01")

    def test_get_subjects(self):
        subjects = self._daris.get_subjects(project_id=4, repo_id=REPO_ID)
        self.assertEqual(subjects[1].name, 'db01')
        self.assertEqual(subjects[2].name, 'db02')
        self.assertEqual(subjects[3].name, 'db03')

    def test_get_studies(self):
        studies = self._daris.get_studies(project_id=4, subject_id=1,
                                          repo_id=REPO_ID, processed=False)
        self.assertEqual(len(studies), 3)
        self.assertEqual(studies[1].name, 'Study1')
        self.assertEqual(studies[2].name, 'Study2')
        self.assertEqual(studies[3].name, 'Study3')

    def test_get_datasets(self):
        datasets = self._daris.get_datasets(
            project_id=4, subject_id=1, study_id=3, repo_id=REPO_ID,
            processed=False)
        self.assertEqual(len(datasets), 8)
        self.assertEqual(
            datasets[1].name,
            'pd+t2_tse_tra_3mm_C21/PD+T2TSE_tra_640I/3:26/SL3/35sl/FoV220')
        self.assertEqual(
            datasets[2].name,
            't1_fl2d_tra_3mm_384_C21/T1FLASH_tra_384_pc/2:06/SL3/33sl/FoV220')
        self.assertEqual(
            datasets[3].name,
            't2_blade_tra_p2_C23/T2TSE_B_tra_384/3:18/G2/SL3/35sl/FoV220/'
            'old_infarct')

    def test_add_remove_subjects(self):
        num_subjects = len(self._daris.get_subjects(project_id=PROJECT_ID))
        subject_id = self._daris.add_subject(
            project_id=PROJECT_ID, name='unittest-subject',
            repo_id=REPO_ID,
            description=("A subject added by a unit-test that should be "
                         "removed by the same test"))
        self._daris.add_subject(
            project_id=PROJECT_ID, subject_id=(subject_id + 1),
            name='unittest-subject2', repo_id=REPO_ID,
            description=("A subject added by a unit-test that should be "
                         "removed by the same test"))
        self.assertEqual(
            len(self._daris.get_subjects(project_id=PROJECT_ID)),
            num_subjects + 2)
        self._daris.delete_subject(
            project_id=PROJECT_ID, subject_id=subject_id,
            repo_id=REPO_ID)
        self._daris.delete_subject(
            project_id=PROJECT_ID, subject_id=(subject_id + 1),
            repo_id=REPO_ID)
        self.assertEqual(
            num_subjects,
            len(self._daris.get_subjects(project_id=PROJECT_ID)))

    def test_add_remove_study(self):
        for processed in (False, True):
            num_studies = len(self._daris.get_studies(project_id=PROJECT_ID,
                                                      subject_id=SUBJECT_ID,
                                                      processed=processed))
            study_id = self._daris.add_study(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                name='unittest-study', repo_id=REPO_ID,
                description=("A study added by a unit-test that should be "
                             "removed by the same test"), processed=processed)
            self._daris.add_study(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                study_id=(study_id + 1),
                name='unittest-study2', repo_id=REPO_ID,
                description=("A study added by a unit-test that should be "
                             "removed by the same test"), processed=processed)
            self.assertEqual(
                len(self._daris.get_studies(project_id=PROJECT_ID,
                                            subject_id=SUBJECT_ID,
                                            processed=processed)),
                num_studies + 2)
            self._daris.delete_study(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                study_id=study_id, repo_id=REPO_ID, processed=processed)
            self._daris.delete_study(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                study_id=(study_id + 1), repo_id=REPO_ID,
                processed=processed)
            self.assertEqual(
                num_studies,
                len(self._daris.get_studies(project_id=PROJECT_ID,
                                            subject_id=SUBJECT_ID,
                                            processed=processed)))

    def test_add_remove_dataset(self):
        for processed in (False, True):
            num_datasets = len(self._daris.get_datasets(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                study_id=STUDY_ID, processed=processed))
            dataset_id = self._daris.add_dataset(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                study_id=STUDY_ID, name='unittest-dataset',
                repo_id=REPO_ID,
                description=("A dataset added by a unit-test that should be "
                             "removed by the same test"), processed=processed)
            self._daris.add_dataset(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                dataset_id=(dataset_id + 1),
                study_id=STUDY_ID, name='unittest-dataset2',
                repo_id=REPO_ID,
                description=("A dataset added by a unit-test that should be "
                             "removed by the same test"), processed=processed)
            self.assertEqual(
                len(self._daris.get_datasets(
                    project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                    study_id=STUDY_ID, processed=processed)),
                num_datasets + 2)
            self._daris.delete_dataset(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                study_id=STUDY_ID, dataset_id=dataset_id,
                repo_id=REPO_ID, processed=processed)
            self._daris.delete_dataset(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                dataset_id=(dataset_id + 1), repo_id=REPO_ID,
                study_id=STUDY_ID, processed=processed)
            self.assertEqual(
                num_datasets,
                len(self._daris.get_datasets(
                    project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                    study_id=STUDY_ID, processed=processed)))

    def test_upload_download(self):
        dataset_id = self._daris.add_dataset(
            project_id=PROJECT_ID, subject_id=SUBJECT_ID,
            study_id=STUDY_ID, name='unittest-upload',
            repo_id=REPO_ID,
            description=(
                "A dataset added by a unit-test for testing the "
                "upload/download functionality that should be "
                "removed by the same test"), processed=True)
        try:
            self._daris.upload(
                TEST_IMAGE, project_id=PROJECT_ID,
                subject_id=SUBJECT_ID, study_id=STUDY_ID,
                dataset_id=dataset_id, repo_id=REPO_ID, processed=True)
            self._daris.download(
                TEST_IMAGE + '.dnld', project_id=PROJECT_ID,
                subject_id=SUBJECT_ID, study_id=STUDY_ID,
                dataset_id=dataset_id, repo_id=REPO_ID, processed=True)
            self.assertEqual(
                hashlib.md5(open(TEST_IMAGE, 'rb').read()).hexdigest(),
                hashlib.md5(
                    open(TEST_IMAGE + '.dnld', 'rb').read()).hexdigest())
        finally:
            # Remove dataset
            self._daris.delete_dataset(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                study_id=STUDY_ID, dataset_id=dataset_id,
                repo_id=REPO_ID, processed=True)
            try:
                # Clean up downloaded file
                os.remove(TEST_IMAGE + '.dnld')
            except OSError:
                pass  # Ignore if download wasn't created


class TestDarisToken(TestCase):

    token_path = os.path.join(os.path.dirname(__file__), 'test_daris_token')

    def tearDown(self):
        # Remove token_path if present
        rmtree_ignore_missing(self.token_path)

    # FIXME: Token authentication is not working. Need to double check how
    # Parnesh did it
#     def test_create_token_and_login(self):
#         DarisSession(user='test123', password='GaryEgan1', domain='mon-daris',
#                      server=SERVER, token_path=self.token_path,
#                      app_name='unittest').open()
#         with DarisSession(token_path=self.token_path,
#                           app_name='unittest') as daris:
#             self.assertTrue(len(daris.list_projects))


class TestDarisSinkAndSource(TestCase):

    def test_daris_roundtrip(self):
        # Create test data on DaRIS
        daris = DarisSession(user='test123', password='GaryEgan1',
                             domain='mon-daris', server=SERVER)
        with daris:
            study_id = daris.add_study(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                processed=False, name='source-sink-unittest-study',
                description="Used in DarisSource/Sink unittest")
            for name in ('source1', 'source2', 'source3', 'source4'):
                dataset_id = daris.add_dataset(
                    project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                    study_id=study_id, processed=False,
                    name=name, description=(
                        "A dataset added for DarisSink/Source unittest"))
                daris.upload(TEST_IMAGE, project_id=PROJECT_ID,
                             subject_id=SUBJECT_ID, study_id=study_id,
                             processed=False, dataset_id=dataset_id)
        try:
            # Create working dirs
            rmtree_ignore_missing(CACHE_DIR)
            rmtree_ignore_missing(WORKFLOW_DIR)
            os.makedirs(CACHE_DIR)
            os.makedirs(WORKFLOW_DIR)
            # Create DarisSource node
            source = pe.Node(DarisSource(), 'source')
            source.inputs.project_id = PROJECT_ID
            source.inputs.subject_id = SUBJECT_ID
            source.inputs.study_id = study_id
            source.inputs.server = SERVER
            source.inputs.processed = False
            source.inputs.repo_id = REPO_ID
            source.inputs.cache_dir = CACHE_DIR
            source.inputs.domain = 'mon-daris'
            source.inputs.user = 'test123'
            source.inputs.password = 'GaryEgan1'
            source.inputs.dataset_names = ['source1', 'source2', 'source3',
                                           'source4']
            # Create DataSink node
            sink = pe.Node(DarisSink(), 'sink')
            sink.inputs.name = 'unittest_study'
            sink.inputs.description = (
                "A study created by the soure-sink unittest")
            sink.inputs.project_id = PROJECT_ID
            sink.inputs.subject_id = SUBJECT_ID
            sink.inputs.server = SERVER
            sink.inputs.repo_id = REPO_ID
            sink.inputs.cache_dir = CACHE_DIR
            sink.inputs.domain = 'mon-daris'
            sink.inputs.user = 'test123'
            sink.inputs.password = 'GaryEgan1'
            # Create workflow connecting them together
            workflow = pe.Workflow('source-sink-unit-test',
                                   base_dir=WORKFLOW_DIR)
            workflow.add_nodes((source, sink))
            workflow.connect([(source, sink,
                               (('source1', 'sink1'), ('source3', 'sink3'),
                                ('source4', 'sink4')))])
            workflow.run()
            # Check cache was created properly
            source_cache_dir = os.path.join(
                CACHE_DIR, str(REPO_ID), str(PROJECT_ID), str(SUBJECT_ID),
                '1', str(study_id))
            sink_cache_dir = os.path.join(
                CACHE_DIR, str(REPO_ID), str(PROJECT_ID), str(SUBJECT_ID),
                '2', str(study_id))
            self.assertEqual(sorted(os.listdir(source_cache_dir)),
                             ['source1', 'source2', 'source3', 'source4'])
            self.assertEqual(sorted(os.listdir(sink_cache_dir)),
                             ['sink1', 'sink3', 'sink4'])
            with daris:
                datasets = daris.get_datasets(
                    project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                    study_id=study_id, processed=True, repo_id=REPO_ID)
            self.assertEqual(sorted(d.name for d in datasets.itervalues()),
                             ['sink1', 'sink3', 'sink4'])
        finally:
            # Clean up working dirs
            shutil.rmtree(CACHE_DIR, ignore_errors=True)
            shutil.rmtree(WORKFLOW_DIR, ignore_errors=True)
            # Clean up study created for unit-test
            with daris:
                daris.delete_study(
                    project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                    processed=False, study_id=study_id)
                daris.delete_study(
                    project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                    processed=True, study_id=study_id)


def rmtree_ignore_missing(directory):
    try:
        shutil.rmtree(directory)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
