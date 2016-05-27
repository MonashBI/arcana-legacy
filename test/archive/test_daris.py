import os.path
import shutil
import hashlib
from unittest import TestCase
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from neuroanalysis.archive.daris import (
    DarisSession, DarisArchive)
from neuroanalysis.exception import DarisException
from neuroanalysis import Scan, Session
from neuroanalysis.file_formats import nifti_gz_format


# The projects/subjects/studies to alter on DaRIS
SERVER = 'mf-erc.its.monash.edu.au'
REPO_ID = 2
PROJECT_ID = 4
SUBJECT_ID = 12
STUDY_ID = 1
TEST_IMAGE = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', '_data', 'test_image.nii.gz'))


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

    def test_get_files(self):
        files = self._daris.get_files(
            project_id=4, subject_id=1, study_id=3, repo_id=REPO_ID,
            processed=False)
        self.assertEqual(len(files), 8)
        self.assertEqual(
            files[1].name,
            'pd+t2_tse_tra_3mm_C21/PD+T2TSE_tra_640I/3:26/SL3/35sl/FoV220')
        self.assertEqual(
            files[2].name,
            't1_fl2d_tra_3mm_384_C21/T1FLASH_tra_384_pc/2:06/SL3/33sl/FoV220')
        self.assertEqual(
            files[3].name,
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

    def test_add_remove_file(self):
        for processed in (False, True):
            num_files = len(self._daris.get_files(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                study_id=STUDY_ID, processed=processed))
            file_id = self._daris.add_file(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                study_id=STUDY_ID, name='unittest-file',
                repo_id=REPO_ID,
                description=("A file added by a unit-test that should be "
                             "removed by the same test"), processed=processed)
            self._daris.add_file(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                file_id=(file_id + 1),
                study_id=STUDY_ID, name='unittest-file2',
                repo_id=REPO_ID,
                description=("A file added by a unit-test that should be "
                             "removed by the same test"), processed=processed)
            self.assertEqual(
                len(self._daris.get_files(
                    project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                    study_id=STUDY_ID, processed=processed)),
                num_files + 2)
            self._daris.delete_file(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                study_id=STUDY_ID, file_id=file_id,
                repo_id=REPO_ID, processed=processed)
            self._daris.delete_file(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                file_id=(file_id + 1), repo_id=REPO_ID,
                study_id=STUDY_ID, processed=processed)
            self.assertEqual(
                num_files,
                len(self._daris.get_files(
                    project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                    study_id=STUDY_ID, processed=processed)))

    def test_upload_download(self):
        file_id = self._daris.add_file(
            project_id=PROJECT_ID, subject_id=SUBJECT_ID,
            study_id=STUDY_ID, name='unittest-upload',
            repo_id=REPO_ID,
            description=(
                "A file added by a unit-test for testing the "
                "upload/download functionality that should be "
                "removed by the same test"), processed=True)
        try:
            self._daris.upload(
                TEST_IMAGE, project_id=PROJECT_ID,
                subject_id=SUBJECT_ID, study_id=STUDY_ID,
                file_id=file_id, repo_id=REPO_ID, processed=True)
            self._daris.download(
                TEST_IMAGE + '.dnld', project_id=PROJECT_ID,
                subject_id=SUBJECT_ID, study_id=STUDY_ID,
                file_id=file_id, repo_id=REPO_ID, processed=True)
            self.assertEqual(
                hashlib.md5(open(TEST_IMAGE, 'rb').read()).hexdigest(),
                hashlib.md5(
                    open(TEST_IMAGE + '.dnld', 'rb').read()).hexdigest())
        finally:
            # Remove file
            self._daris.delete_file(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                study_id=STUDY_ID, file_id=file_id,
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
        shutil.rmtree(self.token_path, ignore_errors=True)

    # FIXME: Token authentication is not working. Need to double check how
    # Parnesh did it
#     def test_create_token_and_login(self):
#         DarisSession(user='test123', password='GaryEgan1', domain='mon-daris', @IgnorePep8
#                      server=SERVER, token_path=self.token_path,
#                      app_name='unittest').open()
#         with DarisSession(token_path=self.token_path,
#                           app_name='unittest') as daris:
#             self.assertTrue(len(daris.list_projects))


class TestDarisArchive(TestCase):
    TEST_DIR = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '..', '_data', 'daris'))
    CACHE_DIR = os.path.abspath(os.path.join(TEST_DIR, 'cache_dir'))
    WORKFLOW_DIR = os.path.abspath(os.path.join(TEST_DIR, 'workflow_dir'))
    DOMAIN = 'mon-daris'
    USER = 'test123'
    PASSWORD = 'GaryEgan1'

    def setUp(self):
        # Create test data on DaRIS
        self._study_id = None
        self.daris = DarisSession(user='test123', password='GaryEgan1',
                                  domain='mon-daris', server=SERVER)
        # Make cache and working dirs
        shutil.rmtree(self.TEST_DIR, ignore_errors=True)
        os.makedirs(self.CACHE_DIR)
        os.makedirs(self.WORKFLOW_DIR)
        # Upload test study
        with self.daris:  # Opens the daris session
            self.study_id = self.daris.add_study(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                processed=False, name='source-sink-unittest-study',
                description="Used in DarisSource/Sink unittest")
            for name in ('source1.nii.gz', 'source2.nii.gz', 'source3.nii.gz',
                         'source4.nii.gz'):
                file_id = self.daris.add_file(
                    project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                    study_id=self.study_id, processed=False,
                    name=name, description=(
                        "A file added for DarisSink/Source unittest"))
                self.daris.upload(TEST_IMAGE, project_id=PROJECT_ID,
                                  subject_id=SUBJECT_ID,
                                  study_id=self.study_id,
                                  processed=False, file_id=file_id)

    def tearDown(self):
        # Clean up working dirs
        shutil.rmtree(self.TEST_DIR, ignore_errors=True)
        # Clean up study created for unit-test
        if self.study_id is not None:
            try:
                with self.daris:
                    self.daris.delete_study(
                        project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                        processed=False, study_id=self.study_id)
                    self.daris.delete_study(
                        project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                        processed=True, study_id=self.study_id)
            except DarisException:
                pass

    def test_archive_roundtrip(self):

        # Create working dirs
        # Create DarisSource node
        archive = DarisArchive(
            server=SERVER, repo_id=REPO_ID,
            cache_dir=self.CACHE_DIR, domain=self.DOMAIN,
            user=self.USER, password=self.PASSWORD)
        source_files = [Scan('source1', nifti_gz_format),
                        Scan('source2', nifti_gz_format),
                        Scan('source3', nifti_gz_format),
                        Scan('source4', nifti_gz_format)]
        inputnode = pe.Node(IdentityInterface(['session']), 'inputnode')
        session = Session(SUBJECT_ID, self.study_id)
        inputnode.inputs.session = (session.subject_id, session.study_id)
        source = archive.source(PROJECT_ID, source_files)
        sink = archive.sink(PROJECT_ID)
        sink.inputs.name = 'archive-roundtrip-unittest'
        sink.inputs.description = (
            "A test study created by archive roundtrip unittest")
        # Create workflow connecting them together
        workflow = pe.Workflow('source-sink-unit-test',
                               base_dir=self.WORKFLOW_DIR)
        workflow.add_nodes((source, sink))
        workflow.connect(inputnode, 'session', source, 'session')
        workflow.connect(inputnode, 'session', sink, 'session')
        for source_file in source_files:
            if source_file.name != 'source2':
                sink_filename = source_file.filename.replace('source', 'sink')
                workflow.connect(source, source_file.filename,
                                 sink, sink_filename)
        workflow.run()
        # Check cache was created properly
        source_cache_dir = os.path.join(
            self.CACHE_DIR, str(REPO_ID), str(PROJECT_ID), str(SUBJECT_ID),
            '1', str(self.study_id))
        sink_cache_dir = os.path.join(
            self.CACHE_DIR, str(REPO_ID), str(PROJECT_ID), str(SUBJECT_ID),
            '2', str(self.study_id))
        self.assertEqual(sorted(os.listdir(source_cache_dir)),
                         ['source1.nii.gz', 'source2.nii.gz',
                          'source3.nii.gz', 'source4.nii.gz'])
        self.assertEqual(sorted(os.listdir(sink_cache_dir)),
                         ['sink1.nii.gz', 'sink3.nii.gz', 'sink4.nii.gz'])
        with self.daris:
            files = self.daris.get_files(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                study_id=self.study_id, processed=True, repo_id=REPO_ID)
        self.assertEqual(sorted(d.name for d in files.itervalues()),
                         ['sink1.nii.gz', 'sink3.nii.gz', 'sink4.nii.gz'])
