import os.path
import errno
import hashlib
from unittest import TestCase
from mbi_pipelines.data_access.daris import DarisSession


class TestDarisSession(TestCase):

    _repo = 2
    _project = 4
    _subject = 12
    _study = 1
    _test_image = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                               'test_upload.nii.gz'))

    def setUp(self):
        self._daris = DarisSession(user='test123', password='GaryEgan1',
                                   domain='mon-daris')
        self._daris.open()

    def tearDown(self):
        self._daris.close()

    def test_get_projects(self):
        projects = self._daris.get_projects(repo_id=self._repo)
        self.assertEqual(
            len(projects), 4,
            "'test123' account only has access to 4 projects")
        self.assertEqual(projects[4].name, 'Barnes_Test_Area_01')
        self.assertEqual(projects[4].description, "Barnes test area 01")

    def test_get_subjects(self):
        subjects = self._daris.get_subjects(project_id=4, repo_id=self._repo)
        self.assertEqual(subjects[1].name, 'db01')
        self.assertEqual(subjects[2].name, 'db02')
        self.assertEqual(subjects[3].name, 'db03')

    def test_get_studies(self):
        studies = self._daris.get_studies(project_id=4, subject_id=1,
                                          repo_id=self._repo, processed=False)
        self.assertEqual(len(studies), 3)
        self.assertEqual(studies[1].name, 'Study1')
        self.assertEqual(studies[2].name, 'Study2')
        self.assertEqual(studies[3].name, 'Study3')

    def test_get_datasets(self):
        datasets = self._daris.get_datasets(
            project_id=4, subject_id=1, study_id=3, repo_id=self._repo,
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
        num_subjects = len(self._daris.get_subjects(project_id=self._project))
        subject_id = self._daris.add_subject(
            project_id=self._project, name='unittest-subject',
            repo_id=self._repo,
            description=("A subject added by a unit-test that should be "
                         "removed by the same test"))
        self._daris.add_subject(
            project_id=self._project, subject_id=(subject_id + 1),
            name='unittest-subject2', repo_id=self._repo,
            description=("A subject added by a unit-test that should be "
                         "removed by the same test"))
        self.assertEqual(
            len(self._daris.get_subjects(project_id=self._project)),
            num_subjects + 2)
        self._daris.delete_subject(
            project_id=self._project, subject_id=subject_id,
            repo_id=self._repo)
        self._daris.delete_subject(
            project_id=self._project, subject_id=(subject_id + 1),
            repo_id=self._repo)
        self.assertEqual(
            num_subjects,
            len(self._daris.get_subjects(project_id=self._project)))

    def test_add_remove_study(self):
        for processed in (False, True):
            num_studies = len(self._daris.get_studies(project_id=self._project,
                                                      subject_id=self._subject,
                                                      processed=processed))
            study_id = self._daris.add_study(
                project_id=self._project, subject_id=self._subject,
                name='unittest-study', repo_id=self._repo,
                description=("A study added by a unit-test that should be "
                             "removed by the same test"), processed=processed)
            self._daris.add_study(
                project_id=self._project, subject_id=self._subject,
                study_id=(study_id + 1),
                name='unittest-study2', repo_id=self._repo,
                description=("A study added by a unit-test that should be "
                             "removed by the same test"), processed=processed)
            self.assertEqual(
                len(self._daris.get_studies(project_id=self._project,
                                            subject_id=self._subject,
                                            processed=processed)),
                num_studies + 2)
            self._daris.delete_study(
                project_id=self._project, subject_id=self._subject,
                study_id=study_id, repo_id=self._repo, processed=processed)
            self._daris.delete_study(
                project_id=self._project, subject_id=self._subject,
                study_id=(study_id + 1), repo_id=self._repo,
                processed=processed)
            self.assertEqual(
                num_studies,
                len(self._daris.get_studies(project_id=self._project,
                                            subject_id=self._subject,
                                            processed=processed)))

    def test_add_remove_dataset(self):
        for processed in (False, True):
            num_datasets = len(self._daris.get_datasets(
                project_id=self._project, subject_id=self._subject,
                study_id=self._study, processed=processed))
            dataset_id = self._daris.add_dataset(
                project_id=self._project, subject_id=self._subject,
                study_id=self._study, name='unittest-dataset',
                repo_id=self._repo,
                description=("A dataset added by a unit-test that should be "
                             "removed by the same test"), processed=processed)
            self._daris.add_dataset(
                project_id=self._project, subject_id=self._subject,
                dataset_id=(dataset_id + 1),
                study_id=self._study, name='unittest-dataset2',
                repo_id=self._repo,
                description=("A dataset added by a unit-test that should be "
                             "removed by the same test"), processed=processed)
            self.assertEqual(
                len(self._daris.get_datasets(
                    project_id=self._project, subject_id=self._subject,
                    study_id=self._study, processed=processed)),
                num_datasets + 2)
            self._daris.delete_dataset(
                project_id=self._project, subject_id=self._subject,
                study_id=self._study, dataset_id=dataset_id,
                repo_id=self._repo, processed=processed)
            self._daris.delete_dataset(
                project_id=self._project, subject_id=self._subject,
                dataset_id=(dataset_id + 1), repo_id=self._repo,
                study_id=self._study, processed=processed)
            self.assertEqual(
                num_datasets,
                len(self._daris.get_datasets(
                    project_id=self._project, subject_id=self._subject,
                    study_id=self._study, processed=processed)))

    def test_upload_download(self):
        dataset_id = self._daris.add_dataset(
            project_id=self._project, subject_id=self._subject,
            study_id=self._study, name='unittest-upload',
            repo_id=self._repo,
            description=(
                "A dataset added by a unit-test for testing the "
                "upload/download functionality that should be "
                "removed by the same test"), processed=True)
        try:
            self._daris.upload(
                self._test_image, project_id=self._project,
                subject_id=self._subject, study_id=self._study,
                dataset_id=dataset_id, repo_id=self._repo, processed=True)
            self._daris.download(
                self._test_image + '.dnld', project_id=self._project,
                subject_id=self._subject, study_id=self._study,
                dataset_id=dataset_id, repo_id=self._repo, processed=True)
            self.assertEqual(
                hashlib.md5(open(self._test_image, 'rb').read()).hexdigest(),
                hashlib.md5(
                    open(self._test_image + '.dnld', 'rb').read()).hexdigest())
        finally:
            # Remove dataset
            self._daris.delete_dataset(
                project_id=self._project, subject_id=self._subject,
                study_id=self._study, dataset_id=dataset_id,
                repo_id=self._repo, processed=True)
            try:
                # Clean up downloaded file
                os.remove(self._test_image + '.dnld')
            except OSError:
                pass  # Ignore if download wasn't created


class TestDarisToken(TestCase):

    token_path = os.path.join(os.path.dirname(__file__), 'test_daris_token')

    def tearDown(self):
        # Remove token_path if present
        try:
            os.remove(self.token_path)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise

    # FIXME: Token authentication is not working. Need to double check how
    # Parnesh did it
#     def test_create_token_and_login(self):
#         DarisSession(user='test123', password='GaryEgan1', domain='mon-daris',
#                      token_path=self.token_path, app_name='unittest').open()
#         with DarisSession(token_path=self.token_path,
#                           app_name='unittest') as daris:
#             self.assertTrue(len(daris.list_projects))
