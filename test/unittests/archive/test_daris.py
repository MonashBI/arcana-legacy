import os.path
import shutil
import hashlib
from unittest import TestCase
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from nianalysis.archive.daris import (
    DarisLogin, DarisArchive, DarisSource, DarisSink, SUBJECT_SUMMARY_ID,
    PROJECT_SUMMARY_ID)
from nianalysis.exceptions import DarisException
from nianalysis.formats import nifti_gz_format
from nianalysis.base import Scan


# The projects/subjects/sessions to alter on DaRIS
SERVER = 'mf-erc.its.monash.edu.au'
REPO_ID = 2
PROJECT_ID = 4
SUBJECT_ID = 12
STUDY_ID = 1
TEST_IMAGE = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', '..', '_data', 'test_image.nii.gz'))


class TestDarisLogin(TestCase):

    def setUp(self):
        self._daris = DarisLogin(user='test123', password='GaryEgan1',
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

    def test_get_sessions(self):
        sessions = self._daris.get_sessions(project_id=4, subject_id=1,
                                            repo_id=REPO_ID, ex_method_id=1)
        self.assertEqual(len(sessions), 3)
        self.assertEqual(sessions[1].name, 'Study1')
        self.assertEqual(sessions[2].name, 'Study2')
        self.assertEqual(sessions[3].name, 'Study3')

    def test_get_files(self):
        files = self._daris.get_files(
            project_id=4, subject_id=1, session_id=3, repo_id=REPO_ID,
            ex_method_id=1)
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

    def test_add_remove_session(self):
        for ex_method_id in (1, 2):
            num_sessions = len(self._daris.get_sessions(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                ex_method_id=ex_method_id))
            session_id = self._daris.add_session(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                name='unittest-session', repo_id=REPO_ID,
                description=("A session added by a unit-test that should be "
                             "removed by the same test"),
                ex_method_id=ex_method_id)
            self._daris.add_session(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                session_id=(session_id + 1),
                name='unittest-session2', repo_id=REPO_ID,
                description=("A session added by a unit-test that should be "
                             "removed by the same test"),
                ex_method_id=ex_method_id)
            self.assertEqual(
                len(self._daris.get_sessions(project_id=PROJECT_ID,
                                             subject_id=SUBJECT_ID,
                                             ex_method_id=ex_method_id)),
                num_sessions + 2)
            self._daris.delete_session(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                session_id=session_id, repo_id=REPO_ID,
                ex_method_id=ex_method_id)
            self._daris.delete_session(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                session_id=(session_id + 1), repo_id=REPO_ID,
                ex_method_id=ex_method_id)
            self.assertEqual(
                num_sessions,
                len(self._daris.get_sessions(project_id=PROJECT_ID,
                                             subject_id=SUBJECT_ID,
                                             ex_method_id=ex_method_id)))

    def test_add_remove_file(self):
        for ex_method_id in (1, 2):
            num_files = len(self._daris.get_files(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                session_id=STUDY_ID, ex_method_id=ex_method_id))
            file_id = self._daris.add_file(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                session_id=STUDY_ID, name='unittest-file',
                repo_id=REPO_ID,
                description=("A file added by a unit-test that should be "
                             "removed by the same test"),
                ex_method_id=ex_method_id)
            self._daris.add_file(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                file_id=(file_id + 1),
                session_id=STUDY_ID, name='unittest-file2',
                repo_id=REPO_ID,
                description=("A file added by a unit-test that should be "
                             "removed by the same test"),
                ex_method_id=ex_method_id)
            self.assertEqual(
                len(self._daris.get_files(
                    project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                    session_id=STUDY_ID, ex_method_id=ex_method_id)),
                num_files + 2)
            self._daris.delete_file(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                session_id=STUDY_ID, file_id=file_id,
                repo_id=REPO_ID, ex_method_id=ex_method_id)
            self._daris.delete_file(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                file_id=(file_id + 1), repo_id=REPO_ID,
                session_id=STUDY_ID, ex_method_id=ex_method_id)
            self.assertEqual(
                num_files,
                len(self._daris.get_files(
                    project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                    session_id=STUDY_ID, ex_method_id=ex_method_id)))

    def test_upload_download(self):
        file_id = self._daris.add_file(
            project_id=PROJECT_ID, subject_id=SUBJECT_ID,
            session_id=STUDY_ID, name='unittest-upload',
            repo_id=REPO_ID,
            description=(
                "A file added by a unit-test for testing the "
                "upload/download functionality that should be "
                "removed by the same test"), ex_method_id=2)
        try:
            self._daris.upload(
                TEST_IMAGE, project_id=PROJECT_ID,
                subject_id=SUBJECT_ID, session_id=STUDY_ID,
                file_id=file_id, repo_id=REPO_ID, ex_method_id=2)
            self._daris.download(
                TEST_IMAGE + '.dnld', project_id=PROJECT_ID,
                subject_id=SUBJECT_ID, session_id=STUDY_ID,
                file_id=file_id, repo_id=REPO_ID, ex_method_id=2)
            self.assertEqual(
                hashlib.md5(open(TEST_IMAGE, 'rb').read()).hexdigest(),
                hashlib.md5(
                    open(TEST_IMAGE + '.dnld', 'rb').read()).hexdigest())
        finally:
            # Remove file
            self._daris.delete_file(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                session_id=STUDY_ID, file_id=file_id,
                repo_id=REPO_ID, ex_method_id=2)
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
#         DarisLogin(user='test123', password='GaryEgan1', domain='mon-daris', @IgnorePep8
#                      server=SERVER, token_path=self.token_path,
#                      app_name='unittest').open()
#         with DarisLogin(token_path=self.token_path,
#                           app_name='unittest') as daris:
#             self.assertTrue(len(daris.list_projects))


class TestDarisArchive(TestCase):

    TEST_DIR = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '..', '..', '_data', 'daris-archive'))
    CACHE_DIR = os.path.abspath(os.path.join(TEST_DIR, 'cache_dir'))
    SUBJECT_ID = 99
    WORKFLOW_DIR = os.path.abspath(os.path.join(TEST_DIR, 'workflow_dir'))
    DOMAIN = 'mon-daris'
    USER = 'test123'
    PASSWORD = 'GaryEgan1'

    def setUp(self):
        # Create test data on DaRIS
        self._session_id = None
        self.daris = DarisLogin(user='test123', password='GaryEgan1',
                                  domain='mon-daris', server=SERVER)
        # Make cache and working dirs
        shutil.rmtree(self.TEST_DIR, ignore_errors=True)
        os.makedirs(self.CACHE_DIR)
        os.makedirs(self.WORKFLOW_DIR)
        # Upload test session
        with self.daris:  # Opens the daris session
            try:
                self.daris.delete_subject(project_id=PROJECT_ID,
                                          subject_id=self.SUBJECT_ID)
            except DarisException:
                pass  # Ignore if present
            self.subject_id = self.daris.add_subject(
                project_id=PROJECT_ID, subject_id=self.SUBJECT_ID,
                name="NiAnalyais Unittest",
                description=(
                    "Automatically generated subject to run DaRIS unittests"))
            self.session_id = self.daris.add_session(
                project_id=PROJECT_ID, subject_id=self.SUBJECT_ID,
                ex_method_id=1, name='source-sink-unittest-session',
                description="Used in DarisSource/Sink unittest")
            for name in ('source1.nii.gz', 'source2.nii.gz', 'source3.nii.gz',
                         'source4.nii.gz'):
                file_id = self.daris.add_file(
                    project_id=PROJECT_ID, subject_id=self.SUBJECT_ID,
                    session_id=self.session_id, ex_method_id=1,
                    name=name, description=(
                        "A file added for DarisSink/Source unittest"))
                self.daris.upload(TEST_IMAGE, project_id=PROJECT_ID,
                                  subject_id=self.SUBJECT_ID,
                                  session_id=self.session_id,
                                  ex_method_id=1, file_id=file_id)

    def tearDown(self):
        # Clean up working dirs
        shutil.rmtree(self.TEST_DIR, ignore_errors=True)
        # Clean up session created for unit-test
        if self.session_id is not None:
            try:
                with self.daris:
                    self.daris.delete_subject(
                        project_id=PROJECT_ID, subject_id=self.SUBJECT_ID)
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
        # Sink files need to be considered to be processed so we set their
        # 'pipeline' attribute to be not None. May need to update this if
        # checks on valid pipelines are included in Scan __init__ method
        sink_files = [Scan('sink1', nifti_gz_format, pipeline=True),
                      Scan('sink3', nifti_gz_format, pipeline=True),
                      Scan('sink4', nifti_gz_format, pipeline=True)]
        inputnode = pe.Node(IdentityInterface(['subject_id', 'session_id']),
                            'inputnode')
        inputnode.inputs.subject_id = str(self.SUBJECT_ID)
        inputnode.inputs.session_id = str(self.session_id)
        source = archive.source(PROJECT_ID, source_files)
        sink = archive.sink(PROJECT_ID, sink_files)
        sink.inputs.name = 'archive-roundtrip-unittest'
        sink.inputs.description = (
            "A test session created by archive roundtrip unittest")
        # Create workflow connecting them together
        workflow = pe.Workflow('source-sink-unit-test',
                               base_dir=self.WORKFLOW_DIR)
        workflow.add_nodes((source, sink))
        workflow.connect(inputnode, 'subject_id', source, 'subject_id')
        workflow.connect(inputnode, 'session_id', source, 'session_id')
        workflow.connect(inputnode, 'subject_id', sink, 'subject_id')
        workflow.connect(inputnode, 'session_id', sink, 'session_id')
        for source_file in source_files:
            if source_file.name != 'source2':
                sink_name = source_file.name.replace('source', 'sink')
                workflow.connect(
                    source, source_file.name + DarisSource.OUTPUT_SUFFIX,
                    sink, sink_name + DarisSink.INPUT_SUFFIX)
        workflow.run()
        # Check cache was created properly
        source_cache_dir = os.path.join(
            self.CACHE_DIR, str(REPO_ID), str(PROJECT_ID),
            str(self.SUBJECT_ID), '1', str(self.session_id))
        sink_cache_dir = os.path.join(
            self.CACHE_DIR, str(REPO_ID), str(PROJECT_ID),
            str(self.SUBJECT_ID), '2', str(self.session_id))
        self.assertEqual(sorted(os.listdir(source_cache_dir)),
                         ['source1.nii.gz', 'source2.nii.gz',
                          'source3.nii.gz', 'source4.nii.gz'])
        self.assertEqual(sorted(os.listdir(sink_cache_dir)),
                         ['sink1.nii.gz', 'sink3.nii.gz', 'sink4.nii.gz'])
        with self.daris:
            files = self.daris.get_files(
                project_id=PROJECT_ID, subject_id=self.SUBJECT_ID,
                session_id=self.session_id, ex_method_id=2, repo_id=REPO_ID)
        self.assertEqual(sorted(d.name for d in files.itervalues()),
                         ['sink1', 'sink3', 'sink4'])


class TestDarisArchiveSummary(TestCase):

    TEST_DIR = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '..', '..', '_data', 'daris-summary'))
    CACHE_DIR = os.path.abspath(os.path.join(TEST_DIR, 'cache_dir'))
    SUBJECT_ID = 98
    WORKFLOW_DIR = os.path.abspath(os.path.join(TEST_DIR, 'workflow_dir'))
    DOMAIN = 'mon-daris'
    USER = 'test123'
    PASSWORD = 'GaryEgan1'

    def setUp(self):
        # Create test data on DaRIS
        self._session_id = None
        self.daris = DarisLogin(user='test123', password='GaryEgan1',
                                  domain='mon-daris', server=SERVER)
        # Make cache and working dirs
        shutil.rmtree(self.TEST_DIR, ignore_errors=True)
        os.makedirs(self.CACHE_DIR)
        os.makedirs(self.WORKFLOW_DIR)
        # Upload test session
        with self.daris:  # Opens the daris session
            try:
                self.daris.delete_subject(project_id=PROJECT_ID,
                                          subject_id=self.SUBJECT_ID)
                self.daris.delete_ex_method(
                    project_id=PROJECT_ID, subject_id=1,
                    ex_method_id=4)
            except DarisException:
                pass  # Ignore if present
            self.subject_id = self.daris.add_subject(
                project_id=PROJECT_ID, subject_id=self.SUBJECT_ID,
                name="NiAnalyais Unittest",
                description=(
                    "Automatically generated subject to run DaRIS unittests"))
            self.session_id = self.daris.add_session(
                project_id=PROJECT_ID, subject_id=self.SUBJECT_ID,
                ex_method_id=1, name='source-sink-unittest-session',
                description="Used in DarisSource/Sink unittest")
            for name in ('source1.nii.gz', 'source2.nii.gz', 'source3.nii.gz',
                         'source4.nii.gz'):
                file_id = self.daris.add_file(
                    project_id=PROJECT_ID, subject_id=self.SUBJECT_ID,
                    session_id=self.session_id, ex_method_id=1,
                    name=name, description=(
                        "A file added for DarisSink/Source unittest"))
                self.daris.upload(TEST_IMAGE, project_id=PROJECT_ID,
                                  subject_id=self.SUBJECT_ID,
                                  session_id=self.session_id,
                                  ex_method_id=1, file_id=file_id)

    def tearDown(self):
        # Clean up working dirs
        shutil.rmtree(self.TEST_DIR, ignore_errors=True)
        # Clean up session created for unit-test
        if self.session_id is not None:
            try:
                with self.daris:
                    self.daris.delete_subject(
                        project_id=PROJECT_ID, subject_id=self.SUBJECT_ID)
                    self.daris.delete_ex_method(
                        project_id=PROJECT_ID, subject_id=1,
                        ex_method_id=4)
            except DarisException:
                pass

    def test_summary(self):
        # Create working dirs
        # Create LocalSource node
        archive = DarisArchive(
            server=SERVER, repo_id=REPO_ID,
            cache_dir=self.CACHE_DIR, domain=self.DOMAIN,
            user=self.USER, password=self.PASSWORD)
        # TODO: Should test out other file formats as well.
        source_files = [Scan('source1', nifti_gz_format),
                        Scan('source2', nifti_gz_format)]
        inputnode = pe.Node(IdentityInterface(['subject_id', 'session_id']),
                            'inputnode')
        inputnode.inputs.subject_id = str(self.SUBJECT_ID)
        inputnode.inputs.session_id = str(self.session_id)
        source = archive.source(str(PROJECT_ID), source_files)
        subject_sink_files = [Scan('sink1', nifti_gz_format, pipeline=True,
                                   multiplicity='per_subject')]
        subject_sink = archive.sink(str(PROJECT_ID),
                                    subject_sink_files,
                                    multiplicity='per_subject')
        subject_sink.inputs.name = 'subject_summary'
        subject_sink.inputs.description = (
            "Tests the sinking of subject-wide scans")
        project_sink_files = [Scan('sink2', nifti_gz_format, pipeline=True,
                                   multiplicity='per_project')]
        project_sink = archive.sink(PROJECT_ID,
                                    project_sink_files,
                                    multiplicity='per_project')

        project_sink.inputs.name = 'project_summary'
        project_sink.inputs.description = (
            "Tests the sinking of project-wide scans")
        # Create workflow connecting them together
        workflow = pe.Workflow('summary_unittest',
                               base_dir=self.WORKFLOW_DIR)
        workflow.add_nodes((source, subject_sink, project_sink))
        workflow.connect(inputnode, 'subject_id', source, 'subject_id')
        workflow.connect(inputnode, 'session_id', source, 'session_id')
        workflow.connect(inputnode, 'subject_id', subject_sink, 'subject_id')
        workflow.connect(
            source, 'source1' + DarisSource.OUTPUT_SUFFIX,
            subject_sink, 'sink1' + DarisSink.INPUT_SUFFIX)
        workflow.connect(
            source, 'source2' + DarisSource.OUTPUT_SUFFIX,
            project_sink, 'sink2' + DarisSink.INPUT_SUFFIX)
        workflow.run()
        # Check cached summary directories were created properly
        subject_dir = os.path.join(
            self.CACHE_DIR, str(REPO_ID), str(PROJECT_ID),
            str(self.SUBJECT_ID), str(SUBJECT_SUMMARY_ID), '1')
        self.assertEqual(sorted(os.listdir(subject_dir)),
                         ['sink1.nii.gz'])
        project_dir = os.path.join(
            self.CACHE_DIR, str(REPO_ID), str(PROJECT_ID), '1',
            str(PROJECT_SUMMARY_ID), '1')
        self.assertEqual(sorted(os.listdir(project_dir)),
                         ['sink2.nii.gz'])
        with self.daris:
            subject_files = self.daris.get_files(
                project_id=PROJECT_ID, subject_id=self.SUBJECT_ID,
                session_id=1, ex_method_id=SUBJECT_SUMMARY_ID,
                repo_id=REPO_ID)
            project_files = self.daris.get_files(
                project_id=PROJECT_ID, subject_id=1, session_id=1,
                ex_method_id=PROJECT_SUMMARY_ID, repo_id=REPO_ID)
        self.assertEqual(sorted(d.name for d in subject_files.itervalues()),
                         ['sink1'])
        self.assertEqual(sorted(d.name for d in project_files.itervalues()),
                         ['sink2'])
        # Reload the data from the summary directories
        reloadinputnode = pe.Node(IdentityInterface(['subject_id',
                                                     'session_id']),
                                  'reload_inputnode')
        reloadinputnode.inputs.subject_id = str(self.SUBJECT_ID)
        reloadinputnode.inputs.session_id = str(self.session_id)
        reloadsource = archive.source(
            PROJECT_ID,
            source_files + subject_sink_files + project_sink_files,
            name='reload_source')
        reloadsink = archive.sink(PROJECT_ID,
                                  [Scan('resink1', nifti_gz_format,
                                        pipeline=True),
                                   Scan('resink2', nifti_gz_format,
                                        pipeline=True)])
        reloadsink.inputs.name = 'reload_summary'
        reloadsink.inputs.description = (
            "Tests the reloading of subject and project summary scans")
        reloadworkflow = pe.Workflow('reload_summary_unittest',
                                     base_dir=self.WORKFLOW_DIR)
        reloadworkflow.connect(reloadinputnode, 'subject_id',
                               reloadsource, 'subject_id')
        reloadworkflow.connect(reloadinputnode, 'session_id',
                               reloadsource, 'session_id')
        reloadworkflow.connect(reloadinputnode, 'subject_id',
                               reloadsink, 'subject_id')
        reloadworkflow.connect(reloadinputnode, 'session_id',
                               reloadsink, 'session_id')
        reloadworkflow.connect(reloadsource,
                               'sink1' + DarisSource.OUTPUT_SUFFIX,
                               reloadsink,
                               'resink1' + DarisSink.INPUT_SUFFIX)
        reloadworkflow.connect(reloadsource,
                               'sink2' + DarisSource.OUTPUT_SUFFIX,
                               reloadsink,
                               'resink2' + DarisSink.INPUT_SUFFIX)
        reloadworkflow.run()
        session_dir = os.path.join(
            self.CACHE_DIR, str(REPO_ID), str(PROJECT_ID),
            str(self.SUBJECT_ID), '2', str(self.session_id))
        self.assertEqual(sorted(os.listdir(session_dir)),
                         ['resink1.nii.gz', 'resink2.nii.gz'])
        with self.daris:
            session_files = self.daris.get_files(
                project_id=PROJECT_ID, subject_id=self.SUBJECT_ID,
                session_id=self.session_id, ex_method_id=2, repo_id=REPO_ID)
        self.assertEqual(sorted(d.name for d in session_files.itervalues()),
                         ['resink1', 'resink2'])
