import os.path
import shutil
import xnat
from unittest import TestCase
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from nianalysis.archive.xnat import (
    XNATArchive, XNATSource, XNATSink, SUBJECT_SUMMARY_NAME,
    PROJECT_SUMMARY_NAME)
from nianalysis.data_formats import nifti_gz_format
from nianalysis.dataset import Dataset
from nianalysis.testing import test_data_dir
import logging

logger = logging.getLogger('NiAnalysis')

REPO_ID = 2
PROJECT_ID = 4
SUBJECT_ID = 12
STUDY_ID = 1
TEST_IMAGE = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', '..', '_data', 'test_image.nii.gz'))


class TestXnatArchive(TestCase):

    XNAT_URL = 'https://mbi-xnat.erc.monash.edu.au'
    XNAT_LOGIN = 'unittest'
    XNAT_PASSWORD = 'Test123!'

    PROJECT_ID = 'DUMMYPROJECTID'
    SUBJECT_ID = 'DUMMYSUBJECTID'
    SESSION_ID = 'DUMMYSESSIONID'
    STUDY_NAME = 'astudy'
    SUMMARY_STUDY_NAME = 'asummary'
    TEST_IMAGE = os.path.abspath(os.path.join(test_data_dir,
                                              'test_image.nii.gz'))
    TEST_DIR = os.path.abspath(os.path.join(test_data_dir, 'xnat'))
    BASE_DIR = os.path.abspath(os.path.join(TEST_DIR, 'base_dir'))
    WORKFLOW_DIR = os.path.abspath(os.path.join(TEST_DIR, 'workflow_dir'))

    def setUp(self):
        # Create test data on DaRIS
        self._session_id = None
        # Make cache and working dirs
        shutil.rmtree(self.TEST_DIR, ignore_errors=True)
        os.makedirs(self.WORKFLOW_DIR)
        with xnat.connect(self.XNAT_URL,
                          user=self.XNAT_LOGIN,
                          password=self.XNAT_PASSWORD) as mbi_xnat:
            project = mbi_xnat.projects['TEST000']
            if SUBJECT_ID in project.subjects:
                project.subjects[self.SUBJECT_ID].delete()
            subject = mbi_xnat.classes.SubjectData(label=self.SUBJECT_ID,
                                                   parent=project)
            session = mbi_xnat.classes.MrSessionData(label=self.SESSION_ID,
                                                       parent=subject)
            for name in ('source1.nii.gz', 'source2.nii.gz', 'source3.nii.gz',
                         'source4.nii.gz'):
                dataset = mbi_xnat.classes.MrScanData(type=name,
                                                      parent=session)
                dataset.upload(TEST_IMAGE, )
                self.daris.upload(TEST_IMAGE, project_id=PROJECT_ID,
                                  subject_id=self.SUBJECT_ID,
                                  session_id=self.session_id,
                                  ex_method_id=1, dataset_id=dataset_id)

    def tearDown(self):
        # Clean up working dirs
        shutil.rmtree(self.TEST_DIR, ignore_errors=True)
        # Clean up session created for unit-test
        if self.session_id is not None:
            try:
                with xnat.connect(self.XNAT_URL,
                                  user=self.XNAT_LOGIN,
                                  password=self.XNAT_PASSWORD) as xnat_login:
                    xnat_login.delete_subject(
                        project_id=PROJECT_ID, subject_id=self.SUBJECT_ID)
            except Exception as e:
                print e
                pass

    def test_archive_roundtrip(self):

        # Create working dirs
        # Create DarisSource node
        archive = XNATArchive(
            server=self.XNAT_URL, repo_id=REPO_ID,
            cache_dir=self.CACHE_DIR, domain=self.DOMAIN,
            user=self.XNAT_LOGIN, password=self.XNAT_PASSWORD)
        source_files = [Dataset('source1', nifti_gz_format),
                        Dataset('source2', nifti_gz_format),
                        Dataset('source3', nifti_gz_format),
                        Dataset('source4', nifti_gz_format)]
        # Sink datasets need to be considered to be processed so we set their
        # 'pipeline' attribute to be not None. May need to update this if
        # checks on valid pipelines are included in Dataset __init__ method
        sink_files = [Dataset('sink1', nifti_gz_format, processed=True),
                      Dataset('sink3', nifti_gz_format, processed=True),
                      Dataset('sink4', nifti_gz_format, processed=True)]
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
                    source, source_file.name + XNATSource.OUTPUT_SUFFIX,
                    sink, sink_name + XNATSink.INPUT_SUFFIX)
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
            datasets = self.daris.get_datasets(
                project_id=PROJECT_ID, subject_id=self.SUBJECT_ID,
                session_id=self.session_id, ex_method_id=2, repo_id=REPO_ID)
        self.assertEqual(sorted(d.name for d in datasets.itervalues()),
                         ['sink1', 'sink3', 'sink4'])
