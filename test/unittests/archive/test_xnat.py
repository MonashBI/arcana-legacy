import os.path
import shutil
import xnat
from unittest import TestCase
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from nianalysis.archive.xnat import (
    XNATArchive, XNATSource, XNATSink)
from nianalysis.data_formats import nifti_gz_format
from nianalysis.dataset import Dataset
from nianalysis.testing import test_data_dir
import logging
from nianalysis.utils import split_extension
from nianalysis.data_formats import data_formats_by_ext

logger = logging.getLogger('NiAnalysis')


class TestXnatArchive(TestCase):

    XNAT_URL = 'https://mbi-xnat.erc.monash.edu.au'
    XNAT_LOGIN = 'unittest'
    XNAT_PASSWORD = 'Test123!'

    PROJECT_ID = 'TEST000'
    SUBJECT_ID = 'TEST000_001'
    SESSION_ID = 'TEST000_001_MR01'
    STUDY_NAME = 'astudy'
    SUMMARY_STUDY_NAME = 'asummary'
    TEST_IMAGE = os.path.abspath(os.path.join(test_data_dir,
                                              'test_image.nii.gz'))
    TEST_DIR = os.path.abspath(os.path.join(test_data_dir, 'xnat'))
    CACHE_DIR = os.path.abspath(os.path.join(TEST_DIR, 'base_dir'))
    WORKFLOW_DIR = os.path.abspath(os.path.join(TEST_DIR, 'workflow_dir'))

    def setUp(self):
        # Create test data on DaRIS
        self._session_id = None
        # Make cache and working dirs
        shutil.rmtree(self.TEST_DIR, ignore_errors=True)
        shutil.rmtree(self.CACHE_DIR, ignore_errors=True)
        os.makedirs(self.WORKFLOW_DIR)
        os.makedirs(self.CACHE_DIR)
        with xnat.connect(self.XNAT_URL,
                          user=self.XNAT_LOGIN,
                          password=self.XNAT_PASSWORD) as mbi_xnat:
            project = mbi_xnat.projects[self.PROJECT_ID]
            if self.SUBJECT_ID in project.subjects:
                project.subjects[self.SUBJECT_ID].delete()
            subject = mbi_xnat.classes.SubjectData(label=self.SUBJECT_ID,
                                                   parent=project)
            session = mbi_xnat.classes.MrSessionData(label=self.SESSION_ID,
                                                     parent=subject)
            for fname in ('source1.nii.gz', 'source2.nii.gz', 'source3.nii.gz',
                          'source4.nii.gz'):
                name, ext = split_extension(fname)
                dataset = mbi_xnat.classes.MrScanData(type=name,
                                                      parent=session)
                resource = dataset.create_resource(
                    data_formats_by_ext[ext].name)
                resource.upload(self.TEST_IMAGE, fname)

    def tearDown(self):
        # Clean up working dirs
        shutil.rmtree(self.TEST_DIR, ignore_errors=True)
        shutil.rmtree(self.CACHE_DIR, ignore_errors=True)
        # Clean up session created for unit-test
        try:
            with xnat.connect(self.XNAT_URL,
                              user=self.XNAT_LOGIN,
                              password=self.XNAT_PASSWORD) as xnat_login:
                xnat_login.delete_subject(
                    project_id=self.PROJECT_ID, subject_id=self.SUBJECT_ID)
        except Exception:
            pass

    def test_archive_roundtrip(self):

        # Create working dirs
        # Create DarisSource node
        archive = XNATArchive(
            server=self.XNAT_URL, cache_dir=self.CACHE_DIR,
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
        inputnode.inputs.session_id = str(self.SESSION_ID)
        source = archive.source(self.PROJECT_ID, source_files)
        sink = archive.sink(self.PROJECT_ID, sink_files)
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
            self.CACHE_DIR, str(self.PROJECT_ID),
            str(self.SUBJECT_ID), str(self.SESSION_ID))
        sink_cache_dir = os.path.join(
            self.CACHE_DIR, str(self.PROJECT_ID),
            str(self.SUBJECT_ID),
            str(self.SESSION_ID) + XNATArchive.PROCESSED_SUFFIX)
        self.assertEqual(sorted(os.listdir(source_cache_dir)),
                         ['source1.nii.gz', 'source2.nii.gz',
                          'source3.nii.gz', 'source4.nii.gz'])
        self.assertEqual(sorted(os.listdir(sink_cache_dir)),
                         ['sink1.nii.gz', 'sink3.nii.gz', 'sink4.nii.gz'])
        with xnat.connect(self.XNAT_URL,
                          user=self.XNAT_LOGIN,
                          password=self.XNAT_PASSWORD) as mbi_xnat:
            dataset_names = mbi_xnat.experiments[
                self.SESSION_ID + XNATArchive.PROCESSED_SUFFIX].scans.keys()
        self.assertEqual(sorted(dataset_names),
                         ['sink1', 'sink3', 'sink4'])
