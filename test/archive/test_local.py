import os.path
import shutil
from unittest import TestCase
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from neuroanalysis.archive.local import LocalArchive
from neuroanalysis.base import AcquiredFile


class TestLocalArchive(TestCase):

    PROJECT_ID = 'DUMMYPROJECTID'
    SUBJECT_ID = 'DUMMYSUBJECTID'
    SESSION_ID = 'DUMMYSESSIONID'
    TEST_IMAGE = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '_data', 'test_image.nii.gz'))
    TEST_DIR = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '_data', 'local'))
    BASE_DIR = os.path.abspath(os.path.join(TEST_DIR, 'base_dir'))
    WORKFLOW_DIR = os.path.abspath(os.path.join(TEST_DIR, 'workflow_dir'))

    def setUp(self):
        # Create test data on DaRIS
        self._study_id = None
        # Make cache and working dirs
        shutil.rmtree(self.TEST_DIR, ignore_errors=True)
        os.makedirs(self.WORKFLOW_DIR)
        session_path = os.path.join(
            self.BASE_DIR, self.PROJECT_ID, self.SUBJECT_ID, self.SESSION_ID)
        os.makedirs(session_path)
        for i in xrange(1, 5):
            shutil.copy(self.TEST_IMAGE,
                        os.path.join(session_path,
                                     'source{}.nii.gz'.format(i)))

    def tearDown(self):
        # Clean up working dirs
        shutil.rmtree(self.TEST_DIR, ignore_errors=True)

    def test_archive_roundtrip(self):

        # Create working dirs
        # Create LocalSource node
        archive = LocalArchive(base_dir=self.BASE_DIR)
        source_files = [AcquiredFile('source1', 'source1.nii.gz'),
                        AcquiredFile('source2', 'source2.nii.gz'),
                        AcquiredFile('source3', 'source3.nii.gz'),
                        AcquiredFile('source4', 'source4.nii.gz')]
        inputnode = pe.Node(IdentityInterface(['session']), 'inputnode')
        inputnode.inputs.session = (self.SUBJECT_ID, self.SESSION_ID)
        source = archive.source(self.PROJECT_ID, source_files)
        sink = archive.sink(self.PROJECT_ID)
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
                sink_filename = source_file.name.replace('source', 'sink')
                workflow.connect(source, source_file.name,
                                 sink, sink_filename)
        workflow.run()
        # Check cache was created properly
        session_dir = os.path.join(
            self.BASE_DIR, str(self.PROJECT_ID), str(self.SUBJECT_ID),
            str(self.SESSION_ID))
        self.assertEqual(sorted(os.listdir(session_dir)),
                         ['sink1', 'sink3', 'sink4',
                          'source1.nii.gz', 'source2.nii.gz',
                          'source3.nii.gz', 'source4.nii.gz'])
