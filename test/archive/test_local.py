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
    TEST_IMAGE = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '_data', 'test_image.nii.gz'))
    BASE_DIR = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '_data', 'local', 'cache_dir'))
    WORKFLOW_DIR = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '_data', 'local', 'workflow_dir'))

    def setUp(self):
        # Create test data on DaRIS
        self._study_id = None
        # Make cache and working dirs
        shutil.rmtree(self.BASE_DIR, ignore_errors=True)
        shutil.rmtree(self.WORKFLOW_DIR, ignore_errors=True)
        os.makedirs(self.WORKFLOW_DIR)
        subject_path = os.path.join(
            self.BASE_DIR, self.PROJECT_ID, self.SUBJECT_ID)
        os.makedirs(subject_path)
        for i in xrange(4):
            shutil.copy(self.TEST_IMAGE,
                        os.path.join(subject_path,
                                     'source{}.nii.gz'.format(i)))

    def tearDown(self):
        # Clean up working dirs
        shutil.rmtree(self.BASE_DIR, ignore_errors=True)
        shutil.rmtree(self.WORKFLOW_DIR, ignore_errors=True)

    def test_archive_roundtrip(self):

        # Create working dirs
        # Create LocalSource node
        archive = LocalArchive(base_dir=self.BASE_DIR)
        source_files = [AcquiredFile('source1', 'source1.nii.gz'),
                        AcquiredFile('source2', 'source2.nii.gz'),
                        AcquiredFile('source3', 'source3.nii.gz'),
                        AcquiredFile('source4', 'source4.nii.gz')]
        inputnode = pe.Node(IdentityInterface(['session']), 'inputnode')
        inputnode.inputs.session = (self.SUBJECT_ID, self.study_id)
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
        source_cache_dir = os.path.join(
            self.BASE_DIR, str(self.PROJECT_ID), str(self.SUBJECT_ID),
            '1', str(self.study_id))
        sink_cache_dir = os.path.join(
            self.BASE_DIR, str(self.PROJECT_ID), str(self.SUBJECT_ID),
            '2', str(self.study_id))
        self.assertEqual(sorted(os.listdir(source_cache_dir)),
                         ['source1.nii.gz', 'source2.nii.gz',
                          'source3.nii.gz', 'source4.nii.gz'])
        self.assertEqual(sorted(os.listdir(sink_cache_dir)),
                         ['sink1', 'sink3', 'sink4'])
