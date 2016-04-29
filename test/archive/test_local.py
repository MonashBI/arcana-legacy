import os.path
from unittest import TestCase
from nipype.pipeline import engine as pe
from neuroanalysis.archive.daris import DarisArchive
from neuroanalysis.archive.local import LocalArchive
from neuroanalysis.utils import rmtree_ignore_missing

SERVER = 'mf-erc.its.monash.edu.au'

# The projects/subjects/studies to alter on DaRIS
PROJECT_ID = 4
TEST_IMAGE = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '_data', 'test_upload.nii.gz'))
BASE_DIR = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '_data', 'archives'))
WORKFLOW_DIR = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '_data', 'workflow_dir'))


class TestArchive(TestCase):

    def test_archive_roundtrip(self):
        # Create different archives to test
        daris = DarisArchive(user='test123', password='GaryEgan1',
                             cache_dir=os.path.join(BASE_DIR, 'daris'),
                             domain='mon-daris', server=SERVER)
        local = LocalArchive(os.path.join(BASE_DIR, 'local'))
        # 
        for archive in (daris, local):
            rmtree_ignore_missing(archive.local_dir)
            rmtree_ignore_missing(WORKFLOW_DIR)
            sink = archive.sink(PROJECT_ID)
            sink = 
        



class TestDarisSinkAndSource(TestCase):

    def test_daris_roundtrip(self):
        # Create test data on DaRIS
        # Create working dirs
        rmtree_ignore_missing(BASE_DIR)
        rmtree_ignore_missing(WORKFLOW_DIR)
        os.makedirs(BASE_DIR)
        os.makedirs(WORKFLOW_DIR)
        # Create DarisSource node
        source = pe.Node(LocalSource(), 'source')
        source.inputs.project_id = PROJECT_ID
        source.inputs.subject_id = SUBJECT_ID
        source.inputs.study_id = study_id
        source.inputs.server = SERVER
        source.inputs.repo_id = REPO_ID
        source.inputs.cache_dir = CACHE_DIR
        source.inputs.domain = 'mon-daris'
        source.inputs.user = 'test123'
        source.inputs.password = 'GaryEgan1'
        source.inputs.files = [
            ('source1', False), ('source2', False), ('source3', False),
            ('source4', False)]
        # Create DataSink node
        sink = pe.Node(DarisSink(), 'sink')
        sink.inputs.name = 'unittest_study'
        sink.inputs.description = (
            "A study created by the soure-sink unittest")
        sink.inputs.project_id = PROJECT_ID
        sink.inputs.subject_id = SUBJECT_ID
        sink.inputs.study_id = study_id
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
            files = daris.get_files(
                project_id=PROJECT_ID, subject_id=SUBJECT_ID,
                study_id=study_id, processed=True, repo_id=REPO_ID)
        self.assertEqual(sorted(d.name for d in files.itervalues()),
                         ['sink1', 'sink3', 'sink4'])