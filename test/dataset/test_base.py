import os.path
import shutil
from unittest import TestCase
from nipype.pipeline import engine as pe
from nianalysis.dataset.base import Dataset, Pipeline
from nianalysis.interfaces.mrtrix import MRConvert


TEST_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '_data',
                                        'base'))


class DummyDataset(Dataset):

    def pipeline1(self):
        workflow = pe.Workflow(name="dummy workflow", base_dir=TEST_DIR)
        mrconvert = pe.Node(MRConvert(), name="convert")
        workflow.add_nodes(mrconvert)
        return Pipeline(
            dataset=self, name='pipeline1', workflow=workflow,
            inputs=['original'], outputs=['flipped_x', 'gradients'],
            description="A dummy pipeline used to test 'run_pipeline' method")

    def pipeline2(self):
        inputs = []
        outputs = []
        workflow = pe.Workflow(name="dummy workflow", base_dir=TEST_DIR)
        mrconvert = pe.Node(MRConvert(), name="convert")
        workflow.add_nodes(mrconvert)
        return Pipeline(
            self, 'pipeline2', workflow, inputs, outputs, options={},
            description="A dummy pipeline used to test 'run_pipeline' method")

    def pipeline3(self):
        inputs = []
        outputs = []
        workflow = pe.Workflow(name="dummy workflow", base_dir=TEST_DIR)
        mrconvert = pe.Node(MRConvert(), name="convert")
        workflow.add_nodes(mrconvert)
        return Pipeline(
            self, 'pipeline3', workflow, inputs, outputs, options={},
            description="A dummy pipeline used to test 'run_pipeline' method")

    def pipeline4(self):
        inputs = []
        outputs = []
        workflow = pe.Workflow(name="dummy workflow", base_dir=TEST_DIR)
        mrconvert = pe.Node(MRConvert(), name="convert")
        workflow.add_nodes(mrconvert)
        return Pipeline(
            self, 'pipeline4', workflow, inputs, outputs, options={},
            description="A dummy pipeline used to test 'run_pipeline' method")

    # The list of dataset components that are acquired by the scanner
    acquired_components = set(
        'original')

    generated_components = {
        'flipped_x': pipeline1,
        'gradients': pipeline1,
        'flipped_xy': pipeline2,
        'flipped_xyz': pipeline3}


class TestRunPipeline(TestCase):

    def setUp(self):
        os.makedirs(TEST_DIR)

    def tearDown(self):
        shutil.rmtree(TEST_DIR, ignore_errors=True)

    def test_run_pipeline(self):
        raise NotImplementedError
