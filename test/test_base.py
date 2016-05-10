import os.path
import shutil
from unittest import TestCase
from nipype.pipeline import engine as pe
from neuroanalysis.base import Dataset, Pipeline


TEST_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '_data',
                                        'base'))


class DummyDataset(Dataset):

    def pipeline1(self):
        inputs = []
        outputs = []
        workflow = pe.Workflow(name="dummy workflow", base_dir=TEST_DIR)
        return Pipeline(
            self, 'pipeline1', workflow, inputs, outputs, options={},
            description="A dummy pipeline used to test 'run_pipeline' method")

    def pipeline2(self):
        inputs = []
        outputs = []
        workflow = pe.Workflow(name="dummy workflow", base_dir=TEST_DIR)
        return Pipeline(
            self, 'pipeline1', workflow, inputs, outputs, options={},
            description="A dummy pipeline used to test 'run_pipeline' method")

    def pipeline3(self):
        inputs = []
        outputs = []
        workflow = pe.Workflow(name="dummy workflow", base_dir=TEST_DIR)
        return Pipeline(
            self, 'pipeline1', workflow, inputs, outputs, options={},
            description="A dummy pipeline used to test 'run_pipeline' method")

    def pipeline4(self):
        inputs = []
        outputs = []
        workflow = pe.Workflow(name="dummy workflow", base_dir=TEST_DIR)
        return Pipeline(
            self, 'pipeline1', workflow, inputs, outputs, options={},
            description="A dummy pipeline used to test 'run_pipeline' method")

    # The list of dataset components that are acquired by the scanner
    acquired_components = set(
        'diffusion', 'distortion_correct', 'gradients')

    generated_components = {
        'one': pipeline1,
        'two': pipeline2,
        'three': pipeline3,
        'four': pipeline3}


class TestRunPipeline(TestCase):

    def setUp(self):
        os.makedirs(TEST_DIR)

    def tearDown(self):
        shutil.rmtree(TEST_DIR, ignore_errors=True)

    def test_run_pipeline(self):
        raise NotImplementedError
