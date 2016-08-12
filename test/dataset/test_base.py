import os.path
import shutil
from unittest import TestCase
from nipype.pipeline import engine as pe
from nianalysis.base import Scan
from nianalysis.formats import nifti_gz_format
from nianalysis.requirements import mrtrix3_req
from nianalysis.dataset.base import Dataset, Pipeline
from nianalysis.interfaces.mrtrix import MRConvert


TEST_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '_data',
                                        'base'))


class DummyDataset(Dataset):

    def pipeline1(self):
        pipeline = self._create_pipeline(
            name='pipeline1',
            inputs=['start'],
            outputs=['pipeline1_1', 'pipeline1_2'],
            description="A dummy pipeline used to test 'run_pipeline' method",
            options={},
            requirements=[mrtrix3_req],
            citations=[],
            approx_runtime=1)
        mrconvert = pe.Node(MRConvert(), name="convert")
        # Connect inputs
        pipeline.connect_input('start', mrconvert, 'in_file')
        # Connect outputs
        pipeline.connect_output('pipeline1_1', mrconvert, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    def pipeline2(self):
        pipeline = self._create_pipeline(
            name='pipeline2',
            inputs=['start', 'pipeline1_1'],
            outputs=['pipeline2'],
            description="A dummy pipeline used to test 'run_pipeline' method",
            options={},
            requirements=[mrtrix3_req],
            citations=[],
            approx_runtime=1)
        mrconvert = pe.Node(MRConvert(), name="convert")
        # Connect inputs
        pipeline.connect_input('dwi_scan', mrconvert, 'in_file')
        # Connect outputs
        pipeline.connect_output('dwi_preproc', mrconvert, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    def pipeline3(self):
        pipeline = self._create_pipeline(
            name='pipeline3',
            inputs=['pipeline1_2'],
            outputs=['pipeline3'],
            description="A dummy pipeline used to test 'run_pipeline' method",
            options={},
            requirements=[mrtrix3_req],
            citations=[],
            approx_runtime=1)
        mrconvert = pe.Node(MRConvert(), name="convert")
        # Connect inputs
        pipeline.connect_input('dwi_scan', mrconvert, 'in_file')
        # Connect outputs
        pipeline.connect_output('dwi_preproc', mrconvert, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    def pipeline4(self):
        pipeline = self._create_pipeline(
            name='pipeline4',
            inputs=['pipeline3'],
            outputs=['pipeline4'],
            description="A dummy pipeline used to test 'run_pipeline' method",
            options={},
            requirements=[mrtrix3_req],
            citations=[],
            approx_runtime=1)
        mrconvert = pe.Node(MRConvert(), name="convert")
        # Connect inputs
        pipeline.connect_input('dwi_scan', mrconvert, 'in_file')
        # Connect outputs
        pipeline.connect_output('dwi_preproc', mrconvert, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    components = [
        Scan('start', nifti_gz_format),
        Scan('pipeline1_1', nifti_gz_format, pipeline1),
        Scan('pipeline1_2', nifti_gz_format, pipeline1),
        Scan('pipeline2', nifti_gz_format, pipeline2),
        Scan('pipeline3', nifti_gz_format, pipeline3)]


class TestRunPipeline(TestCase):

    def setUp(self):
        os.makedirs(TEST_DIR)

    def tearDown(self):
        shutil.rmtree(TEST_DIR, ignore_errors=True)

    def test_run_pipeline(self):
        raise NotImplementedError
