import os.path
import shutil
from unittest import TestCase
import subprocess as sp
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import Merge
from nianalysis.dataset import Dataset, DatasetSpec
from nianalysis.data_formats import mrtrix_format
from nianalysis.requirements import mrtrix3_req
from nianalysis.study.base import Study, set_dataset_specs
from nianalysis.study.combined import CombinedStudy
from nianalysis.interfaces.mrtrix import MRMath
from nianalysis.archive.local import LocalArchive
from nianalysis.testing import test_data_dir
import logging

logger = logging.getLogger('NiAnalysis')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


class DummySubStudyA(Study):

    def pipeline1(self):
        pipeline = self._create_pipeline(
            name='pipeline1',
            inputs=['x', 'y'],
            outputs=['z'],
            description="A dummy pipeline used to test CombinedStudy class",
            options={},
            requirements=[mrtrix3_req],
            citations=[],
            approx_runtime=1)
        merge = pe.Node(Merge(2), name="merge")
        mrmath = pe.Node(MRMath(), name="mrmath")
        mrmath.inputs.operation = 'sum'
        # Connect inputs
        pipeline.connect_input('x', merge, 'in1')
        pipeline.connect_input('y', merge, 'in2')
        # Connect nodes
        pipeline.connect(merge, 'out', mrmath, 'in_files')
        # Connect outputs
        pipeline.connect_output('z', mrmath, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    _dataset_specs = set_dataset_specs(
        DatasetSpec('x', mrtrix_format),
        DatasetSpec('y', mrtrix_format),
        DatasetSpec('z', mrtrix_format, pipeline1))


class DummySubStudyB(Study):

    def pipeline1(self):
        pipeline = self._create_pipeline(
            name='pipeline1',
            inputs=['w', 'x'],
            outputs=['y', 'z'],
            description="A dummy pipeline used to test CombinedStudy class",
            options={},
            requirements=[mrtrix3_req],
            citations=[],
            approx_runtime=1)
        merge1 = pe.Node(Merge(2), name='merge1')
        merge2 = pe.Node(Merge(2), name='merge2')
        merge3 = pe.Node(Merge(2), name='merge3')
        mrsum1 = pe.Node(MRMath(), name="mrsum1")
        mrsum1.inputs.operation = 'sum'
        mrsum2 = pe.Node(MRMath(), name="mrsum2")
        mrsum2.inputs.operation = 'sum'
        mrproduct = pe.Node(MRMath(), name="mrproduct")
        mrproduct.inputs.operation = 'product'
        # Connect inputs
        pipeline.connect_input('w', merge1, 'in1')
        pipeline.connect_input('x', merge1, 'in2')
        pipeline.connect_input('x', merge2, 'in1')
        # Connect nodes
        pipeline.connect(merge1, 'out', mrsum1, 'in_files')
        pipeline.connect(mrsum1, 'out_file', merge2, 'in2')
        pipeline.connect(merge2, 'out', mrsum2, 'in_files')
        pipeline.connect(mrsum1, 'out_file', merge3, 'in1')
        pipeline.connect(mrsum2, 'out_file', merge3, 'in2')
        pipeline.connect(merge3, 'out', mrproduct, 'in_files')
        # Connect outputs
        pipeline.connect_output('y', mrsum2, 'out_file')
        pipeline.connect_output('z', mrproduct, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    _dataset_specs = set_dataset_specs(
        DatasetSpec('w', mrtrix_format),
        DatasetSpec('x', mrtrix_format),
        DatasetSpec('y', mrtrix_format, pipeline1),
        DatasetSpec('z', mrtrix_format, pipeline1))


class DummyCombinedStudy(CombinedStudy):

    sub_study_specs = {'A': (DummySubStudyA, {'a': 'x', 'b': 'y', 'd': 'z'}),
                       'B': (DummySubStudyB, {'b': 'w', 'c': 'x', 'e': 'y',
                                              'f': 'z'})}

    pipeline_a1 = CombinedStudy.translate('A', DummySubStudyA.pipeline1)
    pipeline_b1 = CombinedStudy.translate('B', DummySubStudyB.pipeline1)

    _dataset_specs = set_dataset_specs(
        DatasetSpec('a', mrtrix_format),
        DatasetSpec('b', mrtrix_format),
        DatasetSpec('c', mrtrix_format),
        DatasetSpec('d', mrtrix_format, pipeline_a1),
        DatasetSpec('e', mrtrix_format, pipeline_b1),
        DatasetSpec('f', mrtrix_format, pipeline_b1))


class TestCombinedStudy(TestCase):

    PROJECT_ID = 'PROJECTID'
    SUBJECT_ID = 'SUBJECTID1'
    SESSION_ID = 'SESSIONID1'
    STUDY_NAME = 'combined'
    ONES_SLICE_IMAGE = os.path.abspath(os.path.join(test_data_dir,
                                                    'ones_slice.mif'))
    TEST_DIR = os.path.abspath(os.path.join(test_data_dir, 'study'))
    BASE_DIR = os.path.abspath(os.path.join(TEST_DIR, 'base_dir'))
    WORKFLOW_DIR = os.path.abspath(os.path.join(TEST_DIR, 'workflow_dir'))

    def test_combined_study(self):
        # Create test data on DaRIS
        self._session_id = None
        # Make cache and working dirs
        shutil.rmtree(self.TEST_DIR, ignore_errors=True)
        os.makedirs(self.WORKFLOW_DIR)
        session_dir = os.path.join(
            self.BASE_DIR, self.PROJECT_ID, self.SUBJECT_ID, self.SESSION_ID)
        os.makedirs(session_dir)
        shutil.copy(self.ONES_SLICE_IMAGE,
                    os.path.join(session_dir, 'ones.mif'))
        archive = LocalArchive(self.BASE_DIR)
        study = DummyCombinedStudy(
            self.STUDY_NAME, self.PROJECT_ID, archive,
            input_datasets={'a': Dataset('ones', mrtrix_format),
                            'b': Dataset('ones', mrtrix_format),
                            'c': Dataset('ones', mrtrix_format)})
        study.pipeline_a1().run(work_dir=self.WORKFLOW_DIR)
        study.pipeline_b1().run(work_dir=self.WORKFLOW_DIR)
        d_mean = float(sp.check_output(
            'mrstats {} -output mean'.format(
                os.path.join(session_dir, '{}_d.mif'.format(self.STUDY_NAME))),
            shell=True))
        self.assertEqual(d_mean, 2.0)
        e_mean = float(sp.check_output(
            'mrstats {} -output mean'.format(
                os.path.join(session_dir, '{}_e.mif'.format(self.STUDY_NAME))),
            shell=True))
        self.assertEqual(e_mean, 3.0)
        f_mean = float(sp.check_output(
            'mrstats {} -output mean'.format(
                os.path.join(session_dir, '{}_f.mif'.format(self.STUDY_NAME))),
            shell=True))
        self.assertEqual(f_mean, 6.0)
