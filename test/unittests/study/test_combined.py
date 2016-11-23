import os.path
import shutil
from unittest import TestCase
import subprocess as sp
from nipype.pipeline import engine as pe
from nianalysis.dataset import Dataset, DatasetSpec
from nianalysis.data_formats import nifti_gz_format, mrtrix_format
from nianalysis.requirements import mrtrix3_req
from nianalysis.study.base import Study, set_dataset_specs
from nianalysis.study.combined import CombinedStudy
from nianalysis.interfaces.mrtrix import MRConvert, MRCat, MRMath
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
        mrconvert = pe.Node(MRConvert(), name="convert1")
        # Connect inputs
        pipeline.connect_input('x', mrconvert, 'in_file')
        # Connect outputs
        pipeline.connect_output('z', mrconvert, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    _dataset_specs = set_dataset_specs(
        DatasetSpec('x', nifti_gz_format),
        DatasetSpec('y', nifti_gz_format),
        DatasetSpec('z', nifti_gz_format, pipeline1))


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
        mrconvert = pe.Node(MRConvert(), name="convert1")
        # Connect inputs
        pipeline.connect_input('x', mrconvert, 'in_file')
        # Connect outputs
        pipeline.connect_output('z', mrconvert, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    _dataset_specs = set_dataset_specs(
        DatasetSpec('w', nifti_gz_format),
        DatasetSpec('x', nifti_gz_format),
        DatasetSpec('y', nifti_gz_format, pipeline1),
        DatasetSpec('z', nifti_gz_format, pipeline1))


class DummyCombinedStudy(CombinedStudy):

    sub_study_specs = {'A': (DummySubStudyA, {'x': 'a', 'y': 'b', 'z': 'd'}),
                       'B': (DummySubStudyB, {'w': 'b', 'x': 'c', 'y': 'e',
                                              'z': 'f'})}

    pipeline_a1 = CombinedStudy.translate('A', DummySubStudyA.pipeline1)
    pipeline_b1 = CombinedStudy.translate('B', DummySubStudyB.pipeline1)

    _dataset_specs = set_dataset_specs(
        DatasetSpec('a', nifti_gz_format),
        DatasetSpec('b', nifti_gz_format),
        DatasetSpec('c', nifti_gz_format),
        DatasetSpec('d', nifti_gz_format, pipeline_a1),
        DatasetSpec('e', nifti_gz_format, pipeline_b1),
        DatasetSpec('f', nifti_gz_format, pipeline_b1))


class TestCombinedStudy(TestCase):

    PROJECT_ID = 'PROJECTID'
    SUBJECT_IDS = ['SUBJECTID1', 'SUBJECTID2', 'SUBJECTID3']
    SESSION_IDS = ['SESSIONID1', 'SESSIONID2']
    TEST_IMAGE = os.path.abspath(os.path.join(test_data_dir,
                                              'test_image.nii.gz'))
    ONES_SLICE_IMAGE = os.path.abspath(os.path.join(test_data_dir,
                                                    'ones_slice.mif'))
    TEST_DIR = os.path.abspath(os.path.join(test_data_dir, 'study'))
    BASE_DIR = os.path.abspath(os.path.join(TEST_DIR, 'base_dir'))
    WORKFLOW_DIR = os.path.abspath(os.path.join(TEST_DIR, 'workflow_dir'))

    def setUp(self):
        # Create test data on DaRIS
        self._session_id = None
        # Make cache and working dirs
        shutil.rmtree(self.TEST_DIR, ignore_errors=True)
        os.makedirs(self.WORKFLOW_DIR)
        self.subject_paths = []
        self.session_paths = []
        for subject_id in self.SUBJECT_IDS:
            subject_path = os.path.join(self.BASE_DIR, self.PROJECT_ID,
                                        subject_id)
            self.subject_paths.append(subject_path)
            for session_id in self.SESSION_IDS:
                session_path = os.path.join(subject_path, session_id)
                self.session_paths.append(session_path)
                os.makedirs(session_path)
                shutil.copy(self.TEST_IMAGE,
                            os.path.join(session_path, 'start.nii.gz'))
                shutil.copy(self.ONES_SLICE_IMAGE,
                            os.path.join(session_path, 'ones_slice.mif'))
        archive = LocalArchive(self.BASE_DIR)
        self.study = DummyStudy(
            'TestDummy', self.PROJECT_ID, archive,
            input_datasets={'start': Dataset('start', nifti_gz_format),
                            'ones_slice': Dataset('ones_slice',
                                                  mrtrix_format)})

    def tearDown(self):
        # Clean up working dirs
        shutil.rmtree(self.TEST_DIR, ignore_errors=True)

    def test_combined_study(self):
        