#!/usr/bin/env python
from nipype import config
config.enable_debug_mode()
import os.path  # @IgnorePep8
import shutil  # @IgnorePep8
from neuroanalysis.archive import Scan  # @IgnorePep8
from neuroanalysis.mri import DiffusionDataset, NODDIDataset  # @IgnorePep8
from neuroanalysis.archive import LocalArchive  # @IgnorePep8
from neuroanalysis.file_formats import (  # @IgnorePep8
    mrtrix_format, analyze_format, fsl_bvals_format, fsl_bvecs_format)
if __name__ == '__main__':
    # Add '..' directory to path to be able to import utils.py
    import sys
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                 '..')))
    from utils import DummyTestCase as TestCase  # @UnusedImport @UnresolvedImport @IgnorePep8
else:
    from unittest import TestCase  # @Reimport


# ARCHIVE_PATH = os.path.abspath(os.path.join(
#     os.path.dirname(__file__), '..', '_data', 'test_archive'))

BASE_WORK_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', '_data', 'work', 'diffusion'))


class TestDiffusion(TestCase):

    ARCHIVE_PATH = os.path.join(os.environ['HOME'], 'Data', 'MBI', 'noddi')
    PROJECT = 'pilot'
    DATASET_NAME = 'diffusion'
    SUBJECT = 'SUBJECT1'
    SESSION = 'SESSION1'
    WORK_PATH = os.path.abspath(os.path.join(BASE_WORK_PATH, 'diffusion'))

    def setUp(self):
        shutil.rmtree(self.WORK_PATH, ignore_errors=True)
        os.makedirs(self.WORK_PATH)

    def tearDown(self):
        shutil.rmtree(self.WORK_PATH, ignore_errors=True)

    def test_preprocess(self):
        self._remove_generated_files(self.PROJECT)
        dataset = DiffusionDataset(
            name=self.DATASET_NAME,
            project_id=self.PROJECT,
            archive=LocalArchive(self.ARCHIVE_PATH),
            input_scans={
                'dwi': Scan('r_l_noddi_b700_30_directions', mrtrix_format),
                'forward_rpe': Scan('r_l_noddi_b0_6', mrtrix_format),
                'reverse_rpe': Scan('l_r_noddi_b0_6', mrtrix_format)})
        dataset.preprocess_pipeline().run(work_dir=self.WORK_PATH)
        self.assert_(
            os.path.exists(os.path.join(
                self._session_dir(self.PROJECT),
                '{}_preprocessed.mif'.format(self.DATASET_NAME))))

#     def test_brain_mask(self):
#         dataset = DiffusionDataset(
#             name=self.DATASET_NAME,
#             project_id=self.PROJECT,
#             archive=LocalArchive(self.ARCHIVE_PATH),
#             input_scans={
#                 'preprocessed': Scan('r_l_noddi_b700_30_directions',
#                                      mrtrix_format),
#                 'forward_rpe': Scan('r_l_noddi_b0_6', mrtrix_format),
#                 'reverse_rpe': Scan('l_r_noddi_b0_6', mrtrix_format)})
#         dataset.brain_mask_pipeline().run(work_dir=self.WORK_PATH)
#         self.assert_(
#             os.path.exists(os.path.join(
#                 self._session_dir(self.PROJECT),
#                 '{}_preprocessed.mif'.format(self.DATASET_NAME))))
#         self.assert_(os.path.exists(self.PREPROC_PATH))

    def _session_dir(self, project):
        return os.path.join(self.ARCHIVE_PATH, project, self.SUBJECT,
                            self.SESSION)

    def _remove_generated_files(self, project):
        # Remove processed scans
        for fname in os.listdir(self._session_dir(project)):
            if fname.startswith(self.DATASET_NAME):
                pth = os.path.join(self._session_dir(project), fname)
                os.remove(pth)


class TestNODDI(TestCase):

    ARCHIVE_PATH = os.path.join(os.environ['HOME'], 'Data', 'MBI', 'noddi')
    WORK_PATH = os.path.abspath(os.path.join(BASE_WORK_PATH, 'noddi'))
    DATASET_NAME = 'noddi'
    EXAMPLE_INPUT_PROJECT = 'example_input'
    EXAMPLE_OUTPUT_PROJECT = 'example_output'
    SUBJECT = 'SUBJECT1'
    SESSION = 'SESSION1'
    PILOT_PROJECT = 'pilot'

    def setUp(self):
        shutil.rmtree(self.WORK_PATH, ignore_errors=True)
        os.makedirs(self.WORK_PATH)

    def tearDown(self):
        import shutil  # @Reimport @NoMove This avoids some strange None error on unit-test exit @IgnorePep8
        shutil.rmtree(self.WORK_PATH, ignore_errors=True)
        for project in (self.PILOT_PROJECT, self.EXAMPLE_INPUT_PROJECT):
            self._remove_generated_files(project)

    def test_concatenate(self):
        self._remove_generated_files(self.PILOT_PROJECT)
        dataset = NODDIDataset(
            name=self.DATASET_NAME,
            project_id=self.PILOT_PROJECT,
            archive=LocalArchive(self.ARCHIVE_PATH),
            input_scans={
                'low_b_dw_scan': Scan('r_l_noddi_b700_30_directions',
                                      mrtrix_format),
                'high_b_dw_scan': Scan('r_l_noddi_b2000_60_directions',
                                       mrtrix_format)})
        dataset.concatenate_pipeline().run(work_dir=self.WORK_PATH)
        self.assert_(
            os.path.exists(os.path.join(
                self._session_dir(self.PILOT_PROJECT),
                '{}_dwi.mif'.format(self.DATASET_NAME))),
            "Concatenated file was not created")
        # TODO: More thorough testing required

    def test_noddi_fitting(self):
        self._remove_generated_files(self.EXAMPLE_INPUT_PROJECT)
        dataset = NODDIDataset(
            name=self.DATASET_NAME,
            project_id=self.EXAMPLE_INPUT_PROJECT,
            archive=LocalArchive(self.ARCHIVE_PATH),
            input_scans={'preprocessed': Scan('NODDI_DWI', analyze_format),
                         'brain_mask': Scan('brain_mask', analyze_format),
                         'gradient_directions': Scan('NODDI_protocol',
                                                     fsl_bvecs_format),
                         'bvalues': Scan('NODDI_protocol', fsl_bvals_format)})
        dataset.noddi_fitting_pipeline().run()
        for out_name in ['ficvf', 'odi', 'fiso', 'fibredirs_xvec',
                         'fibredirs_yvec', 'fibredirs_zvec', 'fmin', 'kappa',
                         'error_code']:
            self.assert_(
                os.path.exists(os.path.join(
                    self._session_dir(self.EXAMPLE_INPUT_PROJECT),
                    '{}_{}.nii'.format(self.DATASET_NAME, out_name))))

    def _remove_generated_files(self, project):
        # Remove processed scans
        for fname in os.listdir(self._session_dir(project)):
            if fname.startswith(self.DATASET_NAME):
                pth = os.path.join(self._session_dir(project), fname)
                os.remove(pth)

    def _session_dir(self, project):
        return os.path.join(self.ARCHIVE_PATH, project, self.SUBJECT,
                            self.SESSION)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--tester', default='diffusion', type=str,
                        help="Which tester to run the test from")
    parser.add_argument('--test', default='preprocess', type=str,
                        help="Which test to run")
    args = parser.parse_args()
    if args.tester == 'diffusion':
        tester = TestDiffusion()
    elif args.tester == 'noddi':
        tester = TestNODDI()
    else:
        raise Exception("Unrecognised tester '{}'")
    tester.setUp()
    try:
        getattr(tester, 'test_' + args.test)()
    except AttributeError as e:
        if str(e) == 'test_' + args.test:
            raise Exception("Unrecognised test '{}' for '{}' tester"
                            .format(args.test, args.tester))
        else:
            raise
    finally:
        tester.tearDown()
