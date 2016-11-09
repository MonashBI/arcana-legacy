#!/usr/bin/env python
from nipype import config
config.enable_debug_mode()
import os.path  # @IgnorePep8
from nianalysis.base import Scan  # @IgnorePep8
from nianalysis.project.mri import DiffusionProject, NODDIProject  # @IgnorePep8
from nianalysis.archive import LocalArchive  # @IgnorePep8
from nianalysis.formats import (  # @IgnorePep8
    mrtrix_format, analyze_format, fsl_bvals_format, fsl_bvecs_format)
if __name__ == '__main__':
    from nianalysis.testing import DummyTestCase as TestCase  # @IgnorePep8 @UnusedImport
else:
    from nianalysis.testing import BaseImageTestCase as TestCase  # @IgnorePep8 @Reimport


class TestDiffusion(TestCase):

    DATASET_NAME = 'Diffusion'

    def test_preprocess(self):
        self._remove_generated_files(self.PILOT_PROJECT)
        project = DiffusionProject(
            name=self.DATASET_NAME,
            project_id=self.PILOT_PROJECT,
            archive=LocalArchive(self.ARCHIVE_PATH),
            input_scans={
                'dwi_scan':
                Scan('r_l_noddi_b700_30_directions', mrtrix_format),
                'forward_rpe': Scan('r_l_noddi_b0_6', mrtrix_format),
                'reverse_rpe': Scan('l_r_noddi_b0_6', mrtrix_format)})
        project.preprocess_pipeline().run()
        self.assert_(
            os.path.exists(os.path.join(
                self._session_dir(self.PILOT_PROJECT),
                '{}_dwi_preproc.mif'.format(self.DATASET_NAME))))

    def test_extract_b0(self):
        self._remove_generated_files(self.EXAMPLE_INPUT_PROJECT)
        project = DiffusionProject(
            name=self.DATASET_NAME,
            project_id=self.EXAMPLE_INPUT_PROJECT,
            archive=LocalArchive(self.ARCHIVE_PATH),
            input_scans={
                'dwi_preproc': Scan('NODDI_DWI', analyze_format),
                'grad_dirs': Scan('NODDI_protocol', fsl_bvecs_format),
                'bvalues': Scan('NODDI_protocol', fsl_bvals_format)})
        project.extract_b0_pipeline().run()
        self.assert_(
            os.path.exists(os.path.join(
                self._session_dir(self.EXAMPLE_INPUT_PROJECT),
                '{}_mri_scan.nii.gz'.format(self.DATASET_NAME))))

    def test_bias_correct(self):
        self._remove_generated_files(self.EXAMPLE_INPUT_PROJECT)
        project = DiffusionProject(
            name=self.DATASET_NAME,
            project_id=self.EXAMPLE_INPUT_PROJECT,
            archive=LocalArchive(self.ARCHIVE_PATH),
            input_scans={
                'dwi_preproc': Scan('NODDI_DWI', analyze_format),
                'grad_dirs': Scan('NODDI_protocol', fsl_bvecs_format),
                'bvalues': Scan('NODDI_protocol', fsl_bvals_format)})
        project.bias_correct_pipeline(mask_tool='dwi2mask').run()
        self.assert_(
            os.path.exists(os.path.join(
                self._session_dir(self.EXAMPLE_INPUT_PROJECT),
                '{}_bias_correct.nii.gz'.format(self.DATASET_NAME))))


class TestNODDI(TestCase):

    DATASET_NAME = 'NODDI'

    def test_concatenate(self):
        self._remove_generated_files(self.PILOT_PROJECT)
        project = NODDIProject(
            name=self.DATASET_NAME,
            project_id=self.PILOT_PROJECT,
            archive=LocalArchive(self.ARCHIVE_PATH),
            input_scans={
                'low_b_dw_scan': Scan('r_l_noddi_b700_30_directions',
                                      mrtrix_format),
                'high_b_dw_scan': Scan('r_l_noddi_b2000_60_directions',
                                       mrtrix_format)})
        project.concatenate_pipeline().run()
        self.assert_(
            os.path.exists(os.path.join(
                self._session_dir(self.PILOT_PROJECT),
                '{}_dwi.mif'.format(self.DATASET_NAME))),
            "Concatenated file was not created")
        # TODO: More thorough testing required

    def test_noddi_fitting(self, nthreads=6):
        self._remove_generated_files(self.EXAMPLE_INPUT_PROJECT)
        project = NODDIProject(
            name=self.DATASET_NAME,
            project_id=self.EXAMPLE_INPUT_PROJECT,
            archive=LocalArchive(self.ARCHIVE_PATH),
            input_scans={'dwi_preproc': Scan('NODDI_DWI', analyze_format),
                         'brain_mask': Scan('roi_mask', analyze_format),
                         'grad_dirs': Scan('NODDI_protocol', fsl_bvecs_format),
                         'bvalues': Scan('NODDI_protocol', fsl_bvals_format)})
        project.noddi_fitting_pipeline(nthreads=nthreads).run()
        ref_out_path = os.path.join(
            self.ARCHIVE_PATH, self.EXAMPLE_OUTPUT_PROJECT, self.SUBJECT,
            self.SESSION)
        gen_out_path = os.path.join(
            self.ARCHIVE_PATH, self.EXAMPLE_INPUT_PROJECT, self.SUBJECT,
            self.SESSION)
        for out_name, mean, stdev in [('ficvf', 1e-5, 1e-2),
                                      ('odi', 1e-4, 1e-2),
                                      ('fiso', 1e-4, 1e-2),
                                      ('fibredirs_xvec', 1e-3, 1e-1),
                                      ('fibredirs_yvec', 1e-3, 1e-1),
                                      ('fibredirs_zvec', 1e-3, 1e-1),
                                      ('kappa', 1e-4, 1e-1)]:
            self.assertImagesAlmostMatch(
                os.path.join(ref_out_path, 'example_{}.nii'.format(out_name)),
                os.path.join(gen_out_path,
                             '{}_{}.nii'.format(self.DATASET_NAME, out_name)),
                mean_threshold=mean, stdev_threshold=stdev)


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
