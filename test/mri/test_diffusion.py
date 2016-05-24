#!/usr/bin/env python
from nipype import config
config.enable_debug_mode()
import os.path  # @IgnorePep8
import shutil  # @IgnorePep8
from neuroanalysis.base import Scan  # @IgnorePep8
from neuroanalysis.mri.diffusion import DiffusionDataset, NODDIDataset  # @IgnorePep8
from neuroanalysis.archive import LocalArchive  # @IgnorePep8
if __name__ == '__main__':
    # Add '..' directory to path to be able to import utils.py
    import sys
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                 '..')))
    from utils import DummyTestCase as TestCase  # @UnusedImport @UnresolvedImport @IgnorePep8
else:
    from unittest import TestCase  # @Reimport


ARCHIVE_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', '_data', 'test_archive'))
BASE_WORK_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', '_data', 'work', 'diffusion'))


class TestDiffusion(TestCase):

    NODDI_PROJECT = 'noddi-test'
    NODDI_SUBJECT = 'PILOT1'
    NODDI_SESSION = 'SESSION1'
    WORK_PATH = os.path.abspath(os.path.join(BASE_WORK_PATH, 'diffusion'))

    def setUp(self):
        shutil.rmtree(self.WORK_PATH, ignore_errors=True)
        os.makedirs(self.WORK_PATH)
        self.dataset = DiffusionDataset(
            project_id=self.NODDI_PROJECT, archive=LocalArchive(ARCHIVE_PATH),
            scans={'dw_scan': Scan('r_l_noddi_b700_30_directions',
                                     'mrtrix'),
                   'forward_rpe': Scan('r_l_noddi_b0_6', 'mrtrix'),
                   'reverse_rpe': Scan('pre_l_r_noddi_b0_6', 'mrtrix')})

    def tearDown(self):
        shutil.rmtree(self.WORK_PATH, ignore_errors=True)

    def test_preprocess(self):
        self.dataset.preprocess_pipeline().run()


class TestNODDI(TestCase):

    NODDI_PROJECT = 'noddi-test'
    NODDI_SUBJECT = 'PILOT1'
    NODDI_SESSION = 'SESSION1'
    WORK_PATH = os.path.abspath(os.path.join(BASE_WORK_PATH, 'noddi'))
    SESSION_DIR = os.path.join(ARCHIVE_PATH, NODDI_PROJECT,
                               NODDI_SUBJECT, NODDI_SESSION)
    DATASET_NAME = 'noddi'
    SCAN_PATH_PREFIX = os.path.join(SESSION_DIR, DATASET_NAME)
    DW_SCAN_PATH = SCAN_PATH_PREFIX + '_dw_scan.mif'
    PREPROC_PATH = SCAN_PATH_PREFIX + '_preprocessed.mif'

    def setUp(self):
        self._remove_generated_files()
        os.makedirs(self.WORK_PATH)
        self.dataset = NODDIDataset(
            name=self.DATASET_NAME,
            project_id=self.NODDI_PROJECT, archive=LocalArchive(ARCHIVE_PATH),
            scans={'low_b_dw_scan': Scan('r_l_noddi_b700_30_directions',
                                         'mrtrix'),
                   'high_b_dw_scan': Scan('r_l_noddi_b2000_60_directions',
                                          'mrtrix'),
                   'forward_rpe': Scan('r_l_noddi_b0_6', 'mrtrix'),
                   'reverse_rpe': Scan('l_r_noddi_b0_6', 'mrtrix')})

    def tearDown(self):
        self._remove_generated_files()

    def test_concatenate(self):
        self.dataset.concatenate_pipeline().run()
        self.assert_(os.path.exists(self.DW_SCAN_PATH))

    def test_preprocess(self):
        self.dataset.preprocess_pipeline().run()
        self.assert_(os.path.exists(self.PREPROC_PATH))

    def test_brain_mask(self):
        self.dataset.brain_mask_pipeline().run()
        self.assert_(os.path.exists(self.PREPROC_PATH))

    def _remove_generated_files(self):
        shutil.rmtree(self.WORK_PATH, ignore_errors=True)
        try:
            os.remove(self.DW_SCAN_PATH)
        except:
            pass
        try:
            os.remove(self.PREPROC_PATH)
        except:
            pass


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
