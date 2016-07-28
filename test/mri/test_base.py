#!/usr/bin/env python
from nipype import config
config.enable_debug_mode()
import os.path  # @IgnorePep8
from nianalysis.scans import Scan, nifti_gz_format  # @IgnorePep8
from nianalysis.mri import MRDataset  # @IgnorePep8
from nianalysis.archive import LocalArchive  # @IgnorePep8
if __name__ == '__main__':
    from nianalysis.testing import DummyTestCase as TestCase  # @IgnorePep8 @UnusedImport
else:
    from nianalysis.testing import BaseImageTestCase as TestCase  # @IgnorePep8 @Reimport


class TestMRI(TestCase):

    DATASET_NAME = 'MR'

    def test_brain_mask(self):
        self._remove_generated_files(self.EXAMPLE_INPUT_PROJECT)
        dataset = MRDataset(
            name=self.DATASET_NAME,
            project_id=self.EXAMPLE_INPUT_PROJECT,
            archive=LocalArchive(self.ARCHIVE_PATH),
            input_scans={
                'mri_scan': Scan('mri_scan', nifti_gz_format)})
        dataset.brain_mask_pipeline().run()
        self.assert_(
            os.path.exists(os.path.join(
                self._session_dir(self.EXAMPLE_INPUT_PROJECT),
                '{}_brain_mask.nii.gz'.format(self.DATASET_NAME))))


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--tester', default='diffusion', type=str,
                        help="Which tester to run the test from")
    parser.add_argument('--test', default='preprocess', type=str,
                        help="Which test to run")
    args = parser.parse_args()
    tester = TestMRI()
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
