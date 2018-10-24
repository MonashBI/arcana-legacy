from future import standard_library
standard_library.install_aliases()
from unittest import TestCase  # @IgnorePep8
import tempfile  # @IgnorePep8
from future.utils import PY2  # @IgnorePep8
import shutil  # @IgnorePep8
import os.path  # @IgnorePep8
from arcana.processor import LinearProcessor, MultiProcProcessor, SlurmProcessor  # @IgnorePep8
if PY2:
    import pickle as pkl  # @UnusedImport
else:
    import pickle as pkl  # @Reimport


class TestProcessorPickle(TestCase):

    def setUp(self):
        self.work_dir = tempfile.mkdtemp()
        self.pkl_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.work_dir, ignore_errors=True)
        shutil.rmtree(self.pkl_dir, ignore_errors=True)

    def test_linear_pickle(self):
        processor = LinearProcessor(self.work_dir)
        pkl_path = os.path.join(self.pkl_dir, 'linear.pkl')
        with open(pkl_path, 'wb') as f:
            pkl.dump(processor, f)
        with open(pkl_path, 'rb') as f:
            reread_processor = pkl.load(f)
        self.assertEqual(processor, reread_processor)

    def test_multiproc_pickle(self):
        processor = MultiProcProcessor(self.work_dir, num_processes=1)
        pkl_path = os.path.join(self.pkl_dir, 'multiproc.pkl')
        with open(pkl_path, 'wb') as f:
            pkl.dump(processor, f)
        with open(pkl_path, 'rb') as f:
            reread_processor = pkl.load(f)
        self.assertEqual(processor, reread_processor)

    def test_slurm_pickle(self):
        processor = SlurmProcessor(self.work_dir, email='manager@arcana.com')
        pkl_path = os.path.join(self.pkl_dir, 'slurm.pkl')
        with open(pkl_path, 'wb') as f:
            pkl.dump(processor, f)
        with open(pkl_path, 'rb') as f:
            reread_processor = pkl.load(f)
        self.assertEqual(processor, reread_processor)
