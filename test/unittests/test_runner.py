from future import standard_library
standard_library.install_aliases()
from unittest import TestCase  # @IgnorePep8
import tempfile  # @IgnorePep8
from future.utils import PY2  # @IgnorePep8
import shutil  # @IgnorePep8
import os.path  # @IgnorePep8
from arcana.runner import LinearRunner, MultiProcRunner, SlurmRunner  # @IgnorePep8
if PY2:
    import pickle as pkl  # @UnusedImport
else:
    import pickle as pkl  # @Reimport


class TestRunnerPickle(TestCase):

    def setUp(self):
        self.work_dir = tempfile.mkdtemp()
        self.pkl_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.work_dir, ignore_errors=True)
        shutil.rmtree(self.pkl_dir, ignore_errors=True)

    def test_linear_pickle(self):
        runner = LinearRunner(self.work_dir)
        pkl_path = os.path.join(self.pkl_dir, 'linear.pkl')
        with open(pkl_path, 'wb') as f:
            pkl.dump(runner, f)
        with open(pkl_path, 'rb') as f:
            reread_runner = pkl.load(f)
        self.assertEqual(runner, reread_runner)

    def test_multiproc_pickle(self):
        runner = MultiProcRunner(self.work_dir, num_processes=1)
        pkl_path = os.path.join(self.pkl_dir, 'multiproc.pkl')
        with open(pkl_path, 'wb') as f:
            pkl.dump(runner, f)
        with open(pkl_path, 'rb') as f:
            reread_runner = pkl.load(f)
        self.assertEqual(runner, reread_runner)

    def test_slurm_pickle(self):
        runner = SlurmRunner(self.work_dir, email='manager@arcana.com')
        pkl_path = os.path.join(self.pkl_dir, 'slurm.pkl')
        with open(pkl_path, 'wb') as f:
            pkl.dump(runner, f)
        with open(pkl_path, 'rb') as f:
            reread_runner = pkl.load(f)
        self.assertEqual(runner, reread_runner)
