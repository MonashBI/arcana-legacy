import logging
import tempfile
import shutil
from arcana.processor import SlurmProc
from nipype.interfaces.utility import IdentityInterface
from unittest import TestCase
from arcana.environment.base import Node
from arcana.environment import StaticEnv


logger = logging.getLogger('arcana')

DEFAULT_MEMORY = 4096
DEFAULT_WALL_TIME = 20


class TestSlurmTemplate(TestCase):

    def setUp(self):
        self.work_dir = tempfile.mkdtemp()
        self.processor = SlurmProc(
            self.work_dir, account='test_account', email='test@email.org',
            partition='m3a')

    def tearDown(self):
        shutil.rmtree(self.work_dir)

    def test_template(self):
        n = Node(environment=StaticEnv(), interface=IdentityInterface('x'),
                 name='x', wall_time=150,
                 n_procs=10, mem_gb=2, gpu=True)
        generated = self.processor.slurm_template(n).strip()
        self.assertEqual(
            generated, ref_template.strip(),
            '\n{}\n----\n{}'.format(generated, ref_template))

    def test_wall_time(self):
        self.assertEqual(self.processor.wall_time_str(1550.5), '1-01:50:30')
        self.assertEqual(self.processor.wall_time_str(1.75), '0-00:01:45')
        self.assertEqual(self.processor.wall_time_str(725), '0-12:05:00')


ref_template = """
#!/bin/bash

# Set the email
#SBATCH --email=test@email.org

# Request CPU resource for a parallel job
#SBATCH --ntasks=10

# Memory usage (MB)
#SBATCH --mem-per-cpu=2000

# Set your minimum acceptable walltime, format: day-hours:minutes:seconds
#SBATCH --time=0-02:30:00

# Kill job if dependencies fail
#SBATCH --kill-on-invalid-dep=yes

# Set the account
#SBATCH --account=test_account

# Set the partition to run the job on
#SBATCH --partition=m3a

# Set mail triggers
#SBATCH --mail-type=FAIL

# Node and CPU options
"""
