from nipype.interfaces.fsl.maths import BinaryMaths
from nipype.interfaces.utility import IdentityInterface
from nianalysis.dataset import DatasetSpec, Dataset
from nianalysis.data_formats import nifti_gz_format
from nianalysis.study.base import Study, set_data_specs
from nianalysis.testing import BaseTestCase
from unittest import TestCase
from nianalysis.requirements import fsl5_req, mrtrix3_req
from nianalysis.nodes import Node
from nianalysis.requirements import Requirement
import logging

logger = logging.getLogger('NiAnalysis')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


dummy1_req = Requirement(name='dummy1', min_version=(1, 0))
dummy2_req = Requirement(name='dummy2', min_version=(1, 0))


class RequirementsStudy(Study):

    def pipeline(self):
        pipeline = self.create_pipeline(
            name='pipeline',
            inputs=[DatasetSpec('ones', nifti_gz_format)],
            outputs=[DatasetSpec('twos', nifti_gz_format)],
            description=("A pipeline that tests loading of requirements"),
            default_options={},
            version=1,
            citations=[],)
        # Convert from DICOM to NIfTI.gz format on input
        maths = pipeline.create_node(
            BinaryMaths(), "maths", requirements=[
                (dummy1_req, dummy2_req, fsl5_req), mrtrix3_req])
        maths.inputs.output_type = 'NIFTI_GZ'
        pipeline.connect_input('ones', maths, 'in_file')
        pipeline.connect_input('ones', maths, 'operand_file')
        maths.inputs.operation = 'add'
        pipeline.connect_output('twos', maths, 'out_file')
        pipeline.assert_connected()
        return pipeline

    _data_specs = set_data_specs(
        DatasetSpec('ones', nifti_gz_format),
        DatasetSpec('twos', nifti_gz_format, pipeline))


class TestModuleLoad(BaseTestCase):

    def test_pipeline_prerequisites(self):
        study = self.create_study(
            RequirementsStudy, 'requirements',
            {'ones': Dataset('ones', nifti_gz_format)})
        study.pipeline().run(work_dir=self.work_dir)
        self.assertDatasetCreated('twos.nii.gz', study.name)


class TestSlurmTemplate(TestCase):

    def test_template(self):
        x = Node(IdentityInterface('x'), name='x', wall_time=150,
                 nthreads=10, memory=2000, gpu=True,
                 account='test_account')
        self.assertEqual(
            x.slurm_template.strip(), ref_template.strip(),
            '{}\n----\n{}'.format(x.slurm_template, ref_template))

    def test_wall_time(self):
        x = Node(IdentityInterface('x'), name='x', wall_time=1550.5)
        self.assertEqual(x.wall_time_str, '1-01:50:30')
        y = Node(IdentityInterface('y'), name='y', wall_time=1.75)
        self.assertEqual(y.wall_time_str, '0-00:01:45')
        z = Node(IdentityInterface('z'), name='z', wall_time=725)
        self.assertEqual(z.wall_time_str, '0-12:05:00')


ref_template = """
#!/bin/bash

# Set the partition to run the job on
#SBATCH --partition=m3c

# Request CPU resource for a parallel job, for example:
#   4 Nodes each with 12 Cores/MPI processes
#SBATCH --ntasks=10
# SBATCH --ntasks-per-node=12
# SBATCH --cpus-per-task=1

# Memory usage (MB)
#SBATCH --mem-per-cpu=2000

# Set your minimum acceptable walltime, format: day-hours:minutes:seconds
#SBATCH --time=0-02:30:00

# Kill job if dependencies fail
#SBATCH --kill-on-invalid-dep=yes

# Use reserved node to run job when a node reservation is made for you already
# SBATCH --reservation=reservation_name
#SBATCH --gres=gpu:1
#SBATCH --account=test_account
"""
