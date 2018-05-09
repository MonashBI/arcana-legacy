from arcana.interfaces.mrtrix import MRMath
from nipype.interfaces.utility import IdentityInterface, Merge
from arcana.dataset import DatasetSpec, DatasetMatch
from arcana.study.base import Study, StudyMetaClass
from arcana.testing import BaseTestCase
from unittest import TestCase
from mbianalysis.data_format import nifti_gz_format
from mbianalysis.requirement import (
    dcm2niix1_req, mrtrix3_req)
from arcana.node import Node
from arcana.requirement import Requirement


dummy1_req = Requirement(name='dummy1', min_version=(1, 0))
dummy2_req = Requirement(name='dummy2', min_version=(1, 0))


class RequirementsStudy(Study):

    __metaclass__ = StudyMetaClass

    add_data_specs = [
        DatasetSpec('ones', nifti_gz_format),
        DatasetSpec('twos', nifti_gz_format, 'pipeline')]

    def pipeline(self):
        pipeline = self.create_pipeline(
            name='pipeline',
            inputs=[DatasetSpec('ones', nifti_gz_format)],
            outputs=[DatasetSpec('twos', nifti_gz_format)],
            desc=("A pipeline that tests loading of requirements"),
            version=1,
            citations=[],)
        # Convert from DICOM to NIfTI.gz format on input
        input_merge = pipeline.create_node(
            Merge(2), "input_merge")
        maths = pipeline.create_node(
            MRMath(), "maths", requirements=[
                (dummy1_req, dummy2_req, mrtrix3_req), dcm2niix1_req])
        pipeline.connect_input('ones', input_merge, 'in1')
        pipeline.connect_input('ones', input_merge, 'in2')
        pipeline.connect(input_merge, 'out', maths, 'in_files')
        maths.inputs.operation = 'sum'
        pipeline.connect_output('twos', maths, 'out_file')
        pipeline.assert_connected()
        return pipeline


class TestModuleLoad(BaseTestCase):

    def test_pipeline_prerequisites(self):
        study = self.create_study(
            RequirementsStudy, 'requirements',
            [DatasetMatch('ones', nifti_gz_format, 'ones')])
        study.data('twos')
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
