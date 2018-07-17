from nipype.interfaces.utility import IdentityInterface, Merge, Split
from arcana.dataset import DatasetSpec, DatasetMatch, FieldSpec
from arcana.study.base import Study, StudyMetaClass
from arcana.exception import (ArcanaModulesNotInstalledException,
                              ArcanaRequirementVersionException,
                              ArcanaError)
import unittest
from arcana.testing import BaseTestCase, TestMath
from unittest import TestCase
from arcana.dataset.file_format.standard import text_format
from arcana.node import Node
from arcana.requirement import Requirement
from future.utils import with_metaclass


notinstalled1_req = Requirement(name='notinstalled1', min_version=(1, 0))
notinstalled2_req = Requirement(name='notinstalled2', min_version=(1, 0))
first_req = Requirement('firstmodule', min_version=(0, 15, 9))
second_req = Requirement('secondmodule', min_version=(1, 0, 2))

try:
    avail_modules = Node.available_modules()
    for req in (first_req, second_req):
        Requirement.best_requirement([req], avail_modules)
except (ArcanaModulesNotInstalledException,
        ArcanaRequirementVersionException):
    MODULES_NOT_INSTALLED = True
else:
    MODULES_NOT_INSTALLED = False


class TestMathWithReq(TestMath):

    def _run_interface(self, runtime):
        loaded_modules = Node.preloaded_modules()
        if first_req.name not in loaded_modules:
            raise ArcanaError(
                "Mrtrix module was not loaded in Node")
        if second_req.name not in loaded_modules:
            raise ArcanaError(
                "Dcm2niix module was not loaded in Node")
        return runtime


class RequirementsStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        DatasetSpec('ones', text_format),
        DatasetSpec('twos', text_format, 'pipeline'),
        FieldSpec('threes', float, 'pipeline2'),
        FieldSpec('fours', float, 'pipeline2')]

    def pipeline(self):
        pipeline = self.create_pipeline(
            name='pipeline',
            inputs=[DatasetSpec('ones', text_format)],
            outputs=[DatasetSpec('twos', text_format)],
            desc=("A pipeline that tests loading of requirements"),
            version=1,
            citations=[],)
        # Convert from DICOM to NIfTI.gz format on input
        maths = pipeline.create_node(
            TestMathWithReq(), "maths", requirements=[
                (notinstalled1_req, notinstalled2_req,
                 first_req), second_req])
        maths.inputs.op = 'add'
        maths.inputs.as_file = True
        maths.inputs.y = 1
        pipeline.connect_input('ones', maths, 'x')
        pipeline.connect_output('twos', maths, 'z')
        pipeline.assert_connected()
        return pipeline

    def pipeline2(self):
        pipeline = self.create_pipeline(
            name='pipeline2',
            inputs=[DatasetSpec('ones', text_format),
                    DatasetSpec('twos', text_format)],
            outputs=[FieldSpec('threes', float),
                     FieldSpec('fours', float)],
            desc=("A pipeline that tests loading of requirements in "
                  "map nodes"),
            version=1,
            citations=[],)
        # Convert from DICOM to NIfTI.gz format on input
        merge = pipeline.create_node(Merge(2), "merge")
        maths = pipeline.create_map_node(
            TestMathWithReq(), "maths", iterfield='x', requirements=[
                (notinstalled1_req, notinstalled2_req,
                 first_req), second_req])
        split = pipeline.create_node(Split(), 'split')
        split.inputs.splits = [1, 1]
        split.inputs.squeeze = True
        maths.inputs.op = 'add'
        maths.inputs.y = 2
        pipeline.connect_input('ones', merge, 'in1')
        pipeline.connect_input('twos', merge, 'in2')
        pipeline.connect(merge, 'out', maths, 'x')
        pipeline.connect(maths, 'z', split, 'inlist')
        pipeline.connect_output('threes', split, 'out1')
        pipeline.connect_output('fours', split, 'out2')
        pipeline.assert_connected()
        return pipeline


class TestModuleLoad(BaseTestCase):

    INPUT_DATASETS = {'ones': '1'}

    @unittest.skipIf(MODULES_NOT_INSTALLED,
                     "Dcm2niix and Mrtrix modules are not installed")
    def test_module_load(self):
        study = self.create_study(
            RequirementsStudy, 'requirements',
            [DatasetMatch('ones', text_format, 'ones')])
        self.assertContentsEqual(study.data('twos'), 2.0)

    @unittest.skipIf(MODULES_NOT_INSTALLED,
                     "Dcm2niix and Mrtrix modules are not installed")
    def test_module_load_in_map(self):
        study = self.create_study(
            RequirementsStudy, 'requirements',
            [DatasetMatch('ones', text_format, 'ones')])
        threes = study.data('threes')
        fours = study.data('fours')
        self.assertEqual(next(iter(threes)).value, 3)
        self.assertEqual(next(iter(fours)).value, 4)


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
