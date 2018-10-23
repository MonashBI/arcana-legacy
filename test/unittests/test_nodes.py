from nipype.interfaces.utility import Merge, Split
from arcana.data import (
    AcquiredFilesetSpec, FilesetSpec, FilesetSelector, FieldSpec)
from arcana.study.base import Study, StudyMetaClass
from arcana.exception import (ArcanaModulesNotInstalledException,
                              ArcanaError)
import unittest
from arcana.testing import BaseTestCase, TestMath
from arcana.data.file_format.standard import text_format
from arcana.environment import Requirement, ModulesEnvironment
from arcana.processor import LinearProcessor
from future.utils import with_metaclass


notinstalled1_req = Requirement(name='notinstalled1', min_version=(1, 0))
notinstalled2_req = Requirement(name='notinstalled2', min_version=(1, 0))
first_req = Requirement('mrtrix', min_version=(0, 15, 9))
second_req = Requirement('dcm2niix', min_version=(1, 0, 2))

try:
    ModulesEnvironment._run_module_cmd('avail')
except ArcanaModulesNotInstalledException:
    MODULES_NOT_INSTALLED = True
else:
    MODULES_NOT_INSTALLED = False


class TestMathWithReq(TestMath):

    def _run_interface(self, runtime):
        loaded_modules = ModulesEnvironment.preloaded()
        if first_req.name not in loaded_modules:
            raise ArcanaError(
                "Mrtrix module was not loaded in Node")
        if second_req.name not in loaded_modules:
            raise ArcanaError(
                "Dcm2niix module was not loaded in Node")
        return runtime


class RequirementsStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        AcquiredFilesetSpec('ones', text_format),
        FilesetSpec('twos', text_format, 'pipeline1'),
        FieldSpec('threes', float, 'pipeline2'),
        FieldSpec('fours', float, 'pipeline2')]

    def pipeline1(self):
        pipeline = self.pipeline(
            name='pipeline1',
            desc=("A pipeline that tests loading of requirements"),
            references=[],)
        # Convert from DICOM to NIfTI.gz format on input
        maths = pipeline.create_node(
            TestMathWithReq(), "maths", requirements=[
                (notinstalled1_req, notinstalled2_req,
                 first_req), second_req])
        maths.inputs.op = 'add'
        maths.inputs.as_file = True
        maths.inputs.y = 1
        pipeline.connect_input('ones', maths, 'x', text_format)
        pipeline.connect_output('twos', maths, 'z', text_format)
        return pipeline

    def pipeline2(self):
        pipeline = self.pipeline(
            name='pipeline2',
            desc=("A pipeline that tests loading of requirements in "
                  "map nodes"),
            references=[],)
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
        pipeline.connect_input('ones', merge, 'in1', text_format)
        pipeline.connect_input('twos', merge, 'in2', text_format)
        pipeline.connect(merge, 'out', maths, 'x')
        pipeline.connect(maths, 'z', split, 'inlist')
        pipeline.connect_output('threes', split, 'out1', text_format)
        pipeline.connect_output('fours', split, 'out2', text_format)
        return pipeline


class TestModuleLoad(BaseTestCase):

    INPUT_DATASETS = {'ones': '1'}

    @property
    def processor(self):
        return LinearProcessor(
            self.work_dir)

    @unittest.skipIf(MODULES_NOT_INSTALLED,
                     "Dcm2niix and Mrtrix modules are not installed")
    def test_module_load(self):
        study = self.create_study(
            RequirementsStudy, 'requirements',
            [FilesetSelector('ones', text_format, 'ones')])
        self.assertContentsEqual(study.data('twos'), 2.0)
        self.assertEqual(ModulesEnvironment.preloaded(), {})

    @unittest.skipIf(MODULES_NOT_INSTALLED,
                     "Dcm2niix and Mrtrix modules are not installed")
    def test_module_load_in_map(self):
        study = self.create_study(
            RequirementsStudy, 'requirements',
            [FilesetSelector('ones', text_format, 'ones')])
        threes = study.data('threes')
        fours = study.data('fours')
        self.assertEqual(next(iter(threes)).value, 3)
        self.assertEqual(next(iter(fours)).value, 4)
        self.assertEqual(ModulesEnvironment.preloaded(), {})
