from nipype.interfaces.utility import Merge, Split
from arcana.data import FilesetSpec, FilesetMatch, FieldSpec
from arcana.study.base import Study, StudyMetaClass
from arcana.exception import (ArcanaModulesNotInstalledException,
                              ArcanaError)
import unittest
from arcana.testing import BaseTestCase, TestMath
from arcana.data.file_format.standard import text_format
from arcana.requirement import Requirement, ModulesRequirementManager
from arcana.processor import LinearProcessor
from future.utils import with_metaclass


notinstalled1_req = Requirement(name='notinstalled1', min_version=(1, 0))
notinstalled2_req = Requirement(name='notinstalled2', min_version=(1, 0))
first_req = Requirement('mrtrix', min_version=(0, 15, 9))
second_req = Requirement('dcm2niix', min_version=(1, 0, 2))

try:
    ModulesRequirementManager._run_module_cmd('avail')
except ArcanaModulesNotInstalledException:
    MODULES_NOT_INSTALLED = True
else:
    MODULES_NOT_INSTALLED = False


class TestMathWithReq(TestMath):

    def _run_interface(self, runtime):
        loaded_modules = ModulesRequirementManager.preloaded()
        if first_req.name not in loaded_modules:
            raise ArcanaError(
                "Mrtrix module was not loaded in Node")
        if second_req.name not in loaded_modules:
            raise ArcanaError(
                "Dcm2niix module was not loaded in Node")
        return runtime


class RequirementsStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        FilesetSpec('ones', text_format),
        FilesetSpec('twos', text_format, 'pipeline'),
        FieldSpec('threes', float, 'pipeline2'),
        FieldSpec('fours', float, 'pipeline2')]

    def pipeline(self):
        pipeline = self.pipeline(
            name='pipeline',
            inputs=[FilesetSpec('ones', text_format)],
            outputs=[FilesetSpec('twos', text_format)],
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
        pipeline.connect_input('ones', maths, 'x')
        pipeline.connect_output('twos', maths, 'z')
        pipeline.assert_connected()
        return pipeline

    def pipeline2(self):
        pipeline = self.pipeline(
            name='pipeline2',
            inputs=[FilesetSpec('ones', text_format),
                    FilesetSpec('twos', text_format)],
            outputs=[FieldSpec('threes', float),
                     FieldSpec('fours', float)],
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

    @property
    def processor(self):
        return LinearProcessor(
            self.work_dir,
            requirement_manager=ModulesRequirementManager())

    @unittest.skipIf(MODULES_NOT_INSTALLED,
                     "Dcm2niix and Mrtrix modules are not installed")
    def test_module_load(self):
        study = self.create_study(
            RequirementsStudy, 'requirements',
            [FilesetMatch('ones', text_format, 'ones')])
        self.assertContentsEqual(study.data('twos'), 2.0)
        self.assertEqual(ModulesRequirementManager.preloaded(), {})

    @unittest.skipIf(MODULES_NOT_INSTALLED,
                     "Dcm2niix and Mrtrix modules are not installed")
    def test_module_load_in_map(self):
        study = self.create_study(
            RequirementsStudy, 'requirements',
            [FilesetMatch('ones', text_format, 'ones')])
        threes = study.data('threes')
        fours = study.data('fours')
        self.assertEqual(next(iter(threes)).value, 3)
        self.assertEqual(next(iter(fours)).value, 4)
        self.assertEqual(ModulesRequirementManager.preloaded(), {})
