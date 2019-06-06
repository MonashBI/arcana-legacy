import os
from unittest import TestCase
from nipype.interfaces.utility import Merge, Split
from arcana.data import (
    InputFilesetSpec, FilesetSpec, InputFilesets, FieldSpec)
from arcana.study.base import Study, StudyMetaClass
from arcana.exceptions import (
    ArcanaModulesNotInstalledException, ArcanaError, ArcanaModulesError)
import unittest
from arcana.utils.testing import BaseTestCase, TestMath
from arcana.data.file_format import text_format
from arcana.environment import ModulesEnv
from arcana.processor import SingleProc
from future.utils import with_metaclass
from arcana.environment import BaseRequirement


class DummyRequirement(BaseRequirement):

    def detect_version_str(self):
        try:
            return os.environ[self.name.upper() + '_VERSION']
        except KeyError:
            loaded_modules = ModulesEnv.loaded()
            raise ArcanaError(
                "Did not find {} in environment variables, found '{}'. "
                "The loaded modules are {}"
                .format(self.name.upper() + '_VERSION',
                        "', '".join(os.environ.keys()),
                        ', '.join(loaded_modules)))


first_req = DummyRequirement('firsttestmodule')
second_req = DummyRequirement('secondtestmodule')

try:
    ModulesEnv._run_module_cmd('avail')
except ArcanaModulesNotInstalledException:
    MODULES_NOT_INSTALLED = True
else:
    MODULES_NOT_INSTALLED = False

SKIP_ARGS = (MODULES_NOT_INSTALLED, "Environment modules are not installed")


class TestMathWithReq(TestMath):

    def _run_interface(self, runtime):
        loaded_modules = ModulesEnv.loaded()
        if first_req.name not in loaded_modules:
            raise ArcanaError(
                "First Test module was not loaded in Node")
        if second_req.name not in loaded_modules:
            raise ArcanaError(
                "Second Test module was not loaded in Node")
        return runtime


class RequirementsStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        InputFilesetSpec('ones', text_format),
        FilesetSpec('twos', text_format, 'pipeline1'),
        FieldSpec('threes', float, 'pipeline2'),
        FieldSpec('fours', float, 'pipeline2')]

    def pipeline1(self, **name_maps):
        pipeline = self.new_pipeline(
            name='pipeline1',
            desc=("A pipeline that tests loading of requirements"),
            name_maps=name_maps)
        # Convert from DICOM to NIfTI.gz format on input
        maths = pipeline.add(
            "maths", TestMathWithReq(),
            requirements=[first_req.v('0.15.9'), second_req.v('1.0.2')])
        maths.inputs.op = 'add'
        maths.inputs.as_file = True
        maths.inputs.y = 1
        pipeline.connect_input('ones', maths, 'x', text_format)
        pipeline.connect_output('twos', maths, 'z', text_format)
        return pipeline

    def pipeline2(self, **name_maps):
        pipeline = self.new_pipeline(
            name='pipeline2',
            desc=("A pipeline that tests loading of requirements in "
                  "map nodes"),
            name_maps=name_maps)
        # Convert from DICOM to NIfTI.gz format on input
        merge = pipeline.add("merge", Merge(2))
        maths = pipeline.add(
            "maths", TestMathWithReq(), iterfield='x', requirements=[
                first_req.v('0.15.9'), second_req.v('1.0.2')])
        split = pipeline.add('split', Split())
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

    INPUT_FILESETS = {'ones': '1'}

    @property
    def processor(self):
        return SingleProc(
            self.work_dir)

    @unittest.skipIf(*SKIP_ARGS)
    def test_module_load(self):
        study = self.create_study(
            RequirementsStudy, 'requirements',
            {'ones': 'ones'},
            environment=ModulesEnv())
        self.assertContentsEqual(study.data('twos'), 2.0)
        self.assertEqual(ModulesEnv.loaded(), {})

    @unittest.skipIf(*SKIP_ARGS)
    def test_module_load_in_map(self):
        study = self.create_study(
            RequirementsStudy, 'requirements',
            [InputFilesets('ones', 'ones', text_format)],
            environment=ModulesEnv())
        threes = study.data('threes')
        fours = study.data('fours')
        self.assertEqual(next(iter(threes)).value, 3)
        self.assertEqual(next(iter(fours)).value, 4)
        self.assertEqual(ModulesEnv.loaded(), {})


class TestModulesRun(TestCase):

    @unittest.skipIf(*SKIP_ARGS)
    def test_run_cmd(self):
        ModulesEnv._run_module_cmd('avail')
        self.assertRaises(
            ArcanaModulesError,
            ModulesEnv._run_module_cmd,
            'badcmd')
        self.assertRaises(
            ArcanaModulesError,
            ModulesEnv._run_module_cmd,
            'load', 'somereallyunlikelymodulename')
