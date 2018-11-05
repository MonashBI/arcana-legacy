import os
import os.path as op
import tempfile
import shutil
from nipype.interfaces.utility import Merge  # @IgnorePep8
from arcana.testing import BaseTestCase, TestMath  # @IgnorePep8
from arcana.study.base import Study, StudyMetaClass  # @IgnorePep8
from arcana.study.parameter import SwitchSpec  # @IgnorePep8
from arcana.data import (
    AcquiredFilesetSpec, FieldSpec, FilesetSelector,
    AcquiredFieldSpec, FieldSelector)  # @IgnorePep8
from arcana.data.file_format.standard import text_format  # @IgnorePep8
from future.utils import with_metaclass  # @IgnorePep8
from arcana.study.provenance import Record
from arcana.environment import BaseRequirement


class DummyRequirement(BaseRequirement):

    def __init__(self, *args, **kwargs):
        self.detected_version = kwargs.pop('detected_version')
        super(DummyRequirement, self).__init__(*args, **kwargs)

    def detect_version_str(self):
        return self.detected_version


a_req = DummyRequirement('a', detected_version='1.0.1')
b_req = DummyRequirement('b', detected_version='2.7.2')
c_req = DummyRequirement('c', detected_version='0.2a2')
d_req = DummyRequirement('d', detected_version='0.9.dev10')


class TestProvStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        AcquiredFilesetSpec('acqfile1', text_format),
        AcquiredFilesetSpec('acqfile2', text_format),
        AcquiredFieldSpec('acqfield1', float),
        FieldSpec('derfield1', float, 'pipeline1', array=True)]

    add_param_specs = [
        SwitchSpec('switch', False),
        SwitchSpec('branch', 'foo', ('foo', 'bar', 'wee'))]

    def pipeline1(self, **name_maps):
        pipeline = self.pipeline(
            'pipeline2',
            desc="",
            references=[],
            name_maps=name_maps)
        math1 = pipeline.add(
            'math1',
            TestMath(
                op='add'),
            inputs={
                'x': ('acqfile1', text_format),
                'y': ('acqfile2', text_format)},
            requirements=[
                a_req.v('1.0.1'),
                b_req.v(2)])
        math2 = pipeline.add(
            'math2',
            TestMath(
                op='add'),
            inputs={
                'y': ('acqfield1', float)},
            connect={
                'x': (math1, 'z')},
            requirements=[
                c_req.v(0.1)])
        math3 = pipeline.add(
            'math3',
            TestMath(
                op='mul',
                y=10.0),
            connect={
                'x': (math2, 'z')},
            requirements=[
                b_req.v('2.7.0', '3.0')])
        pipeline.add(
            'merge1',
            Merge(3),
            connect={
                'in1': (math1, 'z'),
                'in2': (math2, 'z'),
                'in3': (math3, 'z')},
            outputs={
                'out': ('derfield1', float)},
            requirements=[
                a_req.v(1),
                d_req.v('0.8.6')])
        return pipeline


class TestProvenance(BaseTestCase):

    INPUT_DATASETS = {'acqfile1': '1.0',
                      'acqfile2': '2.0'}

    INPUT_FIELDS = {'acqfield1': 3}

    def setUp(self):
        super(TestProvenance, self).setUp()
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        super(TestProvenance, self).tearDown()
        shutil.rmtree(self.tempdir)

    def test_derivable(self):
        # Test vanilla study
        study = self.create_study(
            TestProvStudy,
            'study',
            inputs=[FilesetSelector('acqfile1', text_format, 'acqfile1'),
                    FilesetSelector('acqfile2', text_format, 'acqfile2'),
                    FieldSelector('acqfield1', int, 'acqfield1')])
        # Just test to see if the pipeline works
        self.assertEqual(
            next(iter(study.data('derfield1'))).value, [3.0, 6.0, 60.0])
        pipeline1 = study.pipeline1()
        prov = pipeline1.provenance
        record = prov.record([], [])
        path = op.join(self.tempdir, 'prov1.json')
        record.save(path)
        reloaded = Record.load(path)
        self.assertTrue(record.matches(reloaded),
                        "Reloaded record did not match saved record:{}"
                        .format(reloaded.find_mismatch(record, indent='  ')))
