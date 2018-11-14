from pprint import pformat
import os.path as op
import tempfile
import shutil
from nipype.interfaces.utility import Merge, Split  # @IgnorePep8
from arcana.utils.testing import BaseTestCase, TestMath  # @IgnorePep8
from arcana.processor import LinearProcessor
from arcana.study.base import Study, StudyMetaClass  # @IgnorePep8
from arcana.study.parameter import ParameterSpec, SwitchSpec  # @IgnorePep8
from arcana.data import (
    AcquiredFilesetSpec, FilesetSpec, FieldSpec, FilesetSelector,
    AcquiredFieldSpec, FieldSelector)  # @IgnorePep8
from arcana.data.file_format.standard import text_format  # @IgnorePep8
from future.utils import with_metaclass  # @IgnorePep8
from arcana.pipeline.provenance import Record
from arcana.environment import BaseRequirement
from arcana.exceptions import ArcanaProvenanceRecordMismatchError


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
        AcquiredFilesetSpec('acqfile3', text_format),
        AcquiredFieldSpec('acqfield1', float),
        FieldSpec('derfield1', float, 'pipeline1', array=True),
        FilesetSpec('derfile1', text_format, 'pipeline2'),
        FieldSpec('derfield2', float, 'pipeline3'),
        FieldSpec('derfield3', float, 'pipeline3')]

    add_param_specs = [
        SwitchSpec('extra_req', False),
        SwitchSpec('branch', 'foo', ('foo', 'bar', 'wee')),
        ParameterSpec('multiplier', 10.0),
        ParameterSpec('subtract', 3)]

    def pipeline1(self, **name_maps):
        pipeline = self.pipeline(
            'pipeline1',
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
        # Set up different requirements based on switch
        math3_reqs = [a_req.v(1)]
        if self.branch('extra_req'):
            math3_reqs.append(d_req.v('0.8.6'))
        math3 = pipeline.add(
            'math3',
            TestMath(
                op='mul',
                y=self.parameter('multiplier')),
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
            requirements=math3_reqs)
        return pipeline

    def pipeline2(self, **name_maps):
        pipeline = self.pipeline(
            'pipeline2',
            desc="",
            references=[],
            name_maps=name_maps)
        split = pipeline.add(
            'split',
            Split(
                splits=[1, 1, 1],
                squeeze=True),
            inputs={
                'inlist': ('derfield1', float)})
        math1 = pipeline.add(
            'math1',
            TestMath(
                op='add',
                as_file=True),
            inputs={
                'y': ('acqfile3', text_format)},
            connect={
                'x': (split, 'out1')},
            requirements=[
                a_req.v('1.0')])
        pipeline.add(
            'math2',
            TestMath(
                op='add',
                as_file=True),
            inputs={
                'y': ('acqfield1', float)},
            connect={
                'x': (math1, 'z')},
            outputs={
                'z': ('derfile1', text_format)},
            requirements=[
                c_req.v(0.1)])
        return pipeline

    def pipeline3(self, **name_maps):
        pipeline = self.pipeline(
            'pipeline3',
            desc="",
            references=[],
            name_maps=name_maps)
        pipeline.add(
            'math1',
            TestMath(
                op='add',
                as_file=False),
            inputs={
                'x': ('acqfile2', text_format),
                'y': ('derfile1', text_format)},
            outputs={
                'z': ('derfield2', float)},
            requirements=[
                a_req.v('1.0')])
        pipeline.add(
            'math2',
            TestMath(
                op='sub',
                as_file=False,
                y=self.parameter('subtract')),
            inputs={
                'x': ('acqfield1', float)},
            outputs={
                'z': ('derfield3', float)},
            requirements=[
                c_req.v(0.1)])
        return pipeline


STUDY_INPUTS = [FilesetSelector('acqfile1', text_format, 'acqfile1'),
                FilesetSelector('acqfile2', text_format, 'acqfile2'),
                FilesetSelector('acqfile3', text_format, 'acqfile3'),
                FieldSelector('acqfield1', int, 'acqfield1')]


INPUT_DATASETS = {'acqfile1': '1.0',
                  'acqfile2': '2.0',
                  'acqfile3': '3.0'}

INPUT_FIELDS = {'acqfield1': 11}


class TestProv(BaseTestCase):

    INPUT_DATASETS = INPUT_DATASETS

    INPUT_FIELDS = INPUT_FIELDS

    def test_json_roundtrip(self):
        study_name = 'roundtrip_study'
        # Test vanilla study
        study = self.create_study(
            TestProvStudy,
            study_name,
            inputs=STUDY_INPUTS)
        # Just test to see if the pipeline works
        self.assertEqual(next(iter(study.data('derfield1'))).value,
                         [3.0, 14.0, 140.0])
        pipeline1 = study.pipeline1()
        prov = pipeline1.prov()
        record = Record('pipeline1', 'per_session', self.SUBJECT, self.VISIT,
                        study_name, prov)
        tempdir = tempfile.mkdtemp()
        try:
            path = op.join(tempdir, 'prov1.json')
            record.save(path)
            reloaded = Record.load('pipeline1', 'per_session', self.SUBJECT,
                                   self.VISIT, study_name, path)
        finally:
            shutil.rmtree(tempdir)
        mismatches = record.mismatches(reloaded)
        self.assertFalse(mismatches,
                         "Reloaded record did not match saved record:{}"
                         .format(mismatches))

    def test_reprocess(self):
        pass

    def test_protected(self):
        pass


class TestProvInputChange(BaseTestCase):

    INPUT_DATASETS = INPUT_DATASETS

    INPUT_FIELDS = INPUT_FIELDS

    def test_input_change(self):
        study_name = 'input_change_study'
        study = self.create_study(
            TestProvStudy,
            study_name,
            inputs=STUDY_INPUTS)
        self.assertEqual(study.data('derfield2').value(*self.SESSION),
                         19.0)
        # Change acquired file contents, which should cause the checksum check
        # to fail
        with open(study.data('acqfile1').path(*self.SESSION), 'w') as f:
            f.write('99.9')
        # Should detect that the input has changed and throw an error
        self.assertRaises(
            ArcanaProvenanceRecordMismatchError,
            study.data,
            'derfield2')
        new_study = self.create_study(
            TestProvStudy,
            study_name,
            processor=LinearProcessor(self.work_dir, reprocess=True),
            inputs=STUDY_INPUTS)
        self.assertEqual(
            new_study.data('derfield2').value(*self.SESSION), 117.9)
