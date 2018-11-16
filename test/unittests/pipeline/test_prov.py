from pprint import pformat
import os.path as op
import tempfile
import shutil
from nipype.interfaces.utility import Merge, Split  # @IgnorePep8
from arcana.utils.testing import (
    BaseTestCase, BaseMultiSubjectTestCase, TestMath)  # @IgnorePep8
from arcana.processor import LinearProcessor
from arcana.study.base import Study, StudyMetaClass  # @IgnorePep8
from arcana.study.parameter import ParameterSpec, SwitchSpec  # @IgnorePep8
from arcana.data import (
    AcquiredFilesetSpec, FilesetSpec, FieldSpec, FilesetSelector,
    AcquiredFieldSpec, FieldSelector)  # @IgnorePep8
from arcana.data.file_format.standard import text_format  # @IgnorePep8
from future.utils import with_metaclass  # @IgnorePep8
from arcana.pipeline.provenance import Record
from arcana.data import Fileset, Field
from arcana.repository import Tree
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
        AcquiredFilesetSpec('acquired_fileset1', text_format),
        AcquiredFilesetSpec('acquired_fileset2', text_format),
        AcquiredFilesetSpec('acquired_fileset3', text_format),
        AcquiredFieldSpec('acquired_field1', float),
        FilesetSpec('derived_fileset1', text_format, 'pipeline2'),
        FieldSpec('derived_field1', float, 'pipeline1', array=True),
        FieldSpec('derived_field2', float, 'pipeline3'),
        FieldSpec('derived_field3', float, 'pipeline3')]

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
                'x': ('acquired_fileset1', text_format),
                'y': ('acquired_fileset2', text_format)},
            requirements=[
                a_req.v('1.0.1'),
                b_req.v(2)])
        math2 = pipeline.add(
            'math2',
            TestMath(
                op='add'),
            inputs={
                'y': ('acquired_field1', float)},
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
                'out': ('derived_field1', float)},
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
                'inlist': ('derived_field1', float)})
        math1 = pipeline.add(
            'math1',
            TestMath(
                op='add',
                as_file=True),
            inputs={
                'y': ('acquired_fileset3', text_format)},
            connect={
                'x': (split, 'out3')},
            requirements=[
                a_req.v('1.0')])
        pipeline.add(
            'math2',
            TestMath(
                op='add',
                as_file=True),
            inputs={
                'y': ('acquired_field1', float)},
            connect={
                'x': (math1, 'z')},
            outputs={
                'z': ('derived_fileset1', text_format)},
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
                'x': ('acquired_fileset2', text_format),
                'y': ('derived_fileset1', text_format)},
            outputs={
                'z': ('derived_field2', float)},
            requirements=[
                a_req.v('1.0')])
        pipeline.add(
            'math2',
            TestMath(
                op='sub',
                as_file=False,
                y=self.parameter('subtract')),
            inputs={
                'x': ('acquired_field1', float)},
            outputs={
                'z': ('derived_field3', float)},
            requirements=[
                c_req.v(0.1)])
        return pipeline


class TestProvStudyAddNode(with_metaclass(StudyMetaClass, TestProvStudy)):

    add_param_specs = [
        ParameterSpec('a', 50.0),
        ParameterSpec('b', 25.0)]

    def pipeline2(self, **name_maps):
        pipeline = super(TestProvStudyAddNode, self).pipeline2(**name_maps)
        pipeline.add(
            'math3',
            TestMath(
                op='mul',
                as_file=True,
                x=self.parameter('a'),
                y=self.parameter('b')),
            outputs={
                'z': ('derived_fileset1', text_format)})
        return pipeline


class TestProvStudyAddConnect(with_metaclass(StudyMetaClass, TestProvStudy)):

    def pipeline1(self, **name_maps):
        pipeline = super(TestProvStudyAddConnect, self).pipeline1(**name_maps)
        pipeline.connect_input('acquired_field1', pipeline.node('math3'), 'y',
                               float)
        return pipeline


STUDY_INPUTS = [FilesetSelector('acquired_fileset1', text_format,
                                'acquired_fileset1'),
                FilesetSelector('acquired_fileset2', text_format,
                                'acquired_fileset2'),
                FilesetSelector('acquired_fileset3', text_format,
                                'acquired_fileset3'),
                FieldSelector('acquired_field1', int, 'acquired_field1')]


INPUT_DATASETS = {'acquired_fileset1': '1.0',
                  'acquired_fileset2': '2.0',
                  'acquired_fileset3': '3.0'}

INPUT_FIELDS = {'acquired_field1': 11}


class TestProvBasic(BaseTestCase):

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
        self.assertEqual(next(iter(study.data('derived_field1'))).value,
                         [3.0, 14.0, 140.0])
        pipeline1 = study.pipeline1()
        pipeline1.cap()
        record = Record('pipeline1', 'per_session', self.SUBJECT, self.VISIT,
                        study_name, pipeline1.prov)
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

    def test_altered_workflow(self):
        study_name = 'add_node'
        # Test vanilla study
        study = self.create_study(
            TestProvStudy,
            study_name,
            inputs=STUDY_INPUTS)
        self.assertEqual(study.data('derived_field2').value(*self.SESSION),
                         156.0)
        # Rerun results of altered study
        study = self.create_study(
            TestProvStudyAddNode,
            study_name,
            processor=LinearProcessor(self.work_dir, reprocess=True),
            inputs=STUDY_INPUTS)
        self.assertEqual(study.data('derived_field2').value(*self.SESSION),
                         1252.0)
        study_name = 'add_connect'
        # Test vanilla study
        study = self.create_study(
            TestProvStudy,
            study_name,
            inputs=STUDY_INPUTS)
        self.assertEqual(study.data('derived_field2').value(*self.SESSION),
                         156.0)
        # Rerun results of altered study
        study = self.create_study(
            TestProvStudyAddConnect,
            study_name,
            processor=LinearProcessor(self.work_dir, reprocess=True),
            inputs=STUDY_INPUTS)
        self.assertEqual(study.data('derived_field2').value(*self.SESSION),
                         170.0)


class TestProvDialationStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        AcquiredFieldSpec('acquired_field1', float),
        FieldSpec('derived_field1', float, 'pipeline1',
                  frequency='per_visit'),
        FieldSpec('derived_field2', float, 'pipeline2',
                  frequency='per_subject'),
        FieldSpec('derived_field3', float, 'pipeline3',
                  frequency='per_study'),
        FieldSpec('derived_field4', float, 'pipeline4',
                  frequency='per_study'),
        FieldSpec('derived_field5', float, 'pipeline4',
                  frequency='per_study')]

    def pipeline1(self, **name_maps):
        pipeline = self.pipeline(
            'pipeline1',
            desc="",
            references=[],
            name_maps=name_maps)
        pipeline.add(
            'math1',
            TestMath(
                op='add',
                as_file=False),
            inputs={
                'x': ('acquired_fileset2', text_format),
                'y': ('derived_fileset1', text_format)},
            outputs={
                'z': ('derived_field2', float)},
            requirements=[
                a_req.v('1.0')])
        return pipeline

    def pipeline2(self, **name_maps):
        pipeline = self.pipeline(
            'pipeline2',
            desc="",
            references=[],
            name_maps=name_maps)
        pipeline.add(
            'math1',
            TestMath(
                op='add',
                as_file=False),
            inputs={
                'x': ('acquired_fileset2', text_format),
                'y': ('derived_fileset1', text_format)},
            outputs={
                'z': ('derived_field2', float)},
            requirements=[
                a_req.v('1.0')])
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
                'x': ('acquired_fileset2', text_format),
                'y': ('derived_fileset1', text_format)},
            outputs={
                'z': ('derived_field2', float)},
            requirements=[
                a_req.v('1.0')])
        return pipeline

    def pipeline4(self, **name_maps):
        pipeline = self.pipeline(
            'pipeline4',
            desc="",
            references=[],
            name_maps=name_maps)
        pipeline.add(
            'math1',
            TestMath(
                op='add',
                as_file=False),
            inputs={
                'x': ('acquired_fileset2', text_format),
                'y': ('derived_fileset1', text_format)},
            outputs={
                'z': ('derived_field2', float)},
            requirements=[
                a_req.v('1.0')])
        return pipeline


class TestProvDialation(BaseMultiSubjectTestCase):

    NUM_SUBJECTS = 2
    NUM_VISITS = 2

    DATASET_CONTENTS = {'acquired_fileset1': '2',
                        'acquired_fileset2': '3',
                        'acquired_fileset3': '4',
                        'acquired_field1': '5'}

    @property
    def input_tree(self):
        filesets = []
        fields = []
        for subj_i in range(self.NUM_SUBJECTS):
            subject_id = 'SUBJECT{}'.format(subj_i)
            for visit_i in range(self.NUM_VISITS):
                visit_id = 'VISIT{}'.format(visit_i)
                for acq in self.DATASET_CONTENTS:
                    if acq.startswith('acquired_fileset'):
                        filesets.append(
                            Fileset(acq, text_format, 'per_session',
                                    subject_id=subject_id, visit_id=visit_id))
                    else:
                        fields.append(
                            Field(acq, int, 'per_session',
                                  subject_id=subject_id, visit_id=visit_id))
        return Tree.construct(filesets=filesets, fields=fields)


class TestProvInputChange(BaseTestCase):

    INPUT_DATASETS = INPUT_DATASETS
    INPUT_FIELDS = INPUT_FIELDS

    def test_input_change(self):
        study_name = 'input_change_study'
        study = self.create_study(
            TestProvStudy,
            study_name,
            inputs=STUDY_INPUTS)
        self.assertEqual(study.data('derived_field2').value(*self.SESSION),
                         156.0)
        # Change acquired file contents, which should cause the checksum check
        # to fail
        with open(study.data('acquired_fileset1').path(*self.SESSION),
                  'w') as f:
            f.write('99.9')
        # Should detect that the input has changed and throw an error
        self.assertRaises(
            ArcanaProvenanceRecordMismatchError,
            study.data,
            'derived_field2')
        new_study = self.create_study(
            TestProvStudy,
            study_name,
            processor=LinearProcessor(self.work_dir, reprocess=True),
            inputs=STUDY_INPUTS)
        self.assertEqual(
            new_study.data('derived_field2').value(*self.SESSION), 1145.0)
