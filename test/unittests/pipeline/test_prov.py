from pprint import pformat
import os.path as op
import tempfile
from copy import copy
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
from arcana.exceptions import (
    ArcanaReprocessException, ArcanaProtectedOutputConflictError)


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
        FieldSpec('derived_field3', float, 'pipeline3'),
        FieldSpec('derived_field4', float, 'pipeline2')]

    add_param_specs = [
        SwitchSpec('extra_req', False),
        SwitchSpec('branch', 'foo', ('foo', 'bar', 'wee')),
        ParameterSpec('multiplier', 10.0),
        ParameterSpec('subtract', 3)]

    def pipeline1(self, **name_maps):
        pipeline = self.new_pipeline(
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
        pipeline = self.new_pipeline(
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
        math2 = pipeline.add(
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
        pipeline.add(
            'math3',
            TestMath(
                op='sub',
                as_file=False,
                y=-1),
            connect={
                'x': (math2, 'z')},
            outputs={
                'z': ('derived_field4', float)})
        return pipeline

    def pipeline3(self, **name_maps):
        pipeline = self.new_pipeline(
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
            'math4',
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


def change_value_w_prov(field, new_value):
    """
    Changes the value of a field and updates the corresponding provenance
    record so that it appears if the value was given by the pipeline. Used
    to detect whether a value has been reprocessed unnecessarily, as in that
    case the value will be overwritten to the value actually outputted by
    the pipeline
    """
    # Change value in repository to test that the pipeline isn't rerun
    field.value = new_value
    # Update provenance record so it isn't determined to be protected
    record = field.record
    record.prov['outputs'][field.name] = new_value
    field.repository.put_record(record)


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

    def test_altered_workflow(self):
        """
        Tests whether data is regenerated if the pipeline workflows are altered
        """
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

    def test_unchanged_workflow(self):
        """
        Tests that when a parameter is changed that doesn't effect the
        workflows that generated a value, then the data isn't regenerated
        """
        study_name = 'changed_parameter'
        new_value = -99.0
        # Test vanilla study
        study = self.create_study(
            TestProvStudy,
            study_name,
            inputs=STUDY_INPUTS)
        derived_field4 = study.data('derived_field4').item(*self.SESSION)
        self.assertEqual(derived_field4.value, 155.0)
        # Change value to a new value to see if it gets overwritten even
        # it shouldn't as the parameter that is changed doesn't impact on
        # derived field 4.
        change_value_w_prov(derived_field4, new_value)
        study = self.create_study(
            TestProvStudy,
            study_name,
            inputs=STUDY_INPUTS,
            parameters={'subtract': 100})
        new_derived_field4 = study.data('derived_field4').item(*self.SESSION)
        self.assertEqual(new_derived_field4.value, new_value)
        self.assertEqual(
            new_derived_field4.record.prov['outputs']['derived_field4'],
            new_value)

    def test_protect_manually(self):
        """Protect manually altered files and fields from overwrite"""
        study_name = 'manual_protect'
        protected_derived_field4_value = -99.0
        protected_derived_fileset1_value = -999.0
        # Test vanilla study
        study = self.create_study(
            TestProvStudy,
            study_name,
            inputs=STUDY_INPUTS)
        derived_fileset1_collection, derived_field4_collection = study.data(
            ('derived_fileset1', 'derived_field4'))
        self.assertContentsEqual(derived_fileset1_collection, 154.0)
        self.assertEqual(derived_field4_collection.value(*self.SESSION), 155.0)
        # Rerun with new parameters
        study = self.create_study(
            TestProvStudy,
            study_name,
            inputs=STUDY_INPUTS,
            processor=LinearProcessor(self.work_dir, reprocess=True),
            parameters={'multiplier': 100.0})
        derived_fileset1_collection, derived_field4_collection = study.data(
            ('derived_fileset1', 'derived_field4'))
        self.assertContentsEqual(derived_fileset1_collection, 1414.0)
        derived_field4 = derived_field4_collection.item(*self.SESSION)
        self.assertEqual(derived_field4.value, 1415.0)
        # Manually changing the value (or file contents) of a derivative value
        # (without also altering the saved provenance record) will mean
        # that new value/file will be "protected" from reprocessing, and will
        # need to be deleted in order to be regenerated
        derived_field4.value = protected_derived_field4_value
        # Since derived_fileset1 needs to be reprocessed but
        study = self.create_study(
            TestProvStudy,
            study_name,
            processor=LinearProcessor(self.work_dir, reprocess=True),
            inputs=STUDY_INPUTS,
            parameters={'multiplier': 1000.0})
        # Check to see protected conflict error is raise if only one of
        # derived field4/fileset1 is protected
        self.assertRaises(
            ArcanaProtectedOutputConflictError,
            study.data,
            ('derived_fileset1', 'derived_field4'))
        with open(derived_fileset1_collection.path(*self.SESSION), 'w') as f:
            f.write(str(protected_derived_fileset1_value))
        study.clear_caches()
        # Protect the output of derived_fileset1 as well and it should return
        # the protected values
        derived_fileset1_collection, derived_field4_collection = study.data(
            ('derived_fileset1', 'derived_field4'))
        self.assertContentsEqual(derived_fileset1_collection,
                                 protected_derived_fileset1_value)
        self.assertEqual(derived_field4_collection.value(*self.SESSION),
                         protected_derived_field4_value)


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
            ArcanaReprocessException,
            study.data,
            'derived_field2')
        new_study = self.create_study(
            TestProvStudy,
            study_name,
            processor=LinearProcessor(self.work_dir, reprocess=True),
            inputs=STUDY_INPUTS)
        self.assertEqual(
            new_study.data('derived_field2').value(*self.SESSION), 1145.0)


class TestProvDialationStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        AcquiredFieldSpec('acquired_field1', int),
        FieldSpec('derived_field1', int, 'pipeline1'),
        FieldSpec('derived_field2', int, 'pipeline2',
                  frequency='per_subject'),
        FieldSpec('derived_field3', int, 'pipeline3',
                  frequency='per_visit'),
        FieldSpec('derived_field4', int, 'pipeline4',
                  frequency='per_study'),
        FieldSpec('derived_field5', int, 'pipeline5')]

    add_param_specs = [
        ParameterSpec('increment', 1),
        ParameterSpec('pipeline3_op', 'add')]

    def pipeline1(self, **name_maps):
        pipeline = self.new_pipeline(
            'pipeline1',
            desc="",
            references=[],
            name_maps=name_maps)
        pipeline.add(
            'math',
            TestMath(
                op='add',
                y=self.parameter('increment'),
                as_file=False),
            inputs={
                'x': ('acquired_field1', int)},
            outputs={
                'z': ('derived_field1', int)})
        return pipeline

    def pipeline2(self, **name_maps):
        pipeline = self.new_pipeline(
            'pipeline2',
            desc="",
            references=[],
            name_maps=name_maps)
        pipeline.add(
            'math',
            TestMath(
                op='add',
                as_file=False),
            inputs={
                'x': ('derived_field1', int)},
            outputs={
                'z': ('derived_field2', int)},
            joinsource=self.VISIT_ID,
            joinfield='x')
        return pipeline

    def pipeline3(self, **name_maps):
        pipeline = self.new_pipeline(
            'pipeline3',
            desc="",
            references=[],
            name_maps=name_maps)
        pipeline.add(
            'math',
            TestMath(
                op=self.parameter('pipeline3_op'),
                as_file=False),
            inputs={
                'x': ('derived_field1', int)},
            outputs={
                'z': ('derived_field3', int)},
            joinsource=self.SUBJECT_ID,
            joinfield='x')
        return pipeline

    def pipeline4(self, **name_maps):
        pipeline = self.new_pipeline(
            'pipeline4',
            desc="",
            references=[],
            name_maps=name_maps)
        merge1 = pipeline.add(
            'merge1',
            Merge(
                numinputs=1,
                ravel_inputs=True),
            inputs={
                'in1': ('derived_field1', int)},
            joinsource=self.SUBJECT_ID,
            joinfield='in1')
        merge2 = pipeline.add(
            'merge2',
            Merge(
                numinputs=1,
                ravel_inputs=True),
            connect={
                'in1': (merge1, 'out')},
            joinsource=self.VISIT_ID,
            joinfield='in1')
        pipeline.add(
            'math',
            TestMath(
                op='add',
                as_file=False),
            connect={
                'x': (merge2, 'out')},
            outputs={
                'z': ('derived_field4', int)})
        return pipeline

    def pipeline5(self, **name_maps):
        pipeline = self.new_pipeline(
            'pipeline5',
            desc="",
            references=[],
            name_maps=name_maps)
        merge = pipeline.add(
            'merge',
            Merge(
                numinputs=3),
            inputs={
                'in1': ('derived_field2', int),
                'in2': ('derived_field3', int),
                'in3': ('derived_field4', int)})
        pipeline.add(
            'math',
            TestMath(
                op='add',
                as_file=False),
            connect={
                'x': (merge, 'out')},
            outputs={
                'z': ('derived_field5', float)})
        return pipeline


class TestProvDialation(BaseMultiSubjectTestCase):
    """
    Tests the "dialation" of the to process array in the case that there are
    summary outputs (i.e. frequency != 'per_session') of a pipeline.
    """

    NUM_SUBJECTS = 2
    NUM_VISITS = 2
    STUDY_INPUTS = [FieldSelector('acquired_field1', int, 'acquired_field1')]

    DEFAULT_FIELD5_VALUES = {
        ('0', '0'): 41,
        ('0', '1'): 43,
        ('1', '0'): 61,
        ('1', '1'): 63}

    @property
    def input_tree(self):
        fields = []
        for subj_i in range(self.NUM_SUBJECTS):
            for visit_i in range(self.NUM_VISITS):
                fields.append(
                    Field(name='acquired_field1', value=visit_i + subj_i * 10,
                          dtype=int, frequency='per_session',
                          subject_id=str(subj_i), visit_id=str(visit_i)))
        return Tree.construct(fields=fields)

    def test_filter_dialation1(self):
        study_name = 'process_dialation'
        study = self.create_study(
            TestProvDialationStudy,
            study_name,
            inputs=self.STUDY_INPUTS)
        field2 = study.data(
            'derived_field2',
            session_ids=[('0', '0'),
                         ('1', '1')])
        self.assertEqual(field2.value(subject_id='0'), 3)
        self.assertEqual(field2.value(subject_id='1'), 23)
        field3 = study.data(
            'derived_field3',
            session_ids=[('0', '0'),
                         ('1', '1')])
        self.assertEqual(field3.value(visit_id='0'), 12)
        self.assertEqual(field3.value(visit_id='1'), 14)
        field4 = study.data(
            'derived_field4',
            session_ids=[('1', '1')])
        self.assertEqual(field4.value(), 26)

    def test_filter_dialation2(self):
        study_name = 'filter_dialation2'
        study = self.create_study(
            TestProvDialationStudy,
            study_name,
            inputs=self.STUDY_INPUTS)
        field5 = study.data(
            'derived_field5',
            session_ids=[('1', '1')])
        self.assertEqual(len(field5), 1)
        self.assertEqual(field5.value(subject_id='1', visit_id='1'),
                         self.DEFAULT_FIELD5_VALUES[('1', '1')])

        # Check that no more fields were generated than necessary
        tree = study.tree
        sess00 = study.tree.session(subject_id='0', visit_id='0')
        sess01 = study.tree.session(subject_id='0', visit_id='1')
        sess10 = study.tree.session(subject_id='1', visit_id='0')
        sess11 = study.tree.session(subject_id='1', visit_id='1')
        subj0 = study.tree.subject('0')
        subj1 = study.tree.subject('1')
        vis0 = study.tree.visit('0')
        vis1 = study.tree.visit('1')
        self.assertEqual(list(tree.field_keys),
                         [('derived_field4', study_name)])
        self.assertFalse(list(subj0.field_keys))
        self.assertEqual(list(subj1.field_keys),
                         [('derived_field2', study_name)])
        self.assertFalse(list(vis0.field_keys))
        self.assertEqual(list(vis1.field_keys),
                         [('derived_field3', study_name)])
        self.assertEqual(sorted(sess00.field_keys),
                         [('acquired_field1', None),
                          ('derived_field1', study_name)])
        self.assertEqual(sorted(sess01.field_keys),
                         [('acquired_field1', None),
                          ('derived_field1', study_name)])
        self.assertEqual(sorted(sess10.field_keys),
                         [('acquired_field1', None),
                          ('derived_field1', study_name)])
        self.assertEqual(sorted(sess11.field_keys),
                         [('acquired_field1', None),
                          ('derived_field1', study_name),
                          ('derived_field5', study_name)])

    def test_process_dialation(self):
        study_name = 'process_dialation'
        new_value = -101
        study = self.create_study(
            TestProvDialationStudy,
            study_name,
            inputs=self.STUDY_INPUTS)
        study.data('derived_field5')

        def values_equal(field_name, values):
            for subj_i in range(self.NUM_SUBJECTS):
                for vis_i in range(self.NUM_VISITS):
                    sess = study.tree.session(subj_i, vis_i)
                    field = sess.field(field_name, from_study=study_name)
                    self.assertEqual(field.value,
                                     values[(str(subj_i), str(vis_i))])
        # Test generated values
        values_equal('derived_field5', self.DEFAULT_FIELD5_VALUES)
        # Tag the field 1 value so we can detect if it gets regenerated
        orig_field1_values = {}
        orig_field3_values = {}
        for vis_i in range(self.NUM_VISITS):
            for subj_i in range(self.NUM_SUBJECTS):
                sess = study.tree.session(subj_i, vis_i)
                field1 = sess.field('derived_field1', from_study=study_name)
                orig_field1_values[(str(subj_i),
                                    str(vis_i))] = field1.value
                change_value_w_prov(field1, new_value)
            field3 = study.tree.visit(vis_i).field('derived_field3',
                                                   from_study=study_name)
            orig_field3_values[str(vis_i)] = field3.value
        # Rerun analysis with new parameters
        study = self.create_study(
            TestProvDialationStudy,
            study_name,
            inputs=self.STUDY_INPUTS,
            processor=LinearProcessor(self.work_dir, reprocess=True),
            parameters={
                'pipeline3_op': 'mul'})
        study.data('derived_field3', subject_id='0', visit_id='0')
        values_equal('derived_field1',
                     {k: new_value for k in orig_field1_values})
        self.assertEqual(
            study.tree.visit('0').field('derived_field3',
                                        from_study=study_name).value,
            10201)
        self.assertEqual(
            study.tree.visit('1').field('derived_field3',
                                        from_study=study_name).value,
            orig_field3_values['1'])
        study = self.create_study(
            TestProvDialationStudy,
            study_name,
            inputs=self.STUDY_INPUTS,
            processor=LinearProcessor(self.work_dir, reprocess=True),
            parameters={
                'increment': 2})
        study.data('derived_field5', subject_id='0', visit_id='0')
        values_equal('derived_field1',
                     {k: v + 1 for k, v in orig_field1_values.items()})

    def test_dialation_protection(self):
        study_name = 'dialation_protection'
        study = self.create_study(
            TestProvDialationStudy,
            study_name,
            inputs=self.STUDY_INPUTS)
        field5 = study.data('derived_field5')
        for item in field5:
            self.assertEqual(item.value,
                             self.DEFAULT_FIELD5_VALUES[(item.subject_id,
                                                         item.visit_id)])
        field1 = study.data('derived_field1')
        field2 = study.data('derived_field2')
        field1.item(subject_id='0', visit_id='1').value = 1000000
        field1.item(subject_id='1', visit_id='1').value = 2000000
        # Manually change value of field 2
        field2.item(subject_id='0').value = -1000
        study = self.create_study(
            TestProvDialationStudy,
            study_name,
            processor=LinearProcessor(self.work_dir, reprocess=True),
            inputs=self.STUDY_INPUTS,
            parameters={
                'increment': 2})
        # Recalculate value of field5 with new field2 value
        field1, field2, field3, field4, field5 = study.data(
            ['derived_field1', 'derived_field2', 'derived_field3',
             'derived_field4', 'derived_field5'])
        self.assertEqual(field1.value(subject_id='0', visit_id='0'), 2)
        self.assertEqual(field1.value(subject_id='0', visit_id='1'), 1000000)
        self.assertEqual(field1.value(subject_id='1', visit_id='0'), 12)
        self.assertEqual(field1.value(subject_id='1', visit_id='1'), 2000000)
        self.assertEqual(field2.value(subject_id='0'), -1000)
        self.assertEqual(field2.value(subject_id='1'), 2000012)
        self.assertEqual(field3.value(visit_id='0'), 14)
        self.assertEqual(field3.value(visit_id='1'), 3000000)
        self.assertEqual(field4.value(), 3000014)
        self.assertEqual(field5.value(subject_id='0', visit_id='0'),
                         -1000 + 14 + 3000014)
        self.assertEqual(field5.value(subject_id='0', visit_id='1'),
                         -1000 + 3000000 + 3000014)
        self.assertEqual(field5.value(subject_id='1', visit_id='0'),
                         2000012 + 14 + 3000014)
        self.assertEqual(field5.value(subject_id='1', visit_id='1'),
                         2000012 + 3000000 + 3000014)
