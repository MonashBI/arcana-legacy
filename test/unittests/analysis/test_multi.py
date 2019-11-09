from arcana.utils.testing import BaseTestCase, TestMath
from arcana.utils.interfaces import Merge
from arcana.data import InputFilesets, FilesetSpec, InputFilesetSpec
from arcana.data.file_format import text_format
from arcana.analysis.parameter import ParamSpec
from arcana.exceptions import ArcanaOutputNotProducedException
from arcana.analysis.base import Analysis
from arcana.analysis.multi import (
    MultiAnalysis, SubCompSpec, MultiAnalysisMetaClass, AnalysisMetaClass)
from arcana.analysis.parameter import Parameter
from future.utils import with_metaclass
import unittest


class NotSpecifiedRequiredParameter(Exception):
    pass


class AnalysisA(with_metaclass(AnalysisMetaClass, Analysis)):

    add_data_specs = [
        InputFilesetSpec('x', text_format),
        InputFilesetSpec('y', text_format),
        FilesetSpec('z', text_format, 'pipeline_alpha')]

    add_param_specs = [
        ParamSpec('o1', 1),
        ParamSpec('o2', '2'),
        ParamSpec('o3', 3.0)]

    def pipeline_alpha(self, **name_maps):
        pipeline = self.new_pipeline(
            name='pipeline_alpha',
            desc="A dummy pipeline used to test MultiAnalysis class",
            citations=[],
            name_maps=name_maps)
        math = pipeline.add("math", TestMath())
        math.inputs.op = 'add'
        math.inputs.as_file = True
        # Connect inputs
        pipeline.connect_input('x', math, 'x')
        pipeline.connect_input('y', math, 'y')
        # Connect outputs
        pipeline.connect_output('z', math, 'z')
        return pipeline


class AnalysisB(with_metaclass(AnalysisMetaClass, Analysis)):

    add_data_specs = [
        InputFilesetSpec('w', text_format),
        InputFilesetSpec('x', text_format),
        FilesetSpec('y', text_format, 'pipeline_beta'),
        FilesetSpec('z', text_format, 'pipeline_beta')]

    add_param_specs = [
        ParamSpec('o1', 10),
        ParamSpec('o2', '20'),
        ParamSpec('o3', 30.0),
        ParamSpec('product_op', None, dtype=str)]  # To be set to 'product'

    def pipeline_beta(self, **name_maps):
        pipeline = self.new_pipeline(
            name='pipeline_beta',
            desc="A dummy pipeline used to test MultiAnalysis class",
            citations=[],
            name_maps=name_maps)
        add1 = pipeline.add("add1", TestMath())
        add2 = pipeline.add("add2", TestMath())
        prod = pipeline.add("product", TestMath())
        add1.inputs.op = 'add'
        add2.inputs.op = 'add'
        if self.parameter('product_op') is None:
            raise NotSpecifiedRequiredParameter
        prod.inputs.op = self.parameter('product_op')
        add1.inputs.as_file = True
        add2.inputs.as_file = True
        prod.inputs.as_file = True
        # Connect inputs
        pipeline.connect_input('w', add1, 'x')
        pipeline.connect_input('x', add1, 'y')
        pipeline.connect_input('x', add2, 'x')
        # Connect nodes
        pipeline.connect(add1, 'z', add2, 'y')
        pipeline.connect(add1, 'z', prod, 'x')
        pipeline.connect(add2, 'z', prod, 'y')
        # Connect outputs
        pipeline.connect_output('y', add2, 'z')
        pipeline.connect_output('z', prod, 'z')
        return pipeline


class FullMultiAnalysis(with_metaclass(MultiAnalysisMetaClass, MultiAnalysis)):

    add_subcomp_specs = [
        SubCompSpec('ss1', AnalysisA,
                     {'x': 'a',
                      'y': 'b',
                      'z': 'd',
                      'o1': 'p1',
                      'o2': 'p2',
                      'o3': 'p3'}),
        SubCompSpec('ss2', AnalysisB,
                     {'w': 'b',
                      'x': 'c',
                      'y': 'e',
                      'z': 'f',
                      'o1': 'q1',
                      'o2': 'q2',
                      'o3': 'p3',
                      'product_op': 'required_op'})]

    add_data_specs = [
        InputFilesetSpec('a', text_format),
        InputFilesetSpec('b', text_format),
        InputFilesetSpec('c', text_format),
        FilesetSpec('d', text_format, 'pipeline_alpha_trans'),
        FilesetSpec('e', text_format, 'pipeline_beta_trans'),
        FilesetSpec('f', text_format, 'pipeline_beta_trans')]

    add_param_specs = [
        ParamSpec('p1', 100),
        ParamSpec('p2', '200'),
        ParamSpec('p3', 300.0),
        ParamSpec('q1', 150),
        ParamSpec('q2', '250'),
        ParamSpec('required_op', None, dtype=str)]

    pipeline_alpha_trans = MultiAnalysis.translate(
        'ss1', 'pipeline_alpha')
    pipeline_beta_trans = MultiAnalysis.translate(
        'ss2', 'pipeline_beta')


class PartialMultiAnalysis(with_metaclass(MultiAnalysisMetaClass, MultiAnalysis)):

    add_subcomp_specs = [
        SubCompSpec('ss1', AnalysisA,
                     {'x': 'a', 'y': 'b', 'o1': 'p1'}),
        SubCompSpec('ss2', AnalysisB,
                     {'w': 'b', 'x': 'c', 'o1': 'p1'})]

    add_data_specs = [
        InputFilesetSpec('a', text_format),
        InputFilesetSpec('b', text_format),
        InputFilesetSpec('c', text_format)]

    pipeline_alpha_trans = MultiAnalysis.translate(
        'ss1', 'pipeline_alpha')

    add_param_specs = [
        ParamSpec('p1', 1000)]


class MultiMultiAnalysis(with_metaclass(MultiAnalysisMetaClass, MultiAnalysis)):

    add_subcomp_specs = [
        SubCompSpec('ss1', AnalysisA),
        SubCompSpec('full', FullMultiAnalysis),
        SubCompSpec('partial', PartialMultiAnalysis)]

    add_data_specs = [
        FilesetSpec('g', text_format, 'combined_pipeline')]

    add_param_specs = [
        ParamSpec('combined_op', 'add')]

    def combined_pipeline(self, **name_maps):
        pipeline = self.new_pipeline(
            name='combined',
            desc=(
                "A dummy pipeline used to test MultiMultiAnalysis class"),
            citations=[],
            name_maps=name_maps)
        merge = pipeline.add("merge", Merge(3))
        math = pipeline.add("math", TestMath())
        math.inputs.op = self.parameter('combined_op')
        math.inputs.as_file = True
        # Connect inputs
        pipeline.connect_input('ss1_z', merge, 'in1')
        pipeline.connect_input('full_e', merge, 'in2')
        pipeline.connect_input('partial_ss2_z', merge, 'in3')
        # Connect nodes
        pipeline.connect(merge, 'out', math, 'x')
        # Connect outputs
        pipeline.connect_output('g', math, 'z')
        return pipeline


class TestMulti(BaseTestCase):

    INPUT_FILESETS = {'ones': '1'}

    def test_full_multi_analysis(self):
        analysis = self.create_analysis(
            FullMultiAnalysis, 'full',
            [InputFilesets('a', 'ones', text_format),
             InputFilesets('b', 'ones', text_format),
             InputFilesets('c', 'ones', text_format)],
            parameters=[Parameter('required_op', 'mul')])
        d, e, f = analysis.data(('d', 'e', 'f'),
                             subject_id='SUBJECT', visit_id='VISIT')
        self.assertContentsEqual(d, 2.0)
        self.assertContentsEqual(e, 3.0)
        self.assertContentsEqual(f, 6.0)
        # Test parameter values in MultiAnalysis
        self.assertEqual(analysis._get_parameter('p1').value, 100)
        self.assertEqual(analysis._get_parameter('p2').value, '200')
        self.assertEqual(analysis._get_parameter('p3').value, 300.0)
        self.assertEqual(analysis._get_parameter('q1').value, 150)
        self.assertEqual(analysis._get_parameter('q2').value, '250')
        self.assertEqual(analysis._get_parameter('required_op').value, 'mul')
        # Test parameter values in SubComp
        ss1 = analysis.subcomp('ss1')
        self.assertEqual(ss1._get_parameter('o1').value, 100)
        self.assertEqual(ss1._get_parameter('o2').value, '200')
        self.assertEqual(ss1._get_parameter('o3').value, 300.0)
        ss2 = analysis.subcomp('ss2')
        self.assertEqual(ss2._get_parameter('o1').value, 150)
        self.assertEqual(ss2._get_parameter('o2').value, '250')
        self.assertEqual(ss2._get_parameter('o3').value, 300.0)
        self.assertEqual(ss2._get_parameter('product_op').value, 'mul')

    def test_partial_multi_analysis(self):
        analysis = self.create_analysis(
            PartialMultiAnalysis, 'partial',
            [InputFilesets('a', 'ones', text_format),
             InputFilesets('b', 'ones', text_format),
             InputFilesets('c', 'ones', text_format)],
            parameters=[Parameter('ss2_product_op', 'mul')])
        ss1_z = analysis.data('ss1_z',
                           subject_id='SUBJECT', visit_id='VISIT')
        ss2_z = list(analysis.data('ss2_z'))[0]
        self.assertContentsEqual(ss1_z, 2.0)
        self.assertContentsEqual(analysis.data('ss2_y'), 3.0)
        self.assertContentsEqual(ss2_z, 6.0)
        # Test parameter values in MultiAnalysis
        self.assertEqual(analysis._get_parameter('p1').value, 1000)
        self.assertEqual(analysis._get_parameter('ss1_o2').value, '2')
        self.assertEqual(analysis._get_parameter('ss1_o3').value, 3.0)
        self.assertEqual(analysis._get_parameter('ss2_o2').value, '20')
        self.assertEqual(analysis._get_parameter('ss2_o3').value, 30.0)
        self.assertEqual(analysis._get_parameter('ss2_product_op').value, 'mul')
        # Test parameter values in SubComp
        ss1 = analysis.subcomp('ss1')
        self.assertEqual(ss1._get_parameter('o1').value, 1000)
        self.assertEqual(ss1._get_parameter('o2').value, '2')
        self.assertEqual(ss1._get_parameter('o3').value, 3.0)
        ss2 = analysis.subcomp('ss2')
        self.assertEqual(ss2._get_parameter('o1').value, 1000)
        self.assertEqual(ss2._get_parameter('o2').value, '20')
        self.assertEqual(ss2._get_parameter('o3').value, 30.0)
        self.assertEqual(ss2._get_parameter('product_op').value, 'mul')

    def test_multi_multi_analysis(self):
        analysis = self.create_analysis(
            MultiMultiAnalysis, 'multi_multi',
            [InputFilesets('ss1_x', 'ones', text_format),
             InputFilesets('ss1_y', 'ones', text_format),
             InputFilesets('full_a', 'ones', text_format),
             InputFilesets('full_b', 'ones', text_format),
             InputFilesets('full_c', 'ones', text_format),
             InputFilesets('partial_a', 'ones', text_format),
             InputFilesets('partial_b', 'ones', text_format),
             InputFilesets('partial_c', 'ones', text_format)],
            parameters=[Parameter('full_required_op', 'mul'),
                        Parameter('partial_ss2_product_op', 'mul')])
        self.assertContentsEqual(analysis.data('g'), 11.0)
        # Test parameter values in MultiAnalysis
        self.assertEqual(analysis._get_parameter('full_p1').value, 100)
        self.assertEqual(analysis._get_parameter('full_p2').value, '200')
        self.assertEqual(analysis._get_parameter('full_p3').value, 300.0)
        self.assertEqual(analysis._get_parameter('full_q1').value, 150)
        self.assertEqual(analysis._get_parameter('full_q2').value, '250')
        self.assertEqual(analysis._get_parameter('full_required_op').value,
                         'mul')
        # Test parameter values in SubComp
        ss1 = analysis.subcomp('full').subcomp('ss1')
        self.assertEqual(ss1._get_parameter('o1').value, 100)
        self.assertEqual(ss1._get_parameter('o2').value, '200')
        self.assertEqual(ss1._get_parameter('o3').value, 300.0)
        ss2 = analysis.subcomp('full').subcomp('ss2')
        self.assertEqual(ss2._get_parameter('o1').value, 150)
        self.assertEqual(ss2._get_parameter('o2').value, '250')
        self.assertEqual(ss2._get_parameter('o3').value, 300.0)
        self.assertEqual(ss2._get_parameter('product_op').value, 'mul')
        # Test parameter values in MultiAnalysis
        self.assertEqual(analysis._get_parameter('partial_p1').value, 1000)
        self.assertEqual(analysis._get_parameter('partial_ss1_o2').value, '2')
        self.assertEqual(analysis._get_parameter('partial_ss1_o3').value, 3.0)
        self.assertEqual(analysis._get_parameter('partial_ss2_o2').value, '20')
        self.assertEqual(analysis._get_parameter('partial_ss2_o3').value, 30.0)
        self.assertEqual(
            analysis._get_parameter('partial_ss2_product_op').value, 'mul')
        # Test parameter values in SubComp
        ss1 = analysis.subcomp('partial').subcomp('ss1')
        self.assertEqual(ss1._get_parameter('o1').value, 1000)
        self.assertEqual(ss1._get_parameter('o2').value, '2')
        self.assertEqual(ss1._get_parameter('o3').value, 3.0)
        ss2 = analysis.subcomp('partial').subcomp('ss2')
        self.assertEqual(ss2._get_parameter('o1').value, 1000)
        self.assertEqual(ss2._get_parameter('o2').value, '20')
        self.assertEqual(ss2._get_parameter('o3').value, 30.0)
        self.assertEqual(ss2._get_parameter('product_op').value, 'mul')

    def test_missing_parameter(self):
        # Misses the required 'full_required_op' parameter, which sets
        # the operation of the second node in AnalysisB's pipeline to
        # 'product'
        inputs = [InputFilesets('ss1_x', 'ones', text_format),
                  InputFilesets('ss1_y', 'ones', text_format),
                  InputFilesets('full_a', 'ones', text_format),
                  InputFilesets('full_b', 'ones', text_format),
                  InputFilesets('full_c', 'ones', text_format),
                  InputFilesets('partial_a', 'ones', text_format),
                  InputFilesets('partial_b', 'ones', text_format),
                  InputFilesets('partial_c', 'ones', text_format)]
        missing_parameter_analysis = self.create_analysis(
            MultiMultiAnalysis, 'multi_multi',
            inputs,
            parameters=[
                Parameter('partial_ss2_product_op', 'mul')])
        self.assertRaises(
            NotSpecifiedRequiredParameter,
            missing_parameter_analysis.data,
            'g')
        missing_parameter_analysis2 = self.create_analysis(
            MultiMultiAnalysis, 'multi_multi',
            inputs,
            parameters=[Parameter('full_required_op', 'mul')])
        self.assertRaises(
            NotSpecifiedRequiredParameter,
            missing_parameter_analysis2.data,
            'g')
        provided_parameters_analysis = self.create_analysis(
            MultiMultiAnalysis, 'multi_multi',
            inputs,
            parameters=[
                Parameter('partial_ss2_product_op', 'mul'),
                Parameter('full_required_op', 'mul')])
        g = list(provided_parameters_analysis.data('g'))[0]
        self.assertContentsEqual(g, 11.0)
