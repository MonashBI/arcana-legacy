from nianalysis.testing import BaseTestCase as TestCase
import subprocess as sp
from nianalysis.requirements import Requirement
from nianalysis.interfaces.utils import Merge
from nianalysis.dataset import DatasetMatch, DatasetSpec
from nianalysis.data_formats import mrtrix_format
from nianalysis.requirements import mrtrix3_req
from nianalysis.study.base import Study, set_specs
from nianalysis.study.multi import (
    MultiStudy, SubStudySpec, MultiStudyMetaClass)
from nianalysis.interfaces.mrtrix import MRMath
from nianalysis.nodes import NiAnalysisNodeMixin  # @IgnorePep8
from nianalysis.exceptions import NiAnalysisModulesNotInstalledException  # @IgnorePep8


class StudyA(Study):

    def pipeline_alpha(self, **kwargs):  # @UnusedVariable
        pipeline = self.create_pipeline(
            name='pipeline_alpha',
            inputs=[DatasetSpec('x', mrtrix_format),
                    DatasetSpec('y', mrtrix_format)],
            outputs=[DatasetSpec('z', mrtrix_format)],
            description="A dummy pipeline used to test MultiStudy class",
            alterations=kwargs,
            version=1,
            citations=[])
        merge = pipeline.create_node(Merge(2), name="merge")
        mrmath = pipeline.create_node(MRMath(), name="mrmath",
                                      requirements=[mrtrix3_req])
        mrmath.inputs.operation = 'sum'
        # Connect inputs
        pipeline.connect_input('x', merge, 'in1')
        pipeline.connect_input('y', merge, 'in2')
        # Connect nodes
        pipeline.connect(merge, 'out', mrmath, 'in_files')
        # Connect outputs
        pipeline.connect_output('z', mrmath, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    _data_specs = set_specs(
        DatasetSpec('x', mrtrix_format),
        DatasetSpec('y', mrtrix_format),
        DatasetSpec('z', mrtrix_format, 'pipeline_alpha'))

    default_options = {}


class StudyB(Study):

    def pipeline_beta(self, **kwargs):  # @UnusedVariable
        pipeline = self.create_pipeline(
            name='pipeline_beta',
            inputs=[DatasetSpec('w', mrtrix_format),
                    DatasetSpec('x', mrtrix_format)],
            outputs=[DatasetSpec('y', mrtrix_format),
                     DatasetSpec('z', mrtrix_format)],
            description="A dummy pipeline used to test MultiStudy class",
            alterations=kwargs,
            version=1,
            citations=[])
        merge1 = pipeline.create_node(Merge(2), name='merge1')
        merge2 = pipeline.create_node(Merge(2), name='merge2')
        merge3 = pipeline.create_node(Merge(2), name='merge3')
        mrsum1 = pipeline.create_node(MRMath(), name="mrsum1",
                                      requirements=[mrtrix3_req])
        mrsum1.inputs.operation = 'sum'
        mrsum2 = pipeline.create_node(MRMath(), name="mrsum2",
                                      requirements=[mrtrix3_req])
        mrsum2.inputs.operation = 'sum'
        mrproduct = pipeline.create_node(MRMath(), name="mrproduct",
                                         requirements=[mrtrix3_req])
        mrproduct.inputs.operation = 'product'
        # Connect inputs
        pipeline.connect_input('w', merge1, 'in1')
        pipeline.connect_input('x', merge1, 'in2')
        pipeline.connect_input('x', merge2, 'in1')
        # Connect nodes
        pipeline.connect(merge1, 'out', mrsum1, 'in_files')
        pipeline.connect(mrsum1, 'out_file', merge2, 'in2')
        pipeline.connect(merge2, 'out', mrsum2, 'in_files')
        pipeline.connect(mrsum1, 'out_file', merge3, 'in1')
        pipeline.connect(mrsum2, 'out_file', merge3, 'in2')
        pipeline.connect(merge3, 'out', mrproduct, 'in_files')
        # Connect outputs
        pipeline.connect_output('y', mrsum2, 'out_file')
        pipeline.connect_output('z', mrproduct, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    _data_specs = set_specs(
        DatasetSpec('w', mrtrix_format),
        DatasetSpec('x', mrtrix_format),
        DatasetSpec('y', mrtrix_format, 'pipeline_beta'),
        DatasetSpec('z', mrtrix_format, 'pipeline_beta'))


class FullMultiStudy(MultiStudy):

    __metaclass__ = MultiStudyMetaClass

    pipeline_alpha_trans = MultiStudy.translate(
        'ss1', StudyA.pipeline_alpha)
    pipeline_beta_trans = MultiStudy.translate(
        'ss2', StudyB.pipeline_beta)

    _sub_study_specs = set_specs(
        SubStudySpec('ss1', StudyA,
                     {'a': 'x', 'b': 'y', 'd': 'z'}),
        SubStudySpec('ss2', StudyB,
                     {'b': 'w', 'c': 'x', 'e': 'y', 'f': 'z'}))

    _data_specs = set_specs(
        DatasetSpec('a', mrtrix_format),
        DatasetSpec('b', mrtrix_format),
        DatasetSpec('c', mrtrix_format),
        DatasetSpec('d', mrtrix_format, 'pipeline_alpha_trans'),
        DatasetSpec('e', mrtrix_format, 'pipeline_beta_trans'),
        DatasetSpec('f', mrtrix_format, 'pipeline_beta_trans'))


class PartialMultiStudy(MultiStudy):

    __metaclass__ = MultiStudyMetaClass

    pipeline_alpha_trans = MultiStudy.translate(
        'ss1', StudyA.pipeline_alpha)

    _sub_study_specs = set_specs(
        SubStudySpec('ss1', StudyA,
                     {'a': 'x', 'b': 'y'}),
        SubStudySpec('ss2', StudyB,
                     {'b': 'w', 'c': 'x'}))

    _data_specs = set_specs(
        DatasetSpec('a', mrtrix_format),
        DatasetSpec('b', mrtrix_format),
        DatasetSpec('c', mrtrix_format))


class MultiMultiStudy(MultiStudy):

    __metaclass__ = MultiStudyMetaClass

    def combined_pipeline(self, **kwargs):
        pipeline = self.create_pipeline(
            name='combined',
            inputs=[DatasetSpec('ss1_z', mrtrix_format),
                    DatasetSpec('full_e', mrtrix_format),
                    DatasetSpec('partial_ss2_z', mrtrix_format)],
            outputs=[DatasetSpec('g', mrtrix_format)],
            description=(
                "A dummy pipeline used to test MultiMultiStudy class"),
            alterations=kwargs,
            version=1,
            citations=[])
        merge = pipeline.create_node(Merge(3), name="merge")
        mrmath = pipeline.create_node(MRMath(), name="mrmath",
                                      requirements=[mrtrix3_req])
        mrmath.inputs.operation = 'sum'
        # Connect inputs
        pipeline.connect_input('ss1_z', merge, 'in1')
        pipeline.connect_input('full_e', merge, 'in2')
        pipeline.connect_input('partial_ss2_z', merge, 'in3')
        # Connect nodes
        pipeline.connect(merge, 'out', mrmath, 'in_files')
        # Connect outputs
        pipeline.connect_output('g', mrmath, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    _sub_study_specs = set_specs(
        SubStudySpec('ss1', StudyA, {}),
        SubStudySpec('full', FullMultiStudy),
        SubStudySpec('partial', PartialMultiStudy))
    _data_specs = set_specs(
        DatasetSpec('g', mrtrix_format, 'combined_pipeline'))


class TestMulti(TestCase):

    def setUp(self):
        super(TestMulti, self).setUp()
        # Calculate MRtrix module required for 'mrstats' commands
        try:
            self.mrtrix_req = Requirement.best_requirement(
                [mrtrix3_req], NiAnalysisNodeMixin.available_modules(),
                NiAnalysisNodeMixin.preloaded_modules())
        except NiAnalysisModulesNotInstalledException:
            self.mrtrix_req = None

    def test_full_multi_study(self):
        study = self.create_study(
            FullMultiStudy, 'full', [
                DatasetMatch('a', 'ones', mrtrix_format),
                DatasetMatch('b', 'ones', mrtrix_format),
                DatasetMatch('c', 'ones', mrtrix_format)])
        study.pipeline_alpha_trans().run(work_dir=self.work_dir)
        study.pipeline_beta_trans().run(work_dir=self.work_dir)
        if self.mrtrix_req is not None:
            NiAnalysisNodeMixin.load_module(*self.mrtrix_req)
        try:
            d_mean = float(sp.check_output(
                'mrstats {} -output mean'.format(self.output_file_path(
                    'd.mif', study.name)),
                shell=True))
            self.assertEqual(d_mean, 2.0)
            e_mean = float(sp.check_output(
                'mrstats {} -output mean'.format(self.output_file_path(
                    'e.mif', study.name)),
                shell=True))
            self.assertEqual(e_mean, 3.0)
            f_mean = float(sp.check_output(
                'mrstats {} -output mean'.format(self.output_file_path(
                    'f.mif', study.name)),
                shell=True))
            self.assertEqual(f_mean, 6.0)
        finally:
            if self.mrtrix_req is not None:
                NiAnalysisNodeMixin.unload_module(*self.mrtrix_req)

    def test_partial_multi_study(self):
        study = self.create_study(
            PartialMultiStudy, 'partial', [
                DatasetMatch('a', 'ones', mrtrix_format),
                DatasetMatch('b', 'ones', mrtrix_format),
                DatasetMatch('c', 'ones', mrtrix_format)])
        study.pipeline_alpha_trans().run(work_dir=self.work_dir)
        study.ss2_pipeline_beta().run(work_dir=self.work_dir)
        if self.mrtrix_req is not None:
            NiAnalysisNodeMixin.load_module(*self.mrtrix_req)
        try:
            ss1_z_mean = float(sp.check_output(
                'mrstats {} -output mean'.format(self.output_file_path(
                    'ss1_z.mif', study.name)),
                shell=True))
            self.assertEqual(ss1_z_mean, 2.0)
            ss2_y_mean = float(sp.check_output(
                'mrstats {} -output mean'.format(self.output_file_path(
                    'ss2_y.mif', study.name)),
                shell=True))
            self.assertEqual(ss2_y_mean, 3.0)
            ss2_z_mean = float(sp.check_output(
                'mrstats {} -output mean'.format(self.output_file_path(
                    'ss2_z.mif', study.name)),
                shell=True))
            self.assertEqual(ss2_z_mean, 6.0)
        finally:
            if self.mrtrix_req is not None:
                NiAnalysisNodeMixin.unload_module(*self.mrtrix_req)

    def test_multi_multi_study(self):
        study = self.create_study(
            MultiMultiStudy, 'partial', [
                DatasetMatch('ss1_x', 'ones', mrtrix_format),
                DatasetMatch('ss1_y', 'ones', mrtrix_format),
                DatasetMatch('full_a', 'ones', mrtrix_format),
                DatasetMatch('full_b', 'ones', mrtrix_format),
                DatasetMatch('full_c', 'ones', mrtrix_format),
                DatasetMatch('partial_a', 'ones', mrtrix_format),
                DatasetMatch('partial_b', 'ones', mrtrix_format),
                DatasetMatch('partial_c', 'ones', mrtrix_format)])
        study.combined_pipeline().run(work_dir=self.work_dir)
        if self.mrtrix_req is not None:
            NiAnalysisNodeMixin.load_module(*self.mrtrix_req)
        try:
            g_mean = float(sp.check_output(
                'mrstats {} -output mean'.format(self.output_file_path(
                    'g.mif', study.name)),
                shell=True))
            self.assertEqual(g_mean, 11.0)
        finally:
            if self.mrtrix_req is not None:
                NiAnalysisNodeMixin.unload_module(*self.mrtrix_req)
