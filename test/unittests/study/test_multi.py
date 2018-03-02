from nianalysis.testing import BaseTestCase as TestCase
import subprocess as sp
from nianalysis.requirements import Requirement
from nianalysis.interfaces.utils import Merge
from nianalysis.dataset import Dataset, DatasetSpec
from nianalysis.data_formats import mrtrix_format
from nianalysis.requirements import mrtrix3_req
from nianalysis.study.base import Study, set_specs
from nianalysis.study.multi import (
    MultiStudy, translate_pipeline, SubStudySpec, MultiStudyMetaClass)
from nianalysis.interfaces.mrtrix import MRMath
from nianalysis.nodes import NiAnalysisNodeMixin  # @IgnorePep8
from nianalysis.exceptions import NiAnalysisModulesNotInstalledException  # @IgnorePep8


class DummySubStudyA(Study):

    def pipeline1(self, **kwargs):  # @UnusedVariable
        pipeline = self.create_pipeline(
            name='pipeline1',
            inputs=[DatasetSpec('x', mrtrix_format),
                    DatasetSpec('y', mrtrix_format)],
            outputs=[DatasetSpec('z', mrtrix_format)],
            description="A dummy pipeline used to test MultiStudy class",
            default_options={},
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
        DatasetSpec('z', mrtrix_format, pipeline1))


class DummySubStudyB(Study):

    def pipeline1(self, **kwargs):  # @UnusedVariable
        pipeline = self.create_pipeline(
            name='pipeline1',
            inputs=[DatasetSpec('w', mrtrix_format),
                    DatasetSpec('x', mrtrix_format)],
            outputs=[DatasetSpec('y', mrtrix_format),
                     DatasetSpec('z', mrtrix_format)],
            description="A dummy pipeline used to test MultiStudy class",
            default_options={},
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
        DatasetSpec('y', mrtrix_format, pipeline1),
        DatasetSpec('z', mrtrix_format, pipeline1))


class FullMultiStudy(MultiStudy):

    __metaclass__ = MultiStudyMetaClass

    pipeline_a1 = translate_pipeline('ss1', DummySubStudyA.pipeline1)
    pipeline_b1 = translate_pipeline('ss2', DummySubStudyB.pipeline1)

    _sub_study_specs = set_specs(
        SubStudySpec('ss1', DummySubStudyA,
                     {'a': 'x', 'b': 'y', 'd': 'z'}),
        SubStudySpec('ss2', DummySubStudyB,
                     {'b': 'w', 'c': 'x', 'e': 'y', 'f': 'z'}))

    _data_specs = set_specs(
        DatasetSpec('a', mrtrix_format),
        DatasetSpec('b', mrtrix_format),
        DatasetSpec('c', mrtrix_format),
        DatasetSpec('d', mrtrix_format, pipeline_a1),
        DatasetSpec('e', mrtrix_format, pipeline_b1),
        DatasetSpec('f', mrtrix_format, pipeline_b1))


class PartialMultiStudy(MultiStudy):

    __metaclass__ = MultiStudyMetaClass

    pipeline_a1 = translate_pipeline('ss1', DummySubStudyA.pipeline1)

    _sub_study_specs = set_specs(
        SubStudySpec('ss1', DummySubStudyA,
                     {'a': 'x', 'b': 'y'}),
        SubStudySpec('ss2', DummySubStudyB,
                     {'b': 'w', 'c': 'x'}))

    _data_specs = set_specs(
        DatasetSpec('a', mrtrix_format),
        DatasetSpec('b', mrtrix_format),
        DatasetSpec('c', mrtrix_format))


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
            FullMultiStudy, 'full', {
                'a': Dataset('ones', mrtrix_format),
                'b': Dataset('ones', mrtrix_format),
                'c': Dataset('ones', mrtrix_format)})
        study.pipeline_a1().run(work_dir=self.work_dir)
        study.pipeline_b1().run(work_dir=self.work_dir)
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
            PartialMultiStudy, 'partial', {
                'a': Dataset('ones', mrtrix_format),
                'b': Dataset('ones', mrtrix_format),
                'c': Dataset('ones', mrtrix_format)})
        study.pipeline_a1().run(work_dir=self.work_dir)
        study.ss2_pipeline1().run(work_dir=self.work_dir)
        if self.mrtrix_req is not None:
            NiAnalysisNodeMixin.load_module(*self.mrtrix_req)
        try:
            d_mean = float(sp.check_output(
                'mrstats {} -output mean'.format(self.output_file_path(
                    'ss1_z.mif', study.name)),
                shell=True))
            self.assertEqual(d_mean, 2.0)
            e_mean = float(sp.check_output(
                'mrstats {} -output mean'.format(self.output_file_path(
                    'ss2_y.mif', study.name)),
                shell=True))
            self.assertEqual(e_mean, 3.0)
            f_mean = float(sp.check_output(
                'mrstats {} -output mean'.format(self.output_file_path(
                    'ss2_z.mif', study.name)),
                shell=True))
            self.assertEqual(f_mean, 6.0)
        finally:
            if self.mrtrix_req is not None:
                NiAnalysisNodeMixin.unload_module(*self.mrtrix_req)
