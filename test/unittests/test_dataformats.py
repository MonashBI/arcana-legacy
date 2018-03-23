from unittest import TestCase
from nipype.interfaces.utility import IdentityInterface
from nianalysis.testing import BaseTestCase
from nianalysis.interfaces.mrtrix import MRConvert
from nipype.pipeline import engine as pe
from nianalysis.exceptions import NiAnalysisModulesNotInstalledException
from nianalysis.dataset import Dataset
from nianalysis.data_formats import (
    Converter, MrtrixConverter, dicom_format, mrtrix_format,
    get_converter_node, nifti_gz_format)
from nianalysis.requirements import Requirement
from nianalysis.nodes import Node
from nianalysis.study.base import Study, set_specs
from nianalysis.dataset import DatasetMatch, DatasetSpec


dummy_req = Requirement('name-for-module-that-will-never-exist',
                        min_version=(0, 3, 12))


class DummyConverter(Converter):

    requirements = [dummy_req]

    def _get_convert_node(self, node_name, input_format, output_format):  # @UnusedVariable @IgnorePep8
        convert_node = Node(IdentityInterface(['in_out']), name=node_name,
                            requirements=self.requirements)
        return convert_node, 'in_out', 'in_out'

    def input_formats(self):
        return [dicom_format]

    def output_formats(self):
        return [mrtrix_format]


class DummyStudy(Study):

    def pipeline(self):
        pipeline = self.create_pipeline(
            name='pipeline',
            inputs=[DatasetSpec('input', nifti_gz_format)],
            outputs=[DatasetSpec('output', nifti_gz_format)],
            description=("A dummy pipeline used to test dicom-to-nifti "
                         "conversion method"),
            default_options={},
            version=1,
            citations=[])
        identity = pipeline.create_node(IdentityInterface(['field']),
                                        name='identity')
        # Connect inputs
        pipeline.connect_input('input', identity, 'field')
        # Connect outputs
        pipeline.connect_output('output', identity, 'field')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    _data_specs = set_specs(
        DatasetSpec('input', dicom_format),
        DatasetSpec('output', nifti_gz_format, pipeline))


class TestConverterAvailability(TestCase):

    def setUp(self):
        try:
            Node.available_modules()
            self.modules_installed = True
        except NiAnalysisModulesNotInstalledException:
            self.modules_installed = False

    def test_find_mrtrix(self):
        dummy_dataset = Dataset('dummy', dicom_format)
        dummy_source = pe.Node(IdentityInterface(['input']), 'dummy_source')
        dummy_workflow = pe.Workflow('dummy_workflow')
        if self.modules_installed:
            converter_node, _ = get_converter_node(
                dummy_dataset, 'dummy', mrtrix_format,
                dummy_source, dummy_workflow, 'dummy_convert',
                converters=[DummyConverter(), MrtrixConverter()])
            self.assertIsInstance(converter_node.interface, MRConvert)


class TestDicom2Niix(BaseTestCase):

    def test_dcm2niix(self):
        study = self.create_study(
            DummyStudy, 'concatenate', inputs={
                'input': DatasetMatch('t2_tse_tra_p2_448', dicom_format)})
        study.pipeline().run(work_dir=self.work_dir)
        self.assertDatasetCreated('output.nii.gz', study.name)
