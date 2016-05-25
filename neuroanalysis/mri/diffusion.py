from nipype.pipeline import engine as pe
from nipype.interfaces.mrtrix3.utils import BrainMask
from ..interfaces.mrtrix import DWIPreproc, MRCat
from ..interfaces.noddi import CreateROI
from .t2 import T2Dataset
from ..interfaces.mrtrix import MRConvert, ExtractFSLGradients
from neuroanalysis.citations import (
    mrtrix_cite, fsl_cite, eddy_cite, topup_cite, distort_correct_cite,
    noddi_cite)
from neuroanalysis.file_formats import (
    mrtrix_format, nifti_gz_format, fsl_bvecs_format, fsl_bvals_format)
from neuroanalysis.requirements import Requirement


class DiffusionDataset(T2Dataset):

    def preprocess_pipeline(self, phase_encode_direction='AP'):
        """
        Performs a series of FSL preprocessing steps, including Eddy and Topup

        Parameters
        ----------
        phase_encode_direction : str{AP|LR|IS}
            The phase encode direction
        """
        pipeline = self._create_pipeline(
            name='preprocess',
            inputs=['dwi', 'forward_rpe', 'reverse_rpe'],
            outputs=['preprocessed'],
            description="Preprocess dMRI datasets using distortion correction",
            options={'phase_encode_direction': phase_encode_direction},
            requirements=[Requirement('mrtrix3', min_version=(0, 3, 12)),
                          Requirement('fsl', min_version=(5, 0))],
            citations=[fsl_cite, eddy_cite, topup_cite, distort_correct_cite],
            approx_runtime=30)
        # Create preprocessing node
        dwipreproc = pe.Node(DWIPreproc(), name='dwipreproc')
        dwipreproc.inputs.pe_dir = phase_encode_direction
        # Create nodes to convert preprocessed scan and gradients to FSL format
        mrconvert = pe.Node(MRConvert(), name='mrconvert')
        pipeline.connect(dwipreproc, 'out_file', mrconvert, 'in_file')
        extract_grad = pe.Node(ExtractFSLGradients(), name="extract_grad")
        pipeline.connect(dwipreproc, 'out_file', extract_grad, 'in_file')
        # Connect inputs
        pipeline.connect_input('dwi', dwipreproc, 'in_file')
        pipeline.connect_input('forward_rpe', dwipreproc, 'forward_rpe')
        pipeline.connect_input('reverse_rpe', dwipreproc, 'reverse_rpe')
        # Connect outputs
        pipeline.connect_output('preprocessed', mrconvert, 'out_file')
        pipeline.connect_output('bvecs', extract_grad, 'bvecs_file')
        pipeline.connect_output('bvals', extract_grad, 'bvals_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    def test_extract_pipeline(self):
        """
        Performs a series of FSL preprocessing steps, including Eddy and Topup

        Parameters
        ----------
        phase_encode_direction : str{AP|LR|IS}
            The phase encode direction
        """
        pipeline = self._create_pipeline(
            name='test_extract',
            inputs=['preprocessed'],
            outputs=['converted', 'bvecs', 'bvals'],
            description="Test conversion and extract",
            options={},
            requirements=[Requirement('mrtrix3', min_version=(0, 3, 12)),
                          Requirement('fsl', min_version=(5, 0))],
            citations=[mrtrix_cite],
            approx_runtime=5)
        # Create nodes to convert preprocessed scan and gradients to FSL format
        mrconvert = pe.Node(MRConvert(), name='mrconvert')
        mrconvert.inputs.out_ext = 'nii.gz'
        extract_grad = pe.Node(ExtractFSLGradients(), name="extract_grad")
        # Connect inputs
        pipeline.connect_input('preprocessed', mrconvert, 'in_file')
        pipeline.connect_input('preprocessed', extract_grad, 'in_file')
        # Connect outputs
        pipeline.connect_output('converted', mrconvert, 'out_file')
        pipeline.connect_output('bvecs', extract_grad, 'bvecs_file')
        pipeline.connect_output('bvals', extract_grad, 'bvals_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    def brain_mask_pipeline(self):
        """
        Generates a whole brain mask using MRtrix's 'dwi2mask' command
        """
        pipeline = self._create_pipeline(
            name='brain_mask',
            inputs=['preprocessed'],
            outputs=['brain_mask'],
            description="Generate brain mask from b0 images",
            options={},
            requirements=[Requirement('mrtrix3', min_version=(0, 3, 12))],
            citations=[mrtrix_cite], approx_runtime=1)
        # Create mask node
        dwi2mask = pe.Node(BrainMask(), name='dwi2mask')
        dwi2mask.inputs.out_file = 'brain_mask.mif'
        # Connect inputs/outputs
        pipeline.connect_input('preprocessed', dwi2mask, 'in_file')
        pipeline.connect_output('brain_mask', dwi2mask, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    def fod_pipeline(self):
        raise NotImplementedError

    # The list of dataset components that are acquired by the scanner
    acquired_components = {
        'dwi': mrtrix_format, 'forward_rpe': mrtrix_format,
        'reverse_rpe': mrtrix_format}

    generated_components = {
        'fod': (fod_pipeline, mrtrix_format),
        'brain_mask': (brain_mask_pipeline, mrtrix_format),
        'preprocessed': (preprocess_pipeline, mrtrix_format),
        'converted': (test_extract_pipeline, nifti_gz_format),
        'bvecs': (test_extract_pipeline, fsl_bvecs_format),
        'bvals': (test_extract_pipeline, fsl_bvals_format)}


class NODDIDataset(DiffusionDataset):

    def concatenate_pipeline(self):
        """
        Concatenates two dMRI scans (with different b-values) along the
        DW encoding (4th) axis
        """
        pipeline = self._create_pipeline(
            name='concatenation',
            inputs=['low_b_dw_scan', 'high_b_dw_scan'],
            outputs=['dwi'],
            description=(
                "Concatenate low and high b-value dMRI scans for NODDI "
                "processing"),
            options={},
            requirements=[Requirement('mrtrix3', min_version=(0, 3, 12))],
            citations=[mrtrix_cite], approx_runtime=1)
        # Create concatenation node
        mrcat = pe.Node(MRCat(), name='mrcat')
        # Connect inputs/outputs
        pipeline.connect_input('low_b_dw_scan', mrcat, 'first_scan')
        pipeline.connect_input('high_b_dw_scan', mrcat, 'second_scan')
        pipeline.connect_output('dwi', mrcat, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    def create_roi_pipeline(self):
        """
        Creates a ROI in which the NODDI processing will be performed
        """
        pipeline = self._create_pipeline(
            name='create_ROI',
            inputs=['preprocessed', 'brain_mask'],
            outputs=['roi'],
            description=(
                "Creates a ROI in which the NODDI processing will be "
                "performed"),
            options={},
            requirements=[Requirement('matlab', min_version=(2016, 'a')),
                          Requirement('noddi', min_version=(0, 9)),
                          Requirement('niftimatlib', (1, 2))],
            citations=[noddi_cite], approx_runtime=60)
        # Create concatenation node
        create_roi = pe.Node(CreateROI(), name='create_roi')
        # Connect inputs/outputs
        pipeline.connect_input('preprocessed', create_roi, 'in_file')
        pipeline.connect_input('brain_mask', create_roi, 'brain_mask')
        pipeline.connect_output('roi', create_roi, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    acquired_components = acquired_components = {
        'low_b_dw_scan': mrtrix_format, 'high_b_dw_scan': mrtrix_format,
        'forward_rpe': mrtrix_format, 'reverse_rpe': mrtrix_format}

    generated_components = dict(
        DiffusionDataset.generated_components.items() +
        [('dwi', (concatenate_pipeline, mrtrix_format)),
         ('roi', (create_roi_pipeline, mrtrix_format))])
