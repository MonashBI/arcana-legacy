from nipype.pipeline import engine as pe
from nipype.interfaces.mrtrix3.utils import BrainMask
from ..interfaces.mrtrix import DWIPreproc, MRCat
from .t2 import T2Dataset
from neuroanalysis.citations import (
    mrtrix_cite, fsl_cite, eddy_cite, topup_cite, distort_correct_cite)
from neuroanalysis.file_formats import (
    mrtrix_format, nifti_gz_format)


class DiffusionDataset(T2Dataset):

    def preprocess_pipeline(self, phase_encode_direction='AP'):
        """
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
            requirements=['mrtrix3', 'fsl'],
            citations=[fsl_cite, eddy_cite, topup_cite, distort_correct_cite])
        # Create preprocessing node
        dwipreproc = pe.Node(DWIPreproc(), name='dwipreproc')
        dwipreproc.inputs.pe_dir = phase_encode_direction
        # Connect inputs/outputs
        pipeline.connect_input('dwi', dwipreproc, 'in_file')
        pipeline.connect_input('forward_rpe', dwipreproc, 'forward_rpe')
        pipeline.connect_input('reverse_rpe', dwipreproc, 'reverse_rpe')
        pipeline.connect_output('preprocessed', dwipreproc, 'out_file')
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
            requirements=['mrtrix3'],
            citations=[mrtrix_cite])
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
        'preprocessed': (preprocess_pipeline, nifti_gz_format)}


class NODDIDataset(DiffusionDataset):

    def concatenate_pipeline(self):
        """
        Parameters
        ----------
        phase_encode_direction : str{AP|LR|IS}
            The phase encode direction
        """
        pipeline = self._create_pipeline(
            name='concatenation',
            inputs=['low_b_dw_scan', 'high_b_dw_scan'],
            outputs=['dwi'],
            description=(
                "Concatenate low and high b-value dMRI scans for NODDI "
                "processing"),
            options={},
            requirements=['matlab', 'noddi'],
            citations=[mrtrix_cite])
        # Create concatenation node
        mrcat = pe.Node(MRCat(), name='mrcat')
        # Connect inputs/outputs
        pipeline.connect_input('low_b_dw_scan', mrcat, 'first_scan')
        pipeline.connect_input('high_b_dw_scan', mrcat, 'second_scan')
        pipeline.connect_output('dwi', mrcat, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    acquired_components = acquired_components = {
        'low_b_dw_scan': mrtrix_format, 'high_b_dw_scan': mrtrix_format,
        'forward_rpe': mrtrix_format, 'reverse_rpe': mrtrix_format}

    generated_components = dict(
        DiffusionDataset.generated_components.items() +
        [('dwi', (concatenate_pipeline, mrtrix_format))])
