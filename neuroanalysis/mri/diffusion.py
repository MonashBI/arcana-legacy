from nipype.pipeline import engine as pe
from nipype.interfaces.mrtrix3.utils import BrainMask
from ..interfaces.mrtrix import DWIPreproc, MRCat
from .t2 import T2Dataset
from ..base import Citation


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
            citations=[
                Citation(
                    authors=["Andersson, J. L.", "Sotiropoulos, S. N."],
                    title=(
                        "An integrated approach to correction for "
                        "off-resonance effects and subject movement in "
                        "diffusion MR imaging"),
                    journal="NeuroImage", year=2015, vol=125,
                    pages="1063-1078"),
                Citation(
                    authors=["Smith, S. M.", "Jenkinson, M.",
                             "Woolrich, M. W.", "Beckmann, C. F.",
                             "Behrens, T. E.", "Johansen- Berg, H.",
                             "Bannister, P. R.", "De Luca, M.", "Drobnjak, I.",
                             "Flitney, D. E.", "Niazy, R. K.", "Saunders, J.",
                             "Vickers, J.", "Zhang, Y.", "De Stefano, N.",
                             "Brady, J. M. & Matthews, P. M."],
                    title=(
                        "Advances in functional and structural MR image "
                        "analysis and implementation as FSL"),
                    journal="NeuroImage", year=2004, vol=23,
                    pages="S208-S219"),
                Citation(
                    authors=["Skare, S.", "Bammer, R."],
                    title=(
                        "Jacobian weighting of distortion corrected EPI data"),
                    journal=(
                        "Proceedings of the International Society for Magnetic"
                        " Resonance in Medicine"), year=2010, pages="5063"),
                Citation(
                    authors=["Andersson, J. L.", "Skare, S. & Ashburner, J."],
                    title=(
                        "How to correct susceptibility distortions in "
                        "spin-echo echo-planar images: application to "
                        "diffusion tensor imaging"),
                    journal="NeuroImage", year=2003, vol=20,
                    pages="870-888")], requirements=['mrtrix3', 'fsl'])
        # Create preprocessing node
        dwipreproc = pe.Node(DWIPreproc(), name='dwipreproc')
        dwipreproc.inputs.pe_dir = phase_encode_direction
        dwipreproc.inputs.out_file = 'preprocessed.mif'
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
            options={}, citations=[], requirements=['mrtrix3'])
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
        'dwi': 'mrtrix', 'forward_rpe': 'mrtrix', 'reverse_rpe': 'mrtrix'}

    generated_components = {
        'fod': (fod_pipeline, 'mrtrix'),
        'brain_mask': (brain_mask_pipeline, 'mrtrix'),
        'preprocessed': (preprocess_pipeline, 'nifti_gz')}


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
            citations=[], options={}, requirements=['matlab', 'noddi'])
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
        'low_b_dw_scan': 'mrtrix', 'high_b_dw_scan': 'mrtrix',
        'forward_rpe': 'mrtrix', 'reverse_rpe': 'mrtrix'}

    generated_components = dict(
        DiffusionDataset.generated_components.items() +
        [('dwi', (concatenate_pipeline, 'mrtrix'))])
