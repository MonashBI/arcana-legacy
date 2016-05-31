from nipype.pipeline import engine as pe
from nipype.interfaces import fsl
from .base import MRDataset
from neuroanalysis.requirements import Requirement
from neuroanalysis.citations import fsl_cite, bet_cite, bet2_cite
from neuroanalysis.file_formats import mrtrix_format, nifti_gz_format


class T2Dataset(MRDataset):

    def brain_mask_pipeline(self, **kwargs):  # @UnusedVariable
        """
        Generates a whole brain mask using MRtrix's 'dwi2mask' command
        """
        pipeline = self._create_pipeline(
            name='brain_mask',
            inputs=['preprocessed'],
            outputs=['brain_mask'],
            description="Generate brain mask from T2",
            options={},
            requirements=[Requirement('fsl', min_version=(0, 5, 0))],
            citations=[fsl_cite, bet_cite, bet2_cite], approx_runtime=5)
        # Create mask node
        bet = pe.Node(interface=fsl.BET(), name="bet")
        bet.inputs.mask = True
        # Connect inputs/outputs
        pipeline.connect_input('primary_scan', bet, 'in_file')
        pipeline.connect_output('brain_mask', bet, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    acquired_components = {'primary_scan': mrtrix_format}

    generated_components = {'brain_mask': nifti_gz_format}
