from nipype.pipeline import engine as pe
from nipype.interfaces.mrtrix3.utils import BrainMask
from ..interfaces.mrtrix import DWIPreproc, MRCat
from ..base import Pipeline
from .t2 import T2Dataset
from nipype.interfaces.utility import IdentityInterface


class DiffusionDataset(T2Dataset):

    def preprocess_pipeline(self, phase_encode_direction='AP'):
        """
        Parameters
        ----------
        phase_encode_direction : str{AP|LR|IS}
            The phase encode direction
        """
        inputs = ('dwi', 'forward_rpe', 'reverse_rpe')
        outputs = ('preprocessed',)
        # FIXME: Would ideally extract the phase-encode direction from the
        #        image header
        options = {'phase_encode_direction': phase_encode_direction}
        citations = []  # FIXME: Need to add citations
        inputnode = pe.Node(IdentityInterface(fields=inputs),
                            name="preprocess_inputnode")
        dwipreproc = pe.Node(DWIPreproc(), name='dwipreproc')
        dwipreproc.inputs.pe_dir = phase_encode_direction
        dwipreproc.inputs.out_file = 'preprocessed.mif'
        outputnode = pe.Node(IdentityInterface(fields=outputs),
                             name="preprocess_outputnode")
        workflow = pe.Workflow(name='preprocess')
        workflow.connect(inputnode, 'dwi', dwipreproc, 'in_file')
        workflow.connect(inputnode, 'forward_rpe', dwipreproc, 'forward_rpe')
        workflow.connect(inputnode, 'reverse_rpe', dwipreproc, 'reverse_rpe')
        workflow.connect(dwipreproc, 'out_file', outputnode, 'preprocessed')
        return Pipeline(
            dataset=self, name='preprocess', workflow=workflow,
            inputs=inputs, outputs=outputs, inputnode=inputnode,
            outputnode=outputnode, description=(
                "Preprocesses dMRI datasets using distortion correction"),
            citations=citations, options=options,
            requirements=['mrtrix3', 'fsl'])

    def brain_mask_pipeline(self):
        """
        Generates a whole brain mask using MRtrix's 'dwi2mask' command
        """
        inputs = ('preprocessed',)
        outputs = ('brain_mask',)
        # FIXME: Would ideally extract the phase-encode direction from the
        #        image header
        options = {}
        citations = []  # FIXME: Need to add citations
        inputnode = pe.Node(IdentityInterface(fields=inputs),
                            name="brain_mask_inputnode")
        dwi2mask = pe.Node(BrainMask(), name='dwi2mask')
        dwi2mask.inputs.out_file = 'brain_mask.mif'
        outputnode = pe.Node(IdentityInterface(fields=outputs),
                             name="brain_mask_outputnode")
        workflow = pe.Workflow(name='preprocess')
        workflow.connect(inputnode, 'preprocessed', dwi2mask, 'in_file')
        workflow.connect(dwi2mask, 'out_file', outputnode, 'brain_mask')
        return Pipeline(
            dataset=self, name='brain_mask', workflow=workflow,
            inputs=inputs, outputs=outputs, inputnode=inputnode,
            outputnode=outputnode, description=(
                "Generate brain mask from b0 images"),
            citations=citations, options=options,
            requirements=['mrtrix3'])

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
        inputs = ('low_b_dw_scan', 'high_b_dw_scan')
        outputs = ('dwi',)
        options = {}
        citations = []
        inputnode = pe.Node(IdentityInterface(fields=inputs),
                            name="concatenation_inputnode")
        mrcat = pe.Node(MRCat(), name='mrcat')
        outputnode = pe.Node(IdentityInterface(fields=outputs),
                             name="concatenation_outputnode")
        workflow = pe.Workflow(name='concatenation')
        workflow.connect(inputnode, 'low_b_dw_scan', mrcat, 'first_scan')
        workflow.connect(inputnode, 'high_b_dw_scan', mrcat, 'second_scan')
        workflow.connect(mrcat, 'out_file', outputnode, 'dw_scan')
        return Pipeline(
            dataset=self, name='concatenation', workflow=workflow,
            inputs=inputs, outputs=outputs, inputnode=inputnode,
            outputnode=outputnode, description=(
                "Concatenate low and high b-value dMRI scans for NODDI "
                "processing"),
            citations=citations, options=options,
            requirements=['matlab', 'noddi'])

    acquired_components = acquired_components = {
        'low_b_dw_scan': 'mrtrix', 'high_b_dw_scan': 'mrtrix',
        'forward_rpe': 'mrtrix', 'reverse_rpe': 'mrtrix'}

    generated_components = dict(
        DiffusionDataset.generated_components.items() +
        [('dwi', (concatenate_pipeline, 'mrtrix'))])
