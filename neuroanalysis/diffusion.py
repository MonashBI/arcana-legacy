from nipype.pipeline import engine as pe
from .interfaces.mrtrix import DWIPreproc, MRCat
from .base import Dataset, Pipeline, Scan
from nipype.interfaces.utility import IdentityInterface


class DiffusionDataset(Dataset):

    def preprocess_pipeline(self, phase_encode_direction='AP'):
        """
        Parameters
        ----------
        phase_encode_direction : str{AP|LR|IS}
            The phase encode direction
        """
        inputs = ('dw_scan', 'forward_rpe', 'reverse_rpe')
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
        workflow.connect(inputnode, 'dw_scan', dwipreproc, 'in_file')
        workflow.connect(inputnode, 'forward_rpe', dwipreproc, 'forward_rpe')
        workflow.connect(inputnode, 'reverse_rpe', dwipreproc, 'reverse_rpe')
        workflow.connect(dwipreproc, 'out_file', outputnode, 'preprocessed')
        return Pipeline(
            dataset=self, name='preprocess', workflow=workflow,
            inputs=inputs, outputs=outputs, inputnode=inputnode,
            outputnode=outputnode, description=(
                "Preprocesses dMRI datasets using distortion correction"),
            citations=citations, options=options)

    def fod_pipeline(self):
        raise NotImplementedError

    # The list of dataset components that are acquired by the scanner
    acquired_components = {
        'dw_scan': 'mrtrix', 'forward_rpe': 'mrtrix', 'reverse_rpe': 'mrtrix'}

    generated_components = {
        'fod': (fod_pipeline, 'mrtrix'),
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
        outputs = ('dw_scan',)
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
            citations=citations, options=options)

    acquired_components = acquired_components = {
        'low_b_dw_scan': 'mrtrix', 'high_b_dw_scan': 'mrtrix',
        'forward_rpe': 'mrtrix', 'reverse_rpe': 'mrtrix'}

    generated_components = dict(
        DiffusionDataset.generated_components.items() +
        [('dw_scan', (concatenate_pipeline, 'mrtrix'))])
