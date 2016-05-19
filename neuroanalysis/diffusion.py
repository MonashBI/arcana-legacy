from nipype.pipeline import engine as pe
from .interfaces.mrtrix import DWIPreproc
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
        inputs = ('diffusion', 'forward_rpe', 'reverse_rpe')
        outputs = ('preprocessed',)
        options = {'phase_encode_direction': phase_encode_direction}
        inputnode = pe.Node(IdentityInterface(fields=inputs),
                            name="preprocess_inputnode")
        dwipreproc = pe.Node(DWIPreproc(), name='dwipreproc')
        dwipreproc.inputs.pe_dir = phase_encode_direction
        dwipreproc.inputs.out_filename = 'preprocessed.mif'
        outputnode = pe.Node(IdentityInterface(fields=inputs),
                             name="preprocess_outputnode")
        workflow = pe.Workflow(name='preprocess')
        workflow.connect(inputnode, 'diffusion', dwipreproc, 'in_file')
        workflow.connect(inputnode, 'forward_rpe', dwipreproc, 'in_file')
        workflow.connect(inputnode, 'reverse_rpe', dwipreproc, 'in_file')
        workflow.connect(dwipreproc, 'out_file', outputnode, 'preprocessed')
        return Pipeline(
            dataset=self, name='preprocess', workflow=workflow,
            inputs=inputs, outputs=outputs, inputnode=inputnode,
            outputnode=outputnode, description=(
                "Preprocesses dMRI datasets using distortion correction"),
            options=options)

    def fod_pipeline(self, **options):
        raise NotImplementedError

    # The list of dataset components that are acquired by the scanner
    acquired_components = {
        'diffusion': None, 'forward_rpe': None, 'reverse_rpe': None}

    generated_components = {
        'fod': (fod_pipeline, 'mrtrix'),
        'preprocessed': (preprocess_pipeline, 'nifti_gz')}
