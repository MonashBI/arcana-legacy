from nipype.pipeline import engine as pe
from .interfaces.mrtrix import DWIPreproc
from .base import Dataset, Pipeline, Scan


class DiffusionDataset(Dataset):

    def preprocess_pipeline(self):
        inputs = ('diffusion', 'forward_rpe', 'reverse_rpe')
        outputs = ('preprocessed',)
        options = {}
        dwipreproc = pe.Node(DWIPreproc(), name='dwipreproc')
        return Pipeline(
            dataset=self, name='preprocess', workflow=dwipreproc,
            inputs=inputs, outputs=outputs, description=(
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
