from nipype.pipeline import engine as pe
from .interfaces.mrtrix import DWIPreproc
from .base import Dataset, Pipeline


class DiffusionDataset(Dataset):

    def __init__(self, project_id, archive, scan_names):
        super(DiffusionDataset, self).__init__(
            project_id, archive, scan_names=scan_names)

    def preprocess_pipeline(self):
        inputs = ('diffusion.mif', 'distortion_correct.mif')
        outputs = ('preprocessed.mif',)
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
    acquired_components = {'diffusion.mif', 'distortion_correct.mif'}

    generated_components = {
        'fod.mif': fod_pipeline,
        'preprocessed.mif': preprocess_pipeline}
