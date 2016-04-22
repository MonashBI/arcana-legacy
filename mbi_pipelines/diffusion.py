from nipype.interfaces import utility as util
from nipype.pipeline import engine as pe
from nipype.interfaces import fsl as fsl
from nipype.interfaces import mrtrix as mrtrix
from .interfaces.mrtrix import ExtractMRtrixGradients
from .base import Dataset


class DiffusionDataset(Dataset):

    def __init__(self, project_id, archive,
                 gradients='gradients', diffusion='Diffusion',
                 distortion_correct='Diffusion_DISTCORR'):
        super(DiffusionDataset, self).__init__(
            project_id, archive, scan_names={
                'diffusion': diffusion, 'gradients': gradients,
                'distortion_correct': distortion_correct})

    def preprocess_pipeline(self, **kwargs):
        raise NotImplementedError

    def fod_pipeline(self, **kwargs):
        raise NotImplementedError

    # The list of dataset components that are acquired by the scanner
    acquired_components = ('diffusion', 'distortion_correct', 'gradients')

    generated_components = {
        'fod': fod_pipeline}
