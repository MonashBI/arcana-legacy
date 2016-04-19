from nipype.interfaces import utility as util
from nipype.pipeline import engine as pe
from nipype.interfaces import fsl as fsl
from nipype.interfaces import mrtrix as mrtrix
from .interfaces.mrtrix import ExtractMRtrixGradients
from .base import Dataset


class DiffusionDataset(Dataset):

    def __init__(self, project, path, scratch, diffusion='Diffusion',
                 gradients='gradients',
                 distortion_correct='Diffusion_DISTCORR', source='daris'):
        super(DiffusionDataset, self).__init__(
            project, scan_names={
                'diffusion': diffusion, 'gradients': gradients,
                'distortion_correct': distortion_correct}, path=path,
            scratch=scratch, source=source)

    @property
    def preprocess_pipeline(self):
        pass
