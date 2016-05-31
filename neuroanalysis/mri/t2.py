from nipype.pipeline import engine as pe
from nipype.interfaces import fsl
from .base import MRDataset
from neuroanalysis.requirements import Requirement
from neuroanalysis.citations import fsl_cite, bet_cite, bet2_cite
from neuroanalysis.file_formats import mrtrix_format, nifti_gz_format


class T2Dataset(MRDataset):

    pass
