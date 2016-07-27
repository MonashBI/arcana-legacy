from nipype.pipeline import engine as pe
from nipype.interfaces import fsl
from .base import MRDataset
from nianalysis.requirements import Requirement
from nianalysis.citations import fsl_cite, bet_cite, bet2_cite
from nianalysis.scans import mrtrix_format, nifti_gz_format


class T2Dataset(MRDataset):

    pass
