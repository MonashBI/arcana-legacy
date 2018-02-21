from .version_ import __version__
import os
# Should be set explicitly in all FSL interfaces, but this squashes the warning
os.environ['FSLOUTPUTTYPE'] = 'NIFTI_GZ'
