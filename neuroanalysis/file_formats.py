
class FileFormat(object):

    def __init__(self, name, extension):
        self._name = name
        self._extension = extension

    def __repr__(self):
        return "FileFormat(name='{}')".format(self.name)

    @property
    def name(self):
        return self._name

    @property
    def extension(self):
        return self._extension


nifti_format = FileFormat(name='nifti', extension='nii')

nifti_gz_format = FileFormat(name='nifti_gz', extension='nii.gz')

mrtrix_format = FileFormat(name='mrtrix', extension='mif')

analyze_format = FileFormat(name='analyze', extension='img')

dicom_format = FileFormat(name='dicom', extension='')

fsl_bvecs_format = FileFormat(name='fsl_bvecs', extension='bvec')

fsl_bvals_format = FileFormat(name='fsl_bvals', extension='bval')

mrtrix_grad_format = FileFormat(name='mrtrix_grad', extension='b')

matlab_format = FileFormat(name='matlab', extension='mat')
