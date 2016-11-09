from copy import copy


class DatasetFormat(object):

    def __init__(self, name, extension, lctype=None):
        self._name = name
        self._extension = extension
        self._lctype = lctype

    def __repr__(self):
        return "DatasetFormat(name='{}', extension='{}')".format(self.name,
                                                              self.extension)

    @property
    def name(self):
        return self._name

    @property
    def extension(self):
        return self._extension

    @property
    def lctype(self):
        return self._lctype


nifti_format = DatasetFormat(name='nifti', extension='.nii',
                          lctype='nifti/series')
nifti_gz_format = DatasetFormat(name='nifti_gz', extension='.nii.gz',
                             lctype='nifti/gz')
mrtrix_format = DatasetFormat(name='mrtrix', extension='.mif')
analyze_format = DatasetFormat(name='analyze', extension='.img')
dicom_format = DatasetFormat(name='dicom', extension='', lctype='dicom/series')
fsl_bvecs_format = DatasetFormat(name='fsl_bvecs', extension='.bvec')
fsl_bvals_format = DatasetFormat(name='fsl_bvals', extension='.bval')
mrtrix_grad_format = DatasetFormat(name='mrtrix_grad', extension='.b')
matlab_format = DatasetFormat(name='matlab', extension='.mat')


# A dictionary to access all the formats by name
dataset_formats = dict(
    (f.name, f) for f in copy(globals()).itervalues()
    if isinstance(f, DatasetFormat))

dataset_formats_by_ext = dict(
    (f.extension, f) for f in dataset_formats.itervalues())
