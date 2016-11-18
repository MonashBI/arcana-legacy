from copy import copy


class DataFormat(object):

    def __init__(self, name, extension, lctype=None, converter='mrconvert',
                 description='', mrinfo='None'):
        self._name = name
        self._extension = extension
        self._lctype = lctype
        self._converter = converter
        self._description = description
        self._mrinfo = mrinfo

    def __repr__(self):
        return ("DataFormat(name='{}', extension='{}')"
                .format(self.name, self.extension))

    def __str__(self):
        return self.name

    @property
    def name(self):
        return self._name

    @property
    def extension(self):
        return self._extension

    @property
    def lctype(self):
        return self._lctype

    @property
    def converter(self):
        return self._converter

    @property
    def description(self):
        return self._description

    @property
    def mrinfo(self):
        return self._mrinfo


nifti_format = DataFormat(name='nifti', extension='.nii',
                          lctype='nifti/series', mrinfo='NIfTI-1.1')
nifti_gz_format = DataFormat(name='nifti_gz', extension='.nii.gz',
                                lctype='nifti/gz', mrinfo='NIfTI-1.1')
mrtrix_format = DataFormat(name='mrtrix', extension='.mif', mrinfo='MRtrix')
analyze_format = DataFormat(name='analyze', extension='.img')
dicom_format = DataFormat(name='dicom', extension='', lctype='dicom/series', mrinfo='DICOM')
fsl_bvecs_format = DataFormat(name='fsl_bvecs', extension='.bvec')
fsl_bvals_format = DataFormat(name='fsl_bvals', extension='.bval')
mrtrix_grad_format = DataFormat(name='mrtrix_grad', extension='.b')
matlab_format = DataFormat(name='matlab', extension='.mat')
freesurfer_format = DataFormat(name='freesurfer', extension='',
                               converter=None)


# A dictionary to access all the formats by name
data_formats = dict(
    (f.name, f) for f in copy(globals()).itervalues()
    if isinstance(f, DataFormat))


data_formats_by_ext = dict(
    (f.extension, f) for f in data_formats.itervalues())

data_formats_by_mrinfo = dict(
    (f.mrinfo, f) for f in data_formats.itervalues())