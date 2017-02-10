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


class Converter(object):

    def __init__(self, name, input_formats, output_formats, interface,
                 in_field, out_field):
        self._name = name
        self._input_formats = input_formats
        self._output_formats = output_formats
        self._interface = interface
        self._in_field = in_field
        self._out_field = out_field

    @property
    def name(self):
        return self._name

    @property
    def input_formats(self):
        return self._input_formats

    @property
    def output_formats(self):
        return self._output_formats

    @property
    def interface(self):
        return self._interface

    @property
    def in_field(self):
        return self._in_field

    @property
    def out_field(self):
        return self._out_field


nifti_format = DataFormat(name='nifti', extension='.nii',
                          lctype='nifti/series', mrinfo='NIfTI-1.1')
nifti_gz_format = DataFormat(name='nifti_gz', extension='.nii.gz',
                                lctype='nifti/gz', mrinfo='NIfTI-1.1')
mrtrix_format = DataFormat(name='mrtrix', extension='.mif', mrinfo='MRtrix')
analyze_format = DataFormat(name='analyze', extension='.img')
dicom_format = DataFormat(name='dicom', extension=None, lctype='dicom/series',
                          mrinfo='DICOM')
fsl_bvecs_format = DataFormat(name='fsl_bvecs', extension='.bvec')
fsl_bvals_format = DataFormat(name='fsl_bvals', extension='.bval')
mrtrix_grad_format = DataFormat(name='mrtrix_grad', extension='.b')
matlab_format = DataFormat(name='matlab', extension='.mat')
freesurfer_recon_all_format = DataFormat(name='freesurfer_recon_all',
                                         extension='.fs.zip', converter=None)
zip_format = DataFormat(name='zip_format', extension='.zip', converter='unzip')
directory_format = DataFormat(name='directory_format', extension=None,
                              converter='unzip')
text_matrix_format = DataFormat(name='text_matrix', extension='.mat',
                                converter=None)


# A dictionary to access all the formats by name
data_formats = dict(
    (f.name, f) for f in copy(globals()).itervalues()
    if isinstance(f, DataFormat))


data_formats_by_ext = dict(
    (f.extension, f) for f in data_formats.itervalues())

data_formats_by_mrinfo = dict(
    (f.mrinfo, f) for f in data_formats.itervalues())


mrconvert = Converter('mrconvert',
                      [nifti_format, nifti_gz_format, mrtrix_format])

