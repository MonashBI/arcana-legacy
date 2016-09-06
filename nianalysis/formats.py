from copy import copy


class ScanFormat(object):

    def __init__(self, name, extension):
        self._name = name
        self._extension = extension

    def __repr__(self):
        return "ScanFormat(name='{}', extension='{}')".format(self.name,
                                                              self.extension)

    @property
    def name(self):
        return self._name

    @property
    def extension(self):
        return self._extension


nifti_format = ScanFormat(name='nifti', extension='.nii')
nifti_gz_format = ScanFormat(name='nifti_gz', extension='.nii.gz')
mrtrix_format = ScanFormat(name='mrtrix', extension='.mif')
analyze_format = ScanFormat(name='analyze', extension='.img')
dicom_format = ScanFormat(name='dicom', extension='')
fsl_bvecs_format = ScanFormat(name='fsl_bvecs', extension='.bvec')
fsl_bvals_format = ScanFormat(name='fsl_bvals', extension='.bval')
mrtrix_grad_format = ScanFormat(name='mrtrix_grad', extension='.b')
matlab_format = ScanFormat(name='matlab', extension='.mat')


# A dictionary to access all the formats by name
scan_formats = dict(
    (f.name, f) for f in copy(globals()).itervalues()
    if isinstance(f, ScanFormat))

scan_formats_by_ext = dict(
    (f.extension, f) for f in scan_formats.itervalues())
