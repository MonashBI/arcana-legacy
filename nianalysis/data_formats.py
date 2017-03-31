from copy import copy
from abc import ABCMeta, abstractmethod
from nianalysis.nodes import Node
from nianalysis.interfaces.mrtrix import MRConvert
from nianalysis.interfaces.utils import ZipDir, UnzipDir
from nianalysis.exceptions import NiAnalysisError
from nianalysis.requirements import mrtrix3_req


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
dicom_format = DataFormat(name='dicom', extension=None, lctype='dicom/series',
                          mrinfo='DICOM')
fsl_bvecs_format = DataFormat(name='fsl_bvecs', extension='.bvec')
fsl_bvals_format = DataFormat(name='fsl_bvals', extension='.bval')
mrtrix_grad_format = DataFormat(name='mrtrix_grad', extension='.b')
matlab_format = DataFormat(name='matlab', extension='.mat')
freesurfer_recon_all_format = DataFormat(name='freesurfer_recon_all',
                                         extension='.fs.zip', converter=None)
zip_format = DataFormat(name='zip', extension='.zip', converter='unzip')
directory_format = DataFormat(name='directory', extension=None,
                              converter='unzip')
text_matrix_format = DataFormat(name='text_matrix', extension='.mat',
                                converter=None)
rdata_format = DataFormat(name='rdata', extension='.RData', converter=None)


class Converter(object):
    """
    Base class for all NiAnalysis data format converters
    """

    __metaclass__ = ABCMeta

    def convert(self, workflow, source, dataset, dataset_name, node_name,
                 output_format):
        """
        Inserts a format converter node into a complete workflow.

        Parameters
        ----------
        workflow : nipype.Workflow
            The workflow to add the converter node to
        source : nipype.Node
            The source node to draw the data from
        dataset : Dataset
            The dataset to convert
        node_name : str
            (Unique) name for the conversion node
        output_format : DataFormat
            The format to convert the node to
        """
        convert_node, in_field, out_field = self._get_convert_node(
            node_name, dataset.format, output_format)
        try:
            workflow.connect(
                source, dataset_name, convert_node, in_field)
        except:
            raise
        return convert_node, out_field

    @abstractmethod
    def _get_convert_node(self):
        pass

    @abstractmethod
    def input_formats(self):
        "Lists all data formats that the converter tool can read"
        pass

    @abstractmethod
    def output_formats(self):
        "Lists all data formats that the converter tool can write"
        pass


class MrtrixConverter(Converter):

    def _get_convert_node(self, node_name, input_format, output_format):  # @UnusedVariable @IgnorePep8
        convert_node = Node(MRConvert(), name=node_name,
                            required_modules=[mrtrix3_req])
        convert_node.inputs.out_ext = output_format.extension
        convert_node.inputs.quiet = True
        return convert_node, 'in_file', 'out_file'

    def input_formats(self):
        return [nifti_format, nifti_gz_format, mrtrix_format,
                analyze_format, dicom_format]

    def output_formats(self):
        return [nifti_format, nifti_gz_format, analyze_format,
                mrtrix_format]


class UnzipConverter(Converter):

    def _get_convert_node(self, node_name, input_format, output_format):  # @UnusedVariable @IgnorePep8
        convert_node = Node(UnzipDir(), name=node_name)
        return convert_node, 'zipped', 'unzipped'

    def input_formats(self):
        return [zip_format]

    def output_formats(self):
        return [directory_format]


class ZipConverter(Converter):

    def _get_convert_node(self, node_name, input_format, output_format):  # @UnusedVariable @IgnorePep8
        convert_node = Node(ZipDir(), name=node_name)
        return convert_node, 'dirname', 'zipped'

    def input_formats(self):
        return [directory_format]

    def output_formats(self):
        return [zip_format]

# List all possible converters in order of preference
converters = [MrtrixConverter(), UnzipConverter(), ZipConverter()]

# A dictionary to access all the formats by name
data_formats = dict(
    (f.name, f) for f in copy(globals()).itervalues()
    if isinstance(f, DataFormat))


data_formats_by_ext = dict(
    (f.extension, f) for f in data_formats.itervalues())

data_formats_by_mrinfo = dict(
    (f.mrinfo, f) for f in data_formats.itervalues())


def get_converter_node(dataset, dataset_name, output_format, source, workflow,
                       node_name):
    for converter in converters:
        if (dataset.format in converter.input_formats() and
                output_format in converter.output_formats()):
            return converter.convert(workflow, source, dataset, dataset_name,
                                     node_name, output_format)
    raise NiAnalysisError(
        "No available converters to convert between '{}' and '{}' formats."
        .format(dataset.format.name, output_format.name))
