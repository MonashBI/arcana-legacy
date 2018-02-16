from copy import copy
from abc import ABCMeta, abstractmethod
from nianalysis.nodes import Node
from nianalysis.interfaces.mrtrix import MRConvert
from nianalysis.interfaces.utils import (
    ZipDir, UnzipDir, TarGzDir, UnTarGzDir)
from nianalysis.exceptions import (
    NiAnalysisError, NiAnalysisRequirementVersionException,
    NiAnalysisModulesNotInstalledException)
from nianalysis.requirements import (
    mrtrix3_req, dcm2niix_req, mricrogl_req, Requirement)
from nianalysis.interfaces.converters import Dcm2niix
import logging


logger = logging.getLogger('NiAnalysis')


class DataFormat(object):

    def __init__(self, name, extension, lctype=None, description='',
                 mrinfo='None', directory=False):
        self._name = name
        self._extension = extension
        self._lctype = lctype
        self._description = description
        self._mrinfo = mrinfo
        self._directory = directory

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
    def description(self):
        return self._description

    @property
    def mrinfo(self):
        return self._mrinfo

    @property
    def directory(self):
        return self._directory

    @property
    def xnat_resource_name(self):
        return self.name.upper()


nifti_format = DataFormat(name='nifti', extension='.nii',
                          lctype='nifti/series', mrinfo='NIfTI-1.1')
nifti_gz_format = DataFormat(name='nifti_gz', extension='.nii.gz',
                             lctype='nifti/gz', mrinfo='NIfTI-1.1')
mrtrix_format = DataFormat(name='mrtrix', extension='.mif', mrinfo='MRtrix')
analyze_format = DataFormat(name='analyze', extension='.img')
dicom_format = DataFormat(name='dicom', extension=None, lctype='dicom/series',
                          mrinfo='DICOM', directory=True)
fsl_bvecs_format = DataFormat(name='fsl_bvecs', extension='.bvec')
fsl_bvals_format = DataFormat(name='fsl_bvals', extension='.bval')
mrtrix_grad_format = DataFormat(name='mrtrix_grad', extension='.b')
matlab_format = DataFormat(name='matlab', extension='.mat')
freesurfer_recon_all_format = DataFormat(name='freesurfer_recon_all',
                                         extension='.fs.zip')
zip_format = DataFormat(name='zip', extension='.zip')
directory_format = DataFormat(name='directory', extension=None,
                              directory=True)
text_matrix_format = DataFormat(name='text_matrix', extension='.mat')
text_format = DataFormat(name='text', extension='.txt')
rdata_format = DataFormat(name='rdata', extension='.rdata')
ica_format = DataFormat(name='ica', extension='.ica')
par_format = DataFormat(name='parameters', extension='.par')
gif_format = DataFormat(name='gif', extension='.gif')
targz_format = DataFormat(name='targz', extension='.tar.gz')
csv_format = DataFormat(name='comma-separated_file', extension='.csv')
png_format = DataFormat(name='portable-network-graphics',
                        extension='.png')


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

    @property
    def is_available(self):
        """
        Check to see if the required modules to run the conversion are
        available. Defaults to True if modules are not used on the system.
        """
        try:
            available_modules = Node.available_modules()
        except NiAnalysisModulesNotInstalledException:
            # Assume that it is installed but not as a module
            return True
        try:
            for possible_reqs in self.requirements:
                Requirement.best_requirement(possible_reqs, available_modules)
            return True
        except NiAnalysisRequirementVersionException:
            return False


class Dcm2niixConverter(Converter):

    requirements = [(dcm2niix_req, mricrogl_req)]

    def _get_convert_node(self, node_name, input_format, output_format):  # @UnusedVariable @IgnorePep8
        convert_node = Node(Dcm2niix(), name=node_name,
                            requirements=self.requirements, wall_time=20)
        convert_node.inputs.compression = 'y'
        return convert_node, 'input_dir', 'converted'

    def input_formats(self):
        return [dicom_format]

    def output_formats(self):
        return [nifti_format, nifti_gz_format]


class MrtrixConverter(Converter):

    requirements = [mrtrix3_req]

    def _get_convert_node(self, node_name, input_format, output_format):  # @UnusedVariable @IgnorePep8
        convert_node = Node(MRConvert(), name=node_name,
                            requirements=self.requirements)
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

    requirements = []

    def _get_convert_node(self, node_name, input_format, output_format):  # @UnusedVariable @IgnorePep8
        convert_node = Node(UnzipDir(), name=node_name,
                            memory=12000)
        return convert_node, 'zipped', 'unzipped'

    def input_formats(self):
        return [zip_format]

    def output_formats(self):
        return [directory_format]


class ZipConverter(Converter):

    requirements = []

    def _get_convert_node(self, node_name, input_format, output_format):  # @UnusedVariable @IgnorePep8
        convert_node = Node(ZipDir(), name=node_name,
                            memory=12000)
        return convert_node, 'dirname', 'zipped'

    def input_formats(self):
        return [directory_format]

    def output_formats(self):
        return [zip_format]


class TarGzConverter(Converter):

    requirements = []

    def _get_convert_node(self, node_name, input_format, output_format):  # @UnusedVariable @IgnorePep8
        convert_node = Node(TarGzDir(), name=node_name,
                            memory=12000)
        return convert_node, 'dirname', 'zipped'

    def input_formats(self):
        return [directory_format]

    def output_formats(self):
        return [targz_format]


class UnTarGzConverter(Converter):

    requirements = []

    def _get_convert_node(self, node_name, input_format, output_format):  # @UnusedVariable @IgnorePep8
        convert_node = Node(UnTarGzDir(), name=node_name,
                            memory=12000)
        return convert_node, 'gzipped', 'gunzipped'

    def input_formats(self):
        return [targz_format]

    def output_formats(self):
        return [directory_format]

# List all possible converters in order of preference
all_converters = [Dcm2niixConverter(), MrtrixConverter(), UnzipConverter(),
                  ZipConverter(), UnTarGzConverter(), TarGzConverter()]

# A dictionary to access all the formats by name
data_formats = dict(
    (f.name, f) for f in copy(globals()).itervalues()
    if isinstance(f, DataFormat))


data_formats_by_ext = dict(
    (f.extension, f) for f in data_formats.itervalues())

data_formats_by_mrinfo = dict(
    (f.mrinfo, f) for f in data_formats.itervalues())


def get_converter_node(dataset, dataset_name, output_format, source, workflow,
                       node_name, converters=None):
    if converters is None:
        converters = all_converters
    for converter in converters:
        if (dataset.format in converter.input_formats() and
            output_format in converter.output_formats() and
                converter.is_available):
            return converter.convert(workflow, source, dataset, dataset_name,
                                     node_name, output_format)
    raise NiAnalysisError(
        "No available converters to convert between '{}' and '{}' formats."
        .format(dataset.format.name, output_format.name))
