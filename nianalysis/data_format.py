from abc import ABCMeta, abstractmethod
from copy import copy
from nianalysis.node import Node
from nianalysis.interfaces.utils import (
    ZipDir, UnzipDir, TarGzDir, UnTarGzDir)
from nianalysis.exception import (
    NiAnalysisRequirementVersionException,
    NiAnalysisModulesNotInstalledException,
    NiAnalysisUsageError, NiAnalysisDataFormatClashError,
    NiAnalysisNoConverterError,
    NiAnalysisConverterNotAvailableError,
    NiAnalysisDataFormatNotRegisteredError)
from nianalysis.requirement import Requirement
import logging


logger = logging.getLogger('NiAnalysis')


class DataFormat(object):
    """
    Defines a format for a dataset (e.g. DICOM, NIfTI, Matlab file)

    Parameters
    ----------
    name : str
        A name for the data format
    extension : str
        The extension of the format
    desc : str
        A description of what the format is and ideally a link to its
        documentation
    directory : bool
        Whether the format is a directory or a file
    within_dir_exts : List[str]
        A list of extensions that are found within the top level of
        the directory (for directory formats). Used to identify
        formats from paths.
    converters : Dict[str, Converter]
        A dictionary mapping names of alternative data formats
        to Converter objects that can convert from the alternative
        format to this format.
    """

    # To hold registered data formats
    by_names = {}
    by_exts = {}
    by_within_exts = {}

    def __init__(self, name, extension=None, desc='',
                 directory=False, within_dir_exts=None,
                 converters=None):
        if not name.islower():
            raise NiAnalysisUsageError(
                "All data format names must be lower case ('{}')"
                .format(name))
        if extension is None and not directory:
            raise NiAnalysisUsageError(
                "Extension for '{}' format can only be None if it is a "
                "directory".format(name))
        self._name = name
        self._extension = extension
        self._desc = desc
        self._directory = directory
        if within_dir_exts is not None:
            if not directory:
                raise NiAnalysisUsageError(
                    "'within_dir_exts' keyword arg is only valid "
                    "for directory data formats, not '{}'".format(name))
            within_dir_exts = frozenset(within_dir_exts)
        self._within_dir_exts = within_dir_exts
        self._converters = converters if converters is not None else {}

    def __eq__(self, other):
        try:
            return (
                self._name == other._name and
                self._extension == other._extension and
                self._desc == other._desc and
                self._directory == other._directory and
                self._within_dir_exts ==
                other._within_dir_exts and
                self._converters == other._converters)
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return ("DataFormat(name='{}', extension='{}', directory={}{})"
                .format(self.name, self.extension, self.directory,
                        ('within_dir_extension={}'.format(
                            self.within_dir_exts)
                         if self.directory else '')))

    def __str__(self):
        return self.name

    @property
    def name(self):
        return self._name

    @property
    def extension(self):
        return self._extension

    @property
    def ext(self):
        return self._extension

    @property
    def ext_str(self):
        return self.extension if self.extension is not None else ''

    @property
    def desc(self):
        return self._desc

    @property
    def directory(self):
        return self._directory

    @property
    def within_dir_exts(self):
        return self._within_dir_exts

    @property
    def xnat_resource_name(self):
        return self.name.upper()

    def converter_from(self, data_format):
        try:
            converter_cls = self._converters[data_format.name]
        except KeyError:
            raise NiAnalysisNoConverterError(
                "There is no converter to convert {} to {}"
                .format(self, data_format))
        return converter_cls(data_format, self)

    @classmethod
    def register(cls, data_format):
        """
        Registers a data format so they can be recognised by extension
        and by resource type on XNAT

        Parameters
        ----------
        data_format : DataFormat
            The data format to register
        """
        try:
            saved_format = cls.by_names[data_format.name]
            if saved_format != data_format:
                raise NiAnalysisDataFormatClashError(
                    "Cannot register {} due to name clash with previously "
                    "registered {}".format(data_format, saved_format))
        except KeyError:
            if data_format.directory and data_format.extension is None:
                if data_format.within_dir_exts in cls.by_within_exts:
                    raise NiAnalysisDataFormatClashError(
                        "Cannot register {} due to within-directory "
                        "extension clash with previously registered {}"
                        .format(data_format,
                                cls.by_within_exts[
                                    data_format.within_dir_exts]))
            else:
                if data_format.extension in cls.by_exts:
                    raise NiAnalysisDataFormatClashError(
                        "Cannot register {} due to extension clash with "
                        "previously registered {}".format(
                            data_format,
                            cls.by_exts[data_format.ext]))
            cls.by_names[data_format.name] = data_format
            if data_format.ext is not None:
                cls.by_exts[data_format.ext] = data_format
            if data_format.within_dir_exts is not None:
                cls.by_within_exts[
                    data_format.within_dir_exts] = data_format

    @classmethod
    def by_name(cls, name):
        try:
            return cls.by_names[name.lower()]
        except KeyError:
            raise NiAnalysisDataFormatNotRegisteredError(
                "No data format named '{}' has been registered"
                .format(name,
                        ', '.format(repr(f)
                                    for f in cls.by_names.values())))

    @classmethod
    def by_ext(cls, ext):
        try:
            return cls.by_exts[ext]
        except KeyError:
            raise NiAnalysisDataFormatNotRegisteredError(
                "No data format with extension '{}' has been registered"
                .format(
                    ext, ', '.format(repr(f)
                                     for f in cls.by_exts.values())))

    @classmethod
    def by_within_dir_exts(cls, within_exts):
        try:
            return cls.by_within_exts[within_exts]
        except KeyError:
            raise NiAnalysisDataFormatNotRegisteredError(
                "No data format with within-directory extension '{}' "
                "has been registered ({})".format(
                    within_exts,
                    ', '.format(repr(f)
                                for f in cls.by_within_exts.values())))


class Converter(object):
    """
    Base class for all NiAnalysis data format converters

    Parameters
    ----------
    input_format : DataFormat
        The input format to convert from
    output_format : DataFormat
        The output format to convert to
    """

    __metaclass__ = ABCMeta

    def __init__(self, input_format, output_format):
        self._input_format = input_format
        self._output_format = output_format
        try:
            available_modules = Node.available_modules()
            for possible_reqs in self.requirements:
                Requirement.best_requirement(possible_reqs,
                                             available_modules)
        except NiAnalysisRequirementVersionException:
            raise NiAnalysisConverterNotAvailableError(
                "Module(s) required for converter {} ({}) are not "
                "available".format(
                    self,
                    ', '.join(r.name for r in self.requirements)))
        except NiAnalysisModulesNotInstalledException:
            pass

    @property
    def input_format(self):
        return self._input_format

    @property
    def output_format(self):
        return self._output_format

    @abstractmethod
    def get_node(self, name):
        """
        Returns a NiPype node that converts a dataset from the input
        format to the output format

        Parameters
        ----------
        name : str
            Name for the node
        """

    def __repr__(self):
        return "{}(input_format={}, output_format={})".format(
            type(self).__name__, self.input_format, self.output_format)


class UnzipConverter(Converter):

    requirements = []

    def get_node(self, name):
        convert_node = Node(UnzipDir(), name=name, memory=12000)
        return convert_node, 'zipped', 'unzipped'


class ZipConverter(Converter):

    requirements = []

    def get_node(self, name):
        convert_node = Node(ZipDir(), name=name, memory=12000)
        return convert_node, 'dirname', 'zipped'


class TarGzConverter(Converter):

    requirements = []

    def get_node(self, name):
        convert_node = Node(TarGzDir(), name=name, memory=12000)
        return convert_node, 'dirname', 'zipped'


class UnTarGzConverter(Converter):

    requirements = []

    def get_node(self, name):
        convert_node = Node(UnTarGzDir(), name=name, memory=12000)
        return convert_node, 'gzipped', 'gunzipped'


# General formats
directory_format = DataFormat(name='directory', extension=None,
                              directory=True,
                              converters={'zip': UnzipConverter,
                                          'targz': UnTarGzConverter})
text_format = DataFormat(name='text', extension='.txt')


# Compressed formats
zip_format = DataFormat(name='zip', extension='.zip',
                        converters={'directory': ZipConverter})
targz_format = DataFormat(name='targz', extension='.tar.gz',
                          converters={'direcctory': TarGzConverter})

# Register all data formats in module
for data_format in copy(globals()).itervalues():
    if isinstance(data_format, DataFormat):
        DataFormat.register(data_format)
