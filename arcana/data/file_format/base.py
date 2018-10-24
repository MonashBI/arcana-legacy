from builtins import object
from abc import ABCMeta, abstractmethod
from arcana.node import Node
from arcana.exception import (
    ArcanaUsageError, ArcanaFileFormatClashError, ArcanaNoConverterError,
    ArcanaFileFormatNotRegisteredError)
from nipype.interfaces.utility import IdentityInterface
import logging
from future.utils import with_metaclass


logger = logging.getLogger('arcana')


class FileFormat(object):
    """
    Defines a format for a fileset (e.g. DICOM, NIfTI, Matlab file)

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
    alternate_names : List[str]
        A list of alternate names to use to load the file format with
        (when the format is saved by format name, e.g. XNAT, instead
        of a file with an extension)
    array_loader : Function
        A function that takes the fileset path in the given format and
        returns a data array
    """

    # To hold registered data formats
    by_names = {}
    by_exts = {}
    by_within_exts = {}

    def __init__(self, name, extension=None, desc='',
                 directory=False, within_dir_exts=None,
                 converters=None, alternate_names=None,
                 array_loader=None, header_loader=None):
        if not name.islower():
            raise ArcanaUsageError(
                "All data format names must be lower case ('{}')"
                .format(name))
        if extension is None and not directory:
            raise ArcanaUsageError(
                "Extension for '{}' format can only be None if it is a "
                "directory".format(name))
        self._name = name
        self._extension = extension
        self._desc = desc
        self._directory = directory
        if within_dir_exts is not None:
            if not directory:
                raise ArcanaUsageError(
                    "'within_dir_exts' keyword arg is only valid "
                    "for directory data formats, not '{}'".format(name))
            within_dir_exts = frozenset(within_dir_exts)
        self._within_dir_exts = within_dir_exts
        self._converters = converters if converters is not None else {}
        self._alternate_names = (tuple(alternate_names)
                                 if alternate_names is not None else ())
        self._array_loader = array_loader
        self._header_loader = header_loader

    def __eq__(self, other):
        try:
            return (
                self._name == other._name and
                self._extension == other._extension and
                self._desc == other._desc and
                self._directory == other._directory and
                self._within_dir_exts ==
                other._within_dir_exts and
                self.alternate_names == other.alternate_names)
        except AttributeError:
            return False

    def __hash__(self):
        return (
            hash(self._name) ^
            hash(self._extension) ^
            hash(self._desc) ^
            hash(self._directory) ^
            hash(self._within_dir_exts) ^
            hash(self._alternate_names))

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return ("FileFormat(name='{}', extension='{}', directory={}{})"
                .format(self.name, self.extension, self.directory,
                        (', within_dir_extension={}'.format(
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
    def alternate_names(self):
        return self._alternate_names

    @property
    def within_dir_exts(self):
        return self._within_dir_exts

    @property
    def xnat_resource_names(self):
        "Lists acceptable XNAT resource names in order of preference"
        return (self.name.upper(),) + self.alternate_names

    def converter_from(self, file_format, processor=None):
        if file_format == self:
            return IdentityConverter(file_format, self)
        try:
            converter_cls = self._converters[file_format.name]
        except KeyError:
            raise ArcanaNoConverterError(
                "There is no converter to convert {} to {}, available:\n{}"
                .format(file_format, self,
                        '\n'.join(
                            '{} <- {}'.format(k, v)
                            for k, v in self._converters.items())))
        return converter_cls(file_format, self, processor=processor)

    @classmethod
    def register(cls, file_format):
        """
        Registers a data format so they can be recognised by extension
        and by resource type on XNAT

        Parameters
        ----------
        file_format : FileFormat
            The data format to register
        """
        try:
            saved_format = cls.by_names[file_format.name]
            if saved_format != file_format:
                raise ArcanaFileFormatClashError(
                    "Cannot register {} due to name clash with previously "
                    "registered {}".format(file_format, saved_format))
        except KeyError:
            if file_format.directory and file_format.extension is None:
                if file_format.within_dir_exts in cls.by_within_exts:
                    raise ArcanaFileFormatClashError(
                        "Cannot register {} due to within-directory "
                        "extension clash with previously registered {}"
                        .format(file_format,
                                cls.by_within_exts[
                                    file_format.within_dir_exts]))
            else:
                if file_format.extension in cls.by_exts:
                    raise ArcanaFileFormatClashError(
                        "Cannot register {} due to extension clash with "
                        "previously registered {}".format(
                            file_format,
                            cls.by_exts[file_format.ext]))
            for alt_name in file_format.alternate_names:
                if alt_name in cls.by_names:
                    raise ArcanaFileFormatClashError(
                        "Cannot register {} due to alternate name clash"
                        "('{}') with previously registered {}".format(
                            file_format, alt_name,
                            cls.by_names[alt_name]))
            cls.by_names[file_format.name] = file_format
            for alt_name in file_format.alternate_names:
                cls.by_names[alt_name] = file_format
            if file_format.ext is not None:
                cls.by_exts[file_format.ext] = file_format
            if file_format.within_dir_exts is not None:
                cls.by_within_exts[
                    file_format.within_dir_exts] = file_format

    @classmethod
    def by_name(cls, name):
        try:
            return cls.by_names[name.lower()]
        except KeyError:
            raise ArcanaFileFormatNotRegisteredError(
                "No data format named '{}' has been registered"
                .format(name,
                        ', '.format(repr(f)
                                    for f in list(cls.by_names.values()))))

    @classmethod
    def by_ext(cls, ext):
        try:
            return cls.by_exts[ext]
        except KeyError:
            raise ArcanaFileFormatNotRegisteredError(
                "No data format with extension '{}' has been registered"
                .format(
                    ext, ', '.format(repr(f)
                                     for f in list(cls.by_exts.values()))))

    @classmethod
    def by_within_dir_exts(cls, within_exts):
        try:
            return cls.by_within_exts[within_exts]
        except KeyError:
            raise ArcanaFileFormatNotRegisteredError(
                "No data format with within-directory extension '{}' "
                "has been registered ({})".format(
                    within_exts,
                    ', '.format(repr(f)
                                for f in list(cls.by_within_exts.values()))))

    def get_array(self, path):
        """
        Returns array data associated with the given path for the
        file format
        """
        if self._array_loader is None:
            raise ArcanaUsageError(
                "Cannot load array data for '{}' as '{}' file format "
                "doesn't have a registered array loader".format(
                    path, self.name))
        return self._array_loader(path)

    def get_header(self, path):
        """
        Returns header data associated with the given path for the
        file format
        """
        if self._header_loader is None:
            raise ArcanaUsageError(
                "Cannot load header for '{}' as '{}' file format "
                "doesn't have a registered header loader".format(
                    path, self.name))
        return self._header_loader(path)


class Converter(with_metaclass(ABCMeta, object)):
    """
    Base class for all Arcana data format converters

    Parameters
    ----------
    input_format : FileFormat
        The input format to convert from
    output_format : FileFormat
        The output format to convert to
    """

    def __init__(self, input_format, output_format, processor=None):
        self._input_format = input_format
        self._output_format = output_format
        self._processor = processor
        if processor is not None:
            processor.requirements_satisfiable(*self.requirements)

    def __eq__(self, other):
        return (self.input_format == self.input_format and
                self._output_format == other.output_format)

    @property
    def input_format(self):
        return self._input_format

    @property
    def output_format(self):
        return self._output_format

    @property
    def processor(self):
        return self._processor

    @abstractmethod
    def get_node(self, name):
        """
        Returns a NiPype node that converts a fileset from the input
        format to the output format

        Parameters
        ----------
        name : str
            Name for the node
        """

    def __repr__(self):
        return "{}(input_format={}, output_format={})".format(
            type(self).__name__, self.input_format, self.output_format)


class IdentityConverter(Converter):

    requirements = []

    def get_node(self, name, **kwargs):
        return Node(IdentityInterface(['i']), name=name, **kwargs), 'i', 'i'
