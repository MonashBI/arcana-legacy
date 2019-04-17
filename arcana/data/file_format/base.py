from builtins import object
from collections import defaultdict
from past.builtins import basestring
from arcana.exceptions import (
    ArcanaUsageError, ArcanaFileFormatClashError, ArcanaNoConverterError,
    ArcanaFileFormatNotRegisteredError, ArcanaFileFormatError)
from nipype.interfaces.utility import IdentityInterface
from arcana.utils import lower, split_extension
import logging


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
    aux_files : dict[str, str]
        A dictionary of side cars (e.g. header or NIfTI json side cars) aside
        from the primary file, along with their expected extension.
        Automatically they will be assumed to be located adjancent to the
        primary file, with the same base name and this extension. However, in
        the initialisation of the fileset, alternate locations can be specified
    alternate_names : List[str]
        A list of alternate names to use to load the file format with
        (when the format is saved by format name, e.g. XNAT, instead
        of a file with an extension)
    """

    # To hold registered data formats
    by_names = {}
    by_exts = {}
    by_within_exts = {}

    def __init__(self, name, extension=None, desc='',
                 directory=False, within_dir_exts=None,
                 converters=None, aux_files=None, alternate_names=None):
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
        self._aux_files = aux_files if aux_files is not None else {}
        for sc_name, sc_ext in self.aux_files.items():
            if sc_ext == self.ext:
                raise ArcanaUsageError(
                    "Extension for side car '{}' cannot be the same as the "
                    "primary file ('{}')".format(sc_name, sc_ext))

    def __eq__(self, other):
        try:
            return (
                self._name == other._name and
                self._extension == other._extension and
                self._desc == other._desc and
                self._directory == other._directory and
                self._within_dir_exts ==
                other._within_dir_exts and
                self.alternate_names == other.alternate_names and
                self.aux_files == other.aux_files)
        except AttributeError:
            return False

    def __hash__(self):
        return (
            hash(self._name) ^
            hash(self._extension) ^
            hash(self._desc) ^
            hash(self._directory) ^
            hash(self._within_dir_exts) ^
            hash(self._alternate_names) ^
            hash(tuple(sorted(self.aux_files.items()))))

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
    def extensions(self):
        return tuple([self._extension] + sorted(self.aux_file_exts))

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
    def aux_files(self):
        return self._aux_files

    def default_aux_file_paths(self, primary_path):
        """
        Get the default paths for auxiliary files relative to the path of the
        primary file, i.e. the same name as the primary path with a different
        extension

        Parameters
        ----------
        primary_path : str
            Path to the primary file in the fileset

        Returns
        -------
        aux_paths : dict[str, str]
            A dictionary of auxiliary file names and default paths
        """
        return dict((n, primary_path[:-len(self.ext)] + ext)
                    for n, ext in self.aux_files.items())

    @property
    def aux_file_exts(self):
        return frozenset(self._aux_files.values())

    @property
    def within_dir_exts(self):
        return self._within_dir_exts

    @property
    def xnat_resource_names(self):
        "Lists acceptable XNAT resource names in order of preference"
        return (self.name.upper(),) + self.alternate_names

    def converter_from(self, file_format, **kwargs):
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
        return converter_cls(file_format, self, **kwargs)

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
                if file_format.extensions in cls.by_exts:
                    raise ArcanaFileFormatClashError(
                        "Cannot register {} due to extension(s) clash with "
                        "previously registered {}".format(
                            file_format,
                            cls.by_exts[file_format.extensions]))
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
            if file_format.extension is not None:
                cls.by_exts[file_format.extensions] = file_format
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
        # Convert to tuple
        if isinstance(ext, basestring):
            ext = (ext,)
        elif not isinstance(ext, tuple):
            ext = tuple(ext)
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

    def select_primary_and_aux_files(self, candidates):
        """
        Selects primary and auxiliary files that match the format by their file
        extensionsfrom a list of candidate file paths

        Parameters
        ----------
        candidates : list[str]
            The list of filenames to select from

        Returns
        -------
        primary_file : str
            Path to the selected primary file
        aux_files : dict[str, str]
            A dictionary mapping the auxiliary file name to the selected path
        """
        by_ext = defaultdict(list)
        for path in candidates:
            by_ext[split_extension(path)[1].lower()] = path
        try:
            primary_file = by_ext[self.ext]
        except KeyError:
            raise ArcanaFileFormatError(
                "No files match primary file extension of {} out of "
                "potential candidates of {}"
                .format(self, "', '".join(candidates)))
        if len(primary_file) > 1:
            raise ArcanaFileFormatError(
                "Multiple potential files for '{}' primary file of {}"
                .format("', '".join(primary_file), self))
        aux_files = {}
        for aux_name, aux_ext in self.aux_files.items():
            try:
                aux = by_ext[aux_ext]
            except KeyError:
                raise ArcanaFileFormatError(
                    "No files match auxiliary file extension '{}' of {} out of"
                    " potential candidates of {}"
                    .format(aux_ext, self, "', '".join(candidates)))
            if len(aux) > 1:
                raise ArcanaFileFormatError(
                    "Multiple potential files for '{}' auxiliary file ext. "
                    "({}) of {}".format("', '".join(aux),
                                        self))
            aux_files[aux_name] = aux[0]
        return primary_file, aux_files


class Converter(object):
    """
    Base class for all Arcana data format converters

    Parameters
    ----------
    input_format : FileFormat
        The input format to convert from
    output_format : FileFormat
        The output format to convert to
    """

    requirements = []

    def __init__(self, input_format, output_format, wall_time=None,
                 mem_gb=None):
        self._input_format = input_format
        self._output_format = output_format
        self._wall_time = wall_time
        self._mem_gb = mem_gb

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
    def interface(self):
        # To be overridden by subclasses
        return NotImplementedError

    @property
    def input(self):
        # To be overridden by subclasses
        return NotImplementedError

    @property
    def output(self):
        # To be overridden by subclasses
        return NotImplementedError

    @property
    def mem_gb(self):
        return self._mem_gb

    @property
    def wall_time(self):
        return self._wall_time

    def __repr__(self):
        return "{}(input_format={}, output_format={})".format(
            type(self).__name__, self.input_format, self.output_format)


class IdentityConverter(Converter):

    requirements = []
    interface = IdentityInterface(['i'])
    input = 'i'
    output = 'i'
