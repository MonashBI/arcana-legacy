import os.path
from abc import ABCMeta
from nianalysis.data_formats import DataFormat
from copy import copy
from nipype.interfaces.base import traits
import subprocess as sp
from nianalysis.data_formats import (
    data_formats, data_formats_by_ext, data_formats_by_mrinfo, dicom_format)
from nianalysis.utils import split_extension
from logging import getLogger
from nianalysis.exceptions import NiAnalysisError, NiAnalysisUsageError

logger = getLogger('NiAnalysis')


class BaseDatum(object):

    MULTIPLICITY_OPTIONS = ('per_session', 'per_subject', 'per_visit',
                            'per_project')

    __metaclass__ = ABCMeta

    def __init__(self, name, multiplicity='per_session'):  # @ReservedAssignment @IgnorePep8
        assert isinstance(name, basestring)
        assert multiplicity in self.MULTIPLICITY_OPTIONS
        self._name = name
        self._multiplicity = multiplicity

    def __eq__(self, other):
        try:
            return (self.name == other.name and
                    self.multiplicity == other.multiplicity)
        except AttributeError as e:
            assert not e.message.startswith(
                "'{}'".format(self.__class__.__name__))
            return False

    def find_mismatch(self, other, indent=''):
        if self != other:
            self != other
            mismatch = "\n{}{t}('{}') != {t}('{}')".format(
                indent, self.name, other.name,
                t=type(self).__name__)
        else:
            mismatch = ''
        sub_indent = indent + '  '
        if self.name != other.name:
            mismatch += ('\n{}name: self={} v other={}'
                         .format(sub_indent, self.name, other.name))
        if self.multiplicity != other.multiplicity:
            mismatch += ('\n{}multiplicity: self={} v other={}'
                         .format(sub_indent, self.multiplicity,
                                 other.multiplicity))
        return mismatch

    def __lt__(self, other):
        return self.name < other.name

    def __ne__(self, other):
        return not (self == other)

    def __iter__(self):
        return iter(self.to_tuple())

    @property
    def name(self):
        return self._name

    @property
    def multiplicity(self):
        return self._multiplicity

    def renamed(self, name):
        """
        Duplicate the datum and rename it
        """
        duplicate = copy(self)
        duplicate._name = name
        return duplicate

    def initkwargs(self):
        return {'name': self.name,
                'multiplicity': self.multiplicity}


class BaseDataset(BaseDatum):
    """
    An abstract base class representing either an acquired dataset or the
    specification for a processed dataset.

    Parameters
    ----------
    name : str
        The name of the dataset
    format : FileFormat
        The file format used to store the dataset. Can be one of the
        recognised formats
    multiplicity : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_project',
        specifying whether the dataset is present for each session, subject,
        visit or project.
    """

    __metaclass__ = ABCMeta

    def __init__(self, name, format=None, multiplicity='per_session'):  # @ReservedAssignment @IgnorePep8
        super(BaseDataset, self).__init__(name=name, multiplicity=multiplicity)
        assert format is None or isinstance(format, DataFormat)
        self._format = format

    def __eq__(self, other):
        return (super(BaseDataset, self).__eq__(other) and
                self._format == other._format)

    def find_mismatch(self, other, indent=''):
        mismatch = super(BaseDataset, self).find_mismatch(other, indent)
        sub_indent = indent + '  '
        if self.format != other.format:
            mismatch += ('\n{}format: self={} v other={}'
                         .format(sub_indent, self.format,
                                 other.format))
        return mismatch

    @property
    def format(self):
        return self._format

    def to_tuple(self):
        return (self.name, self.format.name, self.multiplicity, self.processed,
                self.is_spec)

    @classmethod
    def from_tuple(cls, tple):
        name, format_name, multiplicity, processed, is_spec = tple
        assert (is_spec and issubclass(DatasetSpec, cls) or
                not is_spec and issubclass(Dataset, cls))
        data_format = data_formats[format_name]
        return cls(name, data_format, pipeline=processed,
                   multiplicity=multiplicity)

    @property
    def filename(self, format=None):  # @ReservedAssignment
        if format is None:
            assert self.format is not None, "Dataset format is undefined"
            format = self.format  # @ReservedAssignment
        return self.name + format.extension

    def match(self, filename):
        base, ext = os.path.splitext(filename)
        return base == self.name and (ext == self.format.extension or
                                      self.format is None)

    def __repr__(self):
        return ("{}(name='{}', format={}, multiplicity={})"
                .format(self.__class__.__name__, self.name, self.format,
                        self.multiplicity))

    def initkwargs(self):
        dct = super(BaseDataset, self).initkwargs()
        dct['format'] = self.format
        return dct


class Dataset(BaseDataset):
    """
    A class representing a dataset, which was primary.

    Parameters
    ----------
    name : str
        The name of the dataset
    format : FileFormat
        The file format used to store the dataset. Can be one of the
        recognised formats
    multiplicity : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_project',
        specifying whether the dataset is present for each session, subject,
        visit or project.
    processed : bool
        Whether the scan was generated or acquired. Depending on the archive
        used to store the dataset this is used to determine the location of the
        dataset.
    location : str
        The directory that the dataset is stored in.
    """

    is_spec = False

    def __init__(self, name, format=None, processed=False,  # @ReservedAssignment @IgnorePep8
                 multiplicity='per_session', location=None):
        super(Dataset, self).__init__(name, format, multiplicity)
        self._processed = processed
        self._location = location
        if not processed and multiplicity != 'per_session':
            raise NiAnalysisUsageError(
                "Datasets with not multiplicity!='per_session' ({}) "
                "must have processed=True".format(self))

    @property
    def prefixed_name(self):
        return self.name

    def __eq__(self, other):
        return (super(Dataset, self).__eq__(other) and
                self.processed == other.processed)

    def find_mismatch(self, other, indent=''):
        mismatch = super(Dataset, self).find_mismatch(other, indent)
        sub_indent = indent + '  '
        if self.processed != other.processed:
            mismatch += ('\n{}processed: self={} v other={}'
                         .format(sub_indent, self.processed,
                                 other.processed))
        return mismatch

    @property
    def path(self):
        if self.location is None:
            raise NiAnalysisError(
                "Dataset '{}' location has not been set, please use "
                "'in_directory' method to set it".format(self.name))
        return os.path.join(self.location, self.name + self.filename)

    @property
    def processed(self):
        return self._processed

    def in_directory(self, dir_path):
        """
        Returns a copy of the dataset with its location set
        """
        cpy = copy(self)
        cpy._location = dir_path

    @classmethod
    def from_path(cls, path, multiplicity='per_session'):
        location = os.path.dirname(path)
        filename = os.path.basename(path)
        name, ext = split_extension(filename)
        processed = (multiplicity != 'per_session')  # required for checks in Dataset
        try:
            data_format = data_formats_by_ext[ext]
        except KeyError:
            # FIXME: Should handle DICOMs in different way. Maybe try to load
            #        with pydicom??
            cmd = ("mrinfo \"{}\" 2>/dev/null | grep Format | "
                   "awk '{{print $2}}'".format(path))
            abbrev = sp.check_output(cmd, shell=True).strip()
            try:
                data_format = data_formats_by_mrinfo[abbrev]
            except KeyError:
                logger.warning("Unrecognised format '{}' of path '{}'"
                               "assuming it is a dicom".format(abbrev, path))
                data_format = dicom_format
        return cls(name, data_format, multiplicity=multiplicity,
                   location=location, processed=processed)

    def initkwargs(self):
        dct = super(Dataset, self).initkwargs()
        dct['processed'] = self.processed
        return dct


class DatasetSpec(BaseDataset):
    """
    A class representing a "specification" for a dataset within a study, which
    can either be an "primary" dataset (e.g from the scanner)
    externally, or a "processed" dataset, which was generated by a processing
    pipeline.

    Parameters
    ----------
    name : str
        The name of the dataset
    format : FileFormat
        The file format used to store the dataset. Can be one of the
        recognised formats
    pipeline : Study.method
        The method of the study that is used to generate the dataset. If None
        the dataset is assumed to be primary external
    multiplicity : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_project',
        specifying whether the dataset is present for each session, subject,
        visit or project.
    description : str
        Description of what the field represents
    """

    is_spec = True

    def __init__(self, name, format=None, pipeline=None,  # @ReservedAssignment @IgnorePep8
                 multiplicity='per_session', description=None):
        super(DatasetSpec, self).__init__(name, format, multiplicity)
        self._pipeline = pipeline
        self._description = description
        self._prefix = ''

    def __eq__(self, other):
        return (super(DatasetSpec, self).__eq__(other) and
                self.pipeline == other.pipeline)

    def find_mismatch(self, other, indent=''):
        mismatch = super(DatasetSpec, self).find_mismatch(other, indent)
        sub_indent = indent + '  '
        if self.pipeline != other.pipeline:
            mismatch += ('\n{}pipeline: self={} v other={}'
                         .format(sub_indent, self.pipeline,
                                 other.pipeline))
        return mismatch

    @property
    def prefixed_name(self):
        return self._prefix + self.name

    @property
    def pipeline(self):
        return self._pipeline

    @property
    def processed(self):
        return self._pipeline is not None

    @property
    def description(self):
        return self._description

    @property
    def filename(self):
        return self._prefix + super(DatasetSpec, self).filename

    def apply_prefix(self, prefix):
        """
        Duplicate the dataset and provide a prefix to apply to the filename
        """
        duplicate = copy(self)
        duplicate._prefix = prefix
        return duplicate

    @classmethod
    def traits_spec(self):
        """
        Return the specification for a Dataset as a tuple
        """
        return traits.Tuple(  # @UndefinedVariable
            traits.Str(  # @UndefinedVariable
                mandatory=True,
                desc="name of file"),
            traits.Str(  # @UndefinedVariable
                mandatory=True,
                desc="name of the dataset format"),
            traits.Str(mandatory=True,  # @UndefinedVariable @IgnorePep8
                       desc="multiplicity of the dataset (one of '{}')".format(
                            "', '".join(self.MULTIPLICITY_OPTIONS))),
            traits.Bool(mandatory=True,  # @UndefinedVariable @IgnorePep8
                        desc=("whether the dataset is stored in the processed "
                              "dataset location")),
            traits.Bool(mandatory=True,  # @UndefinedVariable @IgnorePep8
                        desc=("whether the dataset was explicitly provided to "
                              "the study, or whether it is to be implicitly "
                              "generated")))

    def __repr__(self):
        return ("DatasetSpec(name='{}', format={}, pipeline={}, "
                "multiplicity={})".format(
                    self.name, self.format, self.pipeline, self.multiplicity))

    def initkwargs(self):
        dct = super(DatasetSpec, self).initkwargs()
        dct['pipeline'] = self.pipeline
        dct['description'] = self.description
        return dct


class BaseField(BaseDatum):
    """
    An abstract base class representing either an acquired value or the
    specification for a processed value.

    Parameters
    ----------
    name : str
        The name of the dataset
    dtype : type
        The datatype of the value. Can be one of (float, int, str)
    multiplicity : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_project',
        specifying whether the dataset is present for each session, subject,
        visit or project.
    """

    __metaclass__ = ABCMeta

    dtypes = (int, float, str)

    def __init__(self, name, dtype, multiplicity):
        super(BaseField, self).__init__(name, multiplicity)
        if dtype not in self.dtypes:
            raise NiAnalysisError(
                "Invalid dtype {}, can be one of {}".format(
                    dtype.__name__, ', '.join(self._dtype_names())))
        self._dtype = dtype

    def __eq__(self, other):
        return (super(BaseField, self).__eq__(other) and
                self.dtype == other.dtype)

    def find_mismatch(self, other, indent=''):
        mismatch = super(BaseField, self).find_mismatch(other, indent)
        sub_indent = indent + '  '
        if self.dtype != other.dtype:
            mismatch += ('\n{}dtype: self={} v other={}'
                         .format(sub_indent, self.dtype,
                                 other.dtype))
        return mismatch

    @property
    def dtype(self):
        return self._dtype

    def to_tuple(self):
        return (self.name, self.dtype, self.multiplicity,
                self.processed, self.is_spec)

    @classmethod
    def from_tuple(cls, tple):
        name, dtype, multiplicity, processed, is_spec = tple
        assert (is_spec and issubclass(FieldSpec, cls) or
                not is_spec and issubclass(Field, cls))
        if dtype not in cls.dtypes:
            raise NiAnalysisError(
                "Invalid dtype {}, can be one of {}".format(
                    dtype.__name__, ', '.join(cls._dtype_names())))
        return cls(name, dtype, pipeline=processed,
                   multiplicity=multiplicity)

    @classmethod
    def _dtype_names(cls):
        return (d.__name__ for d in cls.dtypes)

    def initkwargs(self):
        dct = super(BaseField, self).initkwargs()
        dct['dtype'] = self.dtype
        return dct


class Field(BaseField):
    """
    An abstract base class representing either an acquired value or the
    specification for a processed dataset.

    Parameters
    ----------
    name : str
        The name of the dataset
    dtype : type
        The datatype of the value. Can be one of (float, int, str)
    multiplicity : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_project',
        specifying whether the dataset is present for each session, subject,
        visit or project.
    processed : bool
        Whether or not the value belongs in the processed session or not
    """

    is_spec = False

    def __init__(self, name, dtype, multiplicity='per_session',
                 processed=False):
        super(Field, self).__init__(name, dtype, multiplicity)
        self._processed = processed
        if not processed and multiplicity != 'per_session':
            raise NiAnalysisUsageError(
                "Fields with not multiplicity!='per_session' ({}) "
                "must have processed=True".format(self))

    def __eq__(self, other):
        return (super(Field, self).__eq__(other) and
                self.processed == other.processed)

    def find_mismatch(self, other, indent=''):
        mismatch = super(Field, self).find_mismatch(other, indent)
        sub_indent = indent + '  '
        if self.processed != other.processed:
            mismatch += ('\n{}processed: self={} v other={}'
                         .format(sub_indent, self.processed,
                                 other.processed))
        return mismatch

    @property
    def processed(self):
        return self._processed

    def __repr__(self):
        return ("{}(name='{}', dtype={}, multiplicity={}, processed={})"
                .format(self.__class__.__name__, self.name, self.dtype,
                        self.multiplicity, self.processed))

    def initkwargs(self):
        dct = super(Field, self).initkwargs()
        dct['processed'] = self.processed
        return dct


class FieldSpec(BaseField):
    """
    An abstract base class representing either an acquired value or the
    specification for a processed dataset.

    Parameters
    ----------
    name : str
        The name of the dataset
    dtype : type
        The datatype of the value. Can be one of (float, int, str)
    pipeline : method
        Method that generates values for the specified field.
    multiplicity : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_project',
        specifying whether the dataset is present for each session, subject,
        visit or project.
    description : str
        Description of what the field represents
    """

    is_spec = True

    def __init__(self, name, dtype, pipeline=None,
                 multiplicity='per_session', description=None):
        super(FieldSpec, self).__init__(name, dtype, multiplicity)
        self._pipeline = pipeline
        self._description = description
        self._prefix = ''

    def __eq__(self, other):
        return (super(FieldSpec, self).__eq__(other) and
                self.pipeline == other.pipeline)

    def find_mismatch(self, other, indent=''):
        mismatch = super(FieldSpec, self).find_mismatch(other, indent)
        sub_indent = indent + '  '
        if self.pipeline != other.pipeline:
            mismatch += ('\n{}pipeline: self={} v other={}'
                         .format(sub_indent, self.pipeline,
                                 other.pipeline))
        return mismatch

    @property
    def prefixed_name(self):
        return self._prefix + self.name

    @property
    def dtype(self):
        return self._dtype

    @property
    def pipeline(self):
        return self._pipeline

    @property
    def processed(self):
        return self._pipeline is not None

    @property
    def description(self):
        return self._description

    def apply_prefix(self, prefix):
        """
        Duplicate the dataset and provide a prefix to apply to the filename
        """
        duplicate = copy(self)
        duplicate._prefix = prefix
        return duplicate

    @classmethod
    def traits_spec(self):
        """
        Return the specification for a Dataset as a tuple
        """
        return traits.Tuple(  # @UndefinedVariable
            traits.Str(  # @UndefinedVariable
                mandatory=True,
                desc="name of file"),
            traits.Any(  # Should really be of type type but not sure how
                mandatory=True,
                desc="The datatype of the field"),
            traits.Str(mandatory=True,  # @UndefinedVariable @IgnorePep8
                       desc="multiplicity of the dataset (one of '{}')".format(
                            "', '".join(self.MULTIPLICITY_OPTIONS))),
            traits.Bool(mandatory=True,  # @UndefinedVariable @IgnorePep8
                        desc=("whether the field is stored in the processed "
                              "dataset location")),
            traits.Bool(mandatory=True,  # @UndefinedVariable @IgnorePep8
                        desc=("whether the field was explicitly provided to "
                              "the study, or whether it is to be implicitly "
                              "generated")))

    def __repr__(self):
        return ("{}(name='{}', dtype={}, pipeline={}, "
                "multiplicity={})".format(
                    self.__class__.__name__, self.name, self.dtype,
                    self.pipeline, self.multiplicity))

    def initkwargs(self):
        dct = super(FieldSpec, self).initkwargs()
        dct['pipeline'] = self.pipeline
        dct['description'] = self.description
        return dct


class FieldValue(Field):

    def __init__(self, name, value, multiplicity='per_session',
                 processed=False):
        if isinstance(value, int):
            dtype = int
        elif isinstance(value, float):
            dtype = float
        elif isinstance(value, basestring):
            # Attempt to implicitly convert from string
            try:
                value = int(value)
                dtype = int
            except ValueError:
                try:
                    value = float(value)
                    dtype = float
                except ValueError:
                    dtype = str
        else:
            raise NiAnalysisError(
                "Unrecognised field dtype {}".format(value))
        self._value = value
        super(FieldValue, self).__init__(
            name, dtype, multiplicity=multiplicity,
            processed=processed)

    def __eq__(self, other):
        return (super(FieldValue, self).__eq__(other) and
                self.value == other.value)

    def find_mismatch(self, other, indent=''):
        mismatch = super(FieldValue, self).find_mismatch(other, indent)
        sub_indent = indent + '  '
        if self.value != other.value:
            mismatch += ('\n{}value: self={} v other={}'
                         .format(sub_indent, self.value,
                                 other.value))
        return mismatch

    @property
    def value(self):
        return self._value

    def __repr__(self):
        return ("{}(name='{}', value={}, dtype={}, multiplicity={}, "
                "processed={})"
                .format(self.__class__.__name__, self.name,
                        self.value, self.dtype, self.multiplicity,
                        self.processed))
