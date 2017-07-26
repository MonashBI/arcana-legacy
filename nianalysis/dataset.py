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
from nianalysis.exceptions import NiAnalysisError

logger = getLogger('NiAnalysis')


class BaseDataset(object):
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

    MULTIPLICITY_OPTIONS = ('per_session', 'per_subject', 'per_visit',
                            'per_project')

    __metaclass__ = ABCMeta

    def __init__(self, name, format=None, multiplicity='per_session'):  # @ReservedAssignment @IgnorePep8
        assert isinstance(name, basestring)
        assert format is None or isinstance(format, DataFormat)
        assert multiplicity in self.MULTIPLICITY_OPTIONS
        self._name = name
        self._format = format
        self._multiplicity = multiplicity

    def __eq__(self, other):
        try:
            return (self.name == other.name and
                    self.format == other.format and
                    self.multiplicity == other.multiplicity)
        except AttributeError as e:
            assert not e.message.startswith(
                "'{}'".format(self.__class__.__name__))
            return False

    def __ne__(self, other):
        return not (self == other)

    @property
    def name(self):
        return self._name

    @property
    def format(self):
        return self._format

    @property
    def multiplicity(self):
        return self._multiplicity

    def __iter__(self):
        return iter(self.to_tuple())

    def to_tuple(self):
        return self.name, self.format.name, self.multiplicity, self.processed

    @classmethod
    def from_tuple(cls, tple):
        name, format_name, multiplicity, processed = tple
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
        Whether the scan was generated or acquired
    """

    def __init__(self, name, format=None, processed=False,  # @ReservedAssignment @IgnorePep8
                 multiplicity='per_session', location=None):
        super(Dataset, self).__init__(name, format, multiplicity)
        self._processed = processed
        self._location = location

    @property
    def prefixed_name(self):
        return self.name

    def __eq__(self, other):
        return (super(Dataset, self).__eq__(other) and
                self.processed == other.processed)

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
                   location=location)


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
    """

    def __init__(self, name, format=None, pipeline=None,  # @ReservedAssignment @IgnorePep8
                 multiplicity='per_session', description=None):
        super(DatasetSpec, self).__init__(name, format, multiplicity)
        self._pipeline = pipeline
        self._description = description
        self._prefix = ''

    def __eq__(self, other):
        return (super(DatasetSpec, self).__eq__(other) and
                self.pipeline == other.pipeline)

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

    def to_tuple(self):
        return self.name, self.format.name, self.multiplicity, self.processed

    def renamed_copy(self, name):
        cpy = copy(self)
        cpy._name = name
        return cpy

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
                        desc=("whether the file was generate by a pipeline "
                              "or not")))

    def __repr__(self):
        return ("DatasetSpec(name='{}', format={}, pipeline={}, "
                "multiplicity={})".format(
                    self.name, self.format, self.pipeline, self.multiplicity))
