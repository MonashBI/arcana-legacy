from copy import copy
from arcana.data_format import DataFormat
from arcana.exception import (
    ArcanaError, ArcanaUsageError,
    ArcanaOutputNotProducedException,
    ArcanaMissingDataException)
from .base import BaseDataset, BaseField


class BaseSpec(object):

    derived = True

    def __init__(self, name, pipeline_name, desc=None):
        if not isinstance(pipeline_name, basestring):
            raise ArcanaUsageError(
                "Pipeline name for {} '{}' is not a string "
                "'{}'".format(name, pipeline_name))
        self._pipeline_name = pipeline_name
        self._desc = desc
        self._study = None

    def __eq__(self, other):
        return (self._pipeline_name == other._pipeline_name and
                self.desc == other.desc and
                self._study == other._study)

    def bind(self, study):
        """
        Returns a copy of the DatasetSpec bound to the given study

        Parameters
        ----------
        study : Study
            A study to bind the dataset spec to (should happen in the
            study constructor)
        """
        cpy = copy(self)
        cpy._study = study
        cpy.pipeline  # Test to see if pipeline name is present
        return cpy

    def find_mismatch(self, other, indent=''):
        mismatch = ''
        sub_indent = indent + '  '
        if self.pipeline_name != other.pipeline_name:
            mismatch += ('\n{}pipeline_name: self={} v other={}'
                         .format(sub_indent, self.pipeline_name,
                                 other.pipeline_name))
        if self.desc != other.desc:
            mismatch += ('\n{}desc: self={} v other={}'
                         .format(sub_indent, self.desc,
                                 other.desc))
        return mismatch

    @property
    def derivable(self):
        """
        Whether the spec (only valid for derived specs) can be derived
        given the inputs and options provided to the study
        """
        pipeline = self.pipeline()
        if self.name not in (o.name for o in pipeline.outputs):
            return False
        # Check all study inputs required by the pipeline were provided
        try:
            for inpt in pipeline.all_inputs:
                self.study.bound_data_spec(inpt.name)
        except (ArcanaOutputNotProducedException,
                ArcanaMissingDataException):
            return False
        return True

    @property
    def prefixed_name(self):
        return self.study.prefix + self.name

    @property
    def pipeline_name(self):
        return self._pipeline_name

    @property
    def pipeline(self):
        try:
            return getattr(self.study, self.pipeline_name)
        except AttributeError:
            raise ArcanaError(
                "There is no pipeline method named '{}' in present in "
                "'{}' study".format(self.pipeline_name, self.study))

    @property
    def study(self):
        if self._study is None:
            raise ArcanaError(
                "{} is not bound to a study".format(self))
        return self._study

    def apply_prefix(self, prefix):
        """
        Duplicate the dataset and provide a prefix to apply to the filename
        """
        duplicate = copy(self)
        duplicate._prefix = prefix
        return duplicate

    @property
    def desc(self):
        return self._desc

    def basename(self, **kwargs):  # @UnusedVariable
        return self.prefixed_name

    def initkwargs(self):
        dct = {}
        dct['pipeline_name'] = self.pipeline_name
        dct['desc'] = self.desc
        return dct


class DatasetSpec(BaseDataset, BaseSpec):
    """
    A specification for a dataset within a study, which
    can either be an "acquired" dataset (e.g from the scanner)
    externally, or a "generated" dataset, derived from a processing
    pipeline.

    Parameters
    ----------
    name : str
        The name of the dataset
    format : FileFormat
        The file format used to store the dataset. Can be one of the
        recognised formats
    pipeline_name : str
        Name of the method in the study that is used to generate the
        dataset. If None the dataset is assumed to be acq
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_project',
        specifying whether the dataset is present for each session, subject,
        visit or project.
    desc : str
        Description of what the field represents
    """

    is_spec = True

    def __init__(self, name, format, pipeline_name,  # @ReservedAssignment @IgnorePep8
                 frequency='per_session', desc=None, optional=False):
        BaseDataset.__init__(self, name, format, frequency)
        BaseSpec.__init__(self, name, pipeline_name, desc,
                          optional=optional)

    def __eq__(self, other):
        return (BaseDataset.__eq__(self, other) and
                BaseSpec.__eq__(self, other))

    def __repr__(self):
        return ("DatasetSpec(name='{}', format={}, pipeline_name={}, "
                "frequency={})".format(
                    self.name, self.format, self.pipeline_name,
                    self.frequency))

    def find_mismatch(self, other, indent=''):
        mismatch = BaseDataset.find_mismatch(self, other, indent)
        mismatch += BaseSpec.find_mismatch(self, other, indent)
        return mismatch

    def initkwargs(self):
        dct = BaseDataset.initkwargs(self)
        dct.update(BaseSpec.initkwargs(self))
        return dct


class FieldSpec(BaseField, BaseSpec):
    """
    An abstract base class representing either an acquired value or the
    specification for a derived dataset.

    Parameters
    ----------
    name : str
        The name of the dataset
    dtype : type
        The datatype of the value. Can be one of (float, int, str)
    pipeline_name : str
        Name of the method that generates values for the specified field.
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_project',
        specifying whether the dataset is present for each session, subject,
        visit or project.
    desc : str
        Description of what the field represents
    """

    is_spec = True

    def __init__(self, name, dtype, pipeline_name,
                 frequency='per_session', desc=None):
        BaseField.__init__(self, name, dtype, frequency)
        BaseSpec.__init__(self, name, pipeline_name, desc)

    def __eq__(self, other):
        return (BaseField.__eq__(self, other) and
                BaseSpec.__eq__(self, other))

    def find_mismatch(self, other, indent=''):
        mismatch = BaseField.find_mismatch(self, other, indent)
        mismatch += BaseSpec.find_mismatch(self, other, indent)
        return mismatch

    def __repr__(self):
        return ("{}(name='{}', dtype={}, pipeline_name={}, "
                "frequency={})".format(
                    self.__class__.__name__, self.name, self.dtype,
                    self.pipeline_name, self.frequency))

    def initkwargs(self):
        dct = BaseField.initkwargs(self)
        dct.update(BaseSpec.initkwargs(self))
        return dct


class BaseAcquiredSpec(object):

    derived = False

    def __init__(self, desc=None, optional=False):
        self._desc = desc
        self._optional = optional

    def __eq__(self, other):
        return (self.desc == other.desc and
                self.optional == other.optional)

    def bind(self, study):  # @UnusedVariable
        """
        Returns a copy of the DatasetSpec bound to the given study

        Parameters
        ----------
        study : Study
            A study to bind the dataset spec to (should happen in the
            study constructor)
        """
        # Don't need to bind
        return copy(self)

    def find_mismatch(self, other, indent=''):
        mismatch = ''
        sub_indent = indent + '  '
        if self.desc != other.desc:
            mismatch += ('\n{}desc: self={} v other={}'
                         .format(sub_indent, self.desc,
                                 other.desc))
        if self.optional != other.optional:
            mismatch += ('\n{}optional: self={} v other={}'
                         .format(sub_indent, self.optional,
                                 other.optional))
        return mismatch

    @property
    def optional(self):
        return self._optional

    @property
    def desc(self):
        return self._desc

    def basename(self, **kwargs):  # @UnusedVariable
        return self.prefixed_name

    def initkwargs(self):
        dct = {}
        dct['desc'] = self.desc
        dct['optional'] = self.optional
        return dct


class AcquiredDatasetSpec(BaseDataset, BaseAcquiredSpec):
    """
    A specification for an acquired dataset within a study,
    e.g from the scanner

    Parameters
    ----------
    name : str
        The name of the dataset
    format : FileFormat
        The file format used to store the dataset. Can be one of the
        recognised formats
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_project',
        specifying whether the dataset is present for each session, subject,
        visit or project.
    desc : str
        Description of what the field represents
    optional : bool
        Whether the specification is optional or not.
    """

    is_spec = True

    def __init__(self, name, accepted_formats, frequency='per_session',  # @ReservedAssignment @IgnorePep8
                 desc=None, optional=False):
        if isinstance(accepted_formats, DataFormat):
            format = accepted_formats  # @ReservedAssignment
            accepted_formats = [accepted_formats]
        else:
            self._accepted_formats = tuple(accepted_formats)
            format = None  # No format as it can be one of many @ReservedAssignment @IgnorePep8
        BaseDataset.__init__(self, name, format, frequency)
        BaseAcquiredSpec.__init__(self, desc, optional=optional)

    def __eq__(self, other):
        return (BaseDataset.__eq__(self, other) and
                BaseAcquiredSpec.__eq__(self, other))

    def __repr__(self):
        return ("AcquiredDatasetSpec(name='{}', format={}, "
                "frequency={}, optional={})".format(
                    self.name, self.format, self.pipeline_name,
                    self.frequency, self.optional))

    def valid(self, input):  # @ReservedAssignment
        return input.format in self._accepted_formats

    def find_mismatch(self, other, indent=''):
        mismatch = BaseDataset.find_mismatch(self, other, indent)
        mismatch += BaseAcquiredSpec.find_mismatch(self, other, indent)
        return mismatch

    def initkwargs(self):
        dct = BaseDataset.initkwargs(self)
        dct.update(BaseAcquiredSpec.initkwargs(self))
        return dct


class AcquiredFieldSpec(BaseField, BaseAcquiredSpec):
    """
    An abstract base class representing either an acquired value or the
    specification for a derived dataset.

    Parameters
    ----------
    name : str
        The name of the dataset
    dtype : type
        The datatype of the value. Can be one of (float, int, str)
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_project',
        specifying whether the dataset is present for each session, subject,
        visit or project.
    desc : str
        Description of what the field represents
    optional : bool
        Whether the specification is optional or not.
    """

    is_spec = True

    def __init__(self, name, dtype, frequency='per_session', desc=None,
                 optional=False):
        BaseField.__init__(self, name, dtype, frequency)
        BaseAcquiredSpec.__init__(self, name, desc, optional=optional)

    def __eq__(self, other):
        return (BaseField.__eq__(self, other) and
                BaseAcquiredSpec.__eq__(self, other))

    def find_mismatch(self, other, indent=''):
        mismatch = BaseField.find_mismatch(self, other, indent)
        mismatch += BaseAcquiredSpec.find_mismatch(self, other, indent)
        return mismatch

    def __repr__(self):
        return ("{}(name='{}', dtype={}, frequency={}, "
                "optional={})".format(
                    self.__class__.__name__, self.name, self.dtype,
                    self.frequency, self.optional))

    def initkwargs(self):
        dct = BaseField.initkwargs(self)
        dct.update(BaseAcquiredSpec.initkwargs(self))
        return dct


class BaseDatasetInputOrOutput(object):
    """
    An abstract base class representing either a dataset input or output
    to a pipeline

    Parameters
    ----------
    name : str
        The name of the dataset
    format : DataFormat
        The file format used to store the dataset.
    """

    def __init__(self, name, format):  # @ReservedAssignment @IgnorePep8
        self._name = name
        self._format = format

    def __eq__(self, other):
        return (self._name == other._name and
                self._format == other._format)

    def find_mismatch(self, other, indent=''):
        mismatch = ''
        sub_indent = indent + '  '
        if self.name != other.name:
            mismatch += ('\n{}name: self={} v other={}'
                         .name(sub_indent, self.name,
                                 other.name))
        if self.format != other.format:
            mismatch += ('\n{}format: self={} v other={}'
                         .format(sub_indent, self.format,
                                 other.format))
        return mismatch

    @property
    def format(self):
        return self._format

    def __repr__(self):
        return ("{}(name='{}', format={})"
                .format(self.__class__.__name__, self.name,
                        self.format))

    def initkwargs(self):
        dct = {}
        dct['name'] = self.name
        dct['format'] = self.format
        return dct


class BaseFieldInputOrOutput(object):
    """
    An abstract base class representing either a field input or output
    to a pipeline

    Parameters
    ----------
    name : str
        The name of the dataset
    dtype : type
        The type of the field
    """

    def __init__(self, name, dtype):  # @ReservedAssignment @IgnorePep8
        self._name = name
        self._dtype = dtype

    def __eq__(self, other):
        return (self._name == other._name and
                self._dtype == other._dtype)

    def find_mismatch(self, other, indent=''):
        mismatch = ''
        sub_indent = indent + '  '
        if self.name != other.name:
            mismatch += ('\n{}name: self={} v other={}'
                         .name(sub_indent, self.name,
                                 other.name))
        if self.dtype != other.dtype:
            mismatch += ('\n{}dtype: self={} v other={}'
                         .dtype(sub_indent, self.dtype,
                                 other.dtype))
        return mismatch

    @property
    def dtype(self):
        return self._dtype

    def __repr__(self):
        return ("{}(name='{}', dtype={})"
                .dtype(self.__class__.__name__, self.name,
                        self.dtype))

    def initkwargs(self):
        dct = {}
        dct['name'] = self.name
        dct['dtype'] = self.dtype
        return dct

# Basically syntactic sugar at this stage, but could be extended in
# future


class DatasetInput(BaseDatasetInputOrOutput):
    pass


class DatasetOutput(BaseDatasetInputOrOutput):
    pass


class FieldInput(BaseFieldInputOrOutput):
    pass


class FieldOutput(BaseFieldInputOrOutput):
    pass
