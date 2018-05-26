from copy import copy
from arcana.exception import (
    ArcanaError, ArcanaUsageError,
    ArcanaOutputNotProducedException,
    ArcanaMissingDataException)
from .base import BaseDataset, BaseField


class BaseSpec(object):

    def __init__(self, name, pipeline_name=None, desc=None,
                 optional=False):
        if pipeline_name is not None:
            if not isinstance(pipeline_name, basestring):
                raise ArcanaUsageError(
                    "Pipeline name for {} '{}' is not a string "
                    "'{}'".format(name, pipeline_name))
            if optional:
                raise ArcanaUsageError(
                    "Derived datasets cannot be optional ('{}')"
                    .format(name))
        self._pipeline_name = pipeline_name
        self._desc = desc
        self._study = None
        self._optional = optional

    def __eq__(self, other):
        return (self.pipeline_name == other.pipeline_name and
                self.desc == other.desc and
                self._study == other._study and
                self.optional == other.optional)

#     def __hash__(self):
#         return (hash(self.pipeline_name) ^
#                 hash(self.desc) ^
#                 hash(self._study) ^
#                 hash(self.optional))

    def bind(self, study):
        """
        Returns a copy of the DatasetSpec bound to the given study

        Parameters
        ----------
        study : Study
            A study to bind the dataset spec to (should happen in the
            study constructor)
        """
        if self._study is not None:
            bound = self
        else:
            bound = copy(self)
            bound._study = study
            if (self.pipeline_name is not None and
                    not hasattr(study, self.pipeline_name)):
                raise ArcanaError(
                    "{} does not have a method named '{}' required to "
                    "derive {}".format(study, self.pipeline_name,
                                       self))
        return bound

    def find_mismatch(self, other, indent=''):
        mismatch = ''
        sub_indent = indent + '  '
        if self.pipeline_name != other.pipeline_name:
            mismatch += ('\n{}pipeline: self={} v other={}'
                         .format(sub_indent, self.pipeline,
                                 other.pipeline))
        if self.desc != other.desc:
            mismatch += ('\n{}pipeline: self={} v other={}'
                         .format(sub_indent, self.pipeline,
                                 other.pipeline))
        return mismatch

    @property
    def derivable(self):
        """
        Whether the spec (only valid for derived specs) can be derived
        given the inputs and options provided to the study
        """
        if not self.derived:
            raise ArcanaUsageError(
                "'{}' is not a derived {}".format(self.name,
                                                  type(self)))
        try:
            for inpt in self.pipeline.study_inputs:
                self.study.spec(inpt.name)
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
    def optional(self):
        return self._optional

    @property
    def pipeline(self):
        if self.pipeline_name is None:
            raise ArcanaUsageError(
                "{} is an acquired data spec so doesn't have a pipeline"
                .format(self))
        try:
            pipeline = getattr(self.study, self.pipeline_name)()
        except AttributeError:
            raise ArcanaError(
                "There is no pipeline method named '{}' in present in "
                "'{}' study".format(self.pipeline_name, self.study))
        if self.name not in pipeline.output_names:
            raise ArcanaOutputNotProducedException(
                "'{}' is not produced by {} with options: {}".format(
                    self.name, self.study,
                    '\n'.join('{}={}'.format(o.name, o.value)
                              for o in self.study.options)))
        return pipeline

    @property
    def derived(self):
        return self.pipeline_name is not None

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
        dct['optional'] = self.optional
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
    optional : bool
        Whether the specification is optional or not. Only valid for
        "acquired" dataset specs.
    """

    is_spec = True

    def __init__(self, name, format=None, pipeline_name=None,  # @ReservedAssignment @IgnorePep8
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

    def __init__(self, name, dtype, pipeline_name=None,
                 frequency='per_session', desc=None, optional=False):
        BaseField.__init__(self, name, dtype, frequency)
        BaseSpec.__init__(self, name, pipeline_name, desc,
                          optional=optional)

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
