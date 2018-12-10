from past.builtins import basestring
from builtins import object
from operator import attrgetter
from copy import copy, deepcopy
from arcana.exceptions import (
    ArcanaError, ArcanaUsageError,
    ArcanaOutputNotProducedException,
    ArcanaMissingDataException, ArcanaNameError)
from .base import BaseFileset, BaseField
from .item import Fileset, Field
from .collection import FilesetCollection, FieldCollection


class BaseAcquiredSpec(object):

    derived = False
    # For duck-typing with *Selector objects
    skip_missing = False
    drop_if_missing = False
    derivable = False

    def __init__(self, name, desc=None, optional=False, default=None):
        if optional and default is not None:
            raise ArcanaUsageError(
                "'optional' doesn't make sense for specs ('{}') with default "
                "values"
                .format(name))
        self._desc = desc
        self._study = None
        self._optional = optional
        # Set the name of the default collection-like object so it matches
        # the name of the spec
        if default is not None:
            if default.frequency != self.frequency:
                raise ArcanaUsageError(
                    "Frequency of default collection-like object passed to "
                    "'{}' spec ('{}'), does not match spec ('{}')".format(
                        name, default.frequency, self.frequency))
            default = deepcopy(default)
        self._default = default

    def __eq__(self, other):
        return (self.desc == other.desc and
                self.optional == other.optional and
                self.default == other.default)

    def __hash__(self):
        return (hash(self.desc) ^ hash(self.optional) ^ hash(self.default))

    def initkwargs(self):
        dct = {}
        dct['desc'] = self.desc
        dct['optional'] = self.optional
        dct['default'] = deepcopy(self.default)
        return dct

    def find_mismatch(self, other, indent=''):
        mismatch = ''
        sub_indent = indent + '  '
        if self.optional != other.optional:
            mismatch += ('\n{}optional: self={} v other={}'
                         .format(sub_indent, self.optional,
                                 other.optional))
        if self.default != other.default:
            mismatch += ('\n{}default: self={} v other={}'
                         .format(sub_indent, self.default,
                                 other.default))
        if self.desc != other.desc:
            mismatch += ('\n{}desc: self={} v other={}'
                         .format(sub_indent, self.desc, other.desc))
        return mismatch

    def bind(self, study, **kwargs):  # @UnusedVariable
        """
        Returns a copy of the AcquiredSpec bound to the given study

        Parameters
        ----------
        study : Study
            A study to bind the fileset spec to (should happen in the
            study __init__)
        """
        if self.default is None:
            raise ArcanaError(
                "Attempted to bind '{}' to {} but only acquired specs with "
                "a default value should be bound to studies{})".format(
                    self.name, study))
        if self._study is not None:
            # This avoids rebinding specs to sub-studies that have already
            # been bound to the multi-study
            bound = self
        else:
            bound = copy(self)
            bound._study = study
            bound._default = bound.default.bind(study)
        return bound

    @property
    def optional(self):
        return self._optional

    @property
    def study(self):
        if self._study is None:
            raise ArcanaError(
                "{} is not bound to a study".format(self))
        return self._study

    @property
    def default(self):
        # Ensure the default has the same name as the spec to it can be
        # accessed properly by the source node
        if self._default is not None:
            # FIXME: Should use setter but throwing an error for some reason
            self._default.name = self.name
        return self._default

    @property
    def desc(self):
        return self._desc

    @property
    def collection(self):
        if self._study is None:
            raise ArcanaUsageError(
                "{} needs to be bound to a study before accessing "
                "the corresponding collection".format(self))
        if self.default is None:
            raise ArcanaUsageError(
                "{} does not have default so cannot access its collection"
                .format(self))
        return self.default.collection


class BaseSpec(object):

    derived = True
    # For duck-typing with *Selector objects
    skip_missing = False
    drop_if_missing = False
    derivable = True

    def __init__(self, name, pipeline_getter, desc=None):
        if pipeline_getter is not None:
            if not isinstance(pipeline_getter, basestring):
                raise ArcanaUsageError(
                    "Pipeline name for {} '{}' is not a string "
                    "'{}'".format(name, pipeline_getter))
        self._pipeline_getter = pipeline_getter
        self._desc = desc
        self._study = None
        self._collection = None

    def __eq__(self, other):
        return (self.pipeline_getter == other.pipeline_getter and
                self.desc == other.desc)

    def __hash__(self):
        return (hash(self.pipeline_getter) ^ hash(self.desc))

    def initkwargs(self):
        dct = {}
        dct['pipeline_getter'] = self.pipeline_getter
        dct['desc'] = self.desc
        return dct

    def find_mismatch(self, other, indent=''):
        mismatch = ''
        sub_indent = indent + '  '
        if self.pipeline_getter != other.pipeline_getter:
            mismatch += ('\n{}pipeline_getter: self={} v other={}'
                         .format(sub_indent, self.pipeline_getter,
                                 other.pipeline_getter))
        if self.desc != other.desc:
            mismatch += ('\n{}desc: self={} v other={}'
                         .format(sub_indent, self.desc, other.desc))
        return mismatch

    def bind(self, study, **kwargs):  # @UnusedVariable
        """
        Returns a copy of the Spec bound to the given study

        Parameters
        ----------
        study : Study
            A study to bind the fileset spec to (should happen in the
            study __init__)
        """
        if self._study is not None:
            # Avoid rebinding specs in sub-studies that have already
            # been bound to MultiStudy
            bound = self
        else:
            bound = copy(self)
            bound._study = study
            if not hasattr(study, self.pipeline_getter):
                raise ArcanaError(
                    "{} does not have a method named '{}' required to "
                    "derive {}".format(study, self.pipeline_getter,
                                       self))
            bound._bind_tree(study.tree)
        return bound

    @property
    def collection(self):
        if self._collection is None:
            raise ArcanaUsageError(
                "{} needs to be bound to a study before accessing "
                "the corresponding collection".format(self))
        return self._collection

    def _bind_tree(self, tree, **kwargs):
        # Run the match against the tree
        if self.frequency == 'per_session':
            nodes = []
            for subject in tree.subjects:
                for sess in subject.sessions:
                    nodes.append(sess)
        elif self.frequency == 'per_subject':
            nodes = tree.subjects
        elif self.frequency == 'per_visit':
            nodes = tree.visits
        elif self.frequency == 'per_study':
            nodes = [tree]
        else:
            assert False, "Unrecognised frequency '{}'".format(
                self.frequency)
        self._collection = self.CollectionClass(
            self.name, (self._bind_node(n, **kwargs) for n in nodes),
            frequency=self.frequency,
            **self._specific_collection_kwargs)

    @property
    def derivable(self):
        """
        Whether the spec (only valid for derived specs) can be derived
        given the inputs and switches provided to the study
        """
        try:
            # Just need to iterate all study inputs and catch relevant
            # exceptions
            list(self.pipeline.study_inputs)
        except (ArcanaOutputNotProducedException,
                ArcanaMissingDataException):
            return False
        return True

    @property
    def pipeline_getter(self):
        return self._pipeline_getter

    @property
    def pipeline(self):
        return self.study.pipeline(self.pipeline_getter, [self.name])

    @property
    def study(self):
        if self._study is None:
            raise ArcanaError(
                "{} is not bound to a study".format(self))
        return self._study

    @property
    def desc(self):
        return self._desc

    def _tree_node(self, subject_id=None, visit_id=None):
        if self.frequency == 'per_session':
            node = self.study.tree.subject(subject_id).session(visit_id)
        elif self.frequency == 'per_subject':
            node = self.study.tree.subject(subject_id)
        elif self.frequency == 'per_visit':
            node = self.study.tree.visit(visit_id)
        elif self.frequency == 'per_study':
            node = self.study.tree
        else:
            assert False
        return node


class AcquiredFilesetSpec(BaseFileset, BaseAcquiredSpec):
    """
    A specification for an "acquired" fileset (e.g from the scanner or
    standard atlas)

    Parameters
    ----------
    name : str
        The name of the fileset
    valid_formats : FileFormat | list[FileFormat]
        The acceptable file formats for input filesets to match this spec
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_study',
        specifying whether the fileset is present for each session, subject,
        visit or project.
    desc : str
        Description of what the field represents
    optional : bool
        Whether the specification is optional or not. Only valid for
        "acquired" fileset specs.
    default : FilesetCollection | object
        The default value to be passed as an input to this spec if none are
        provided. Can either be an explicit FilesetCollection or any object
        with a 'collection' property that will return a default collection.
        This object should also implement a 'bind(self, study)' method to
        allow the study to be bound to it.
    """

    is_spec = True
    CollectionClass = FilesetCollection

    def __init__(self, name, valid_formats, frequency='per_session', # @ReservedAssignment @IgnorePep8
                 desc=None, optional=False, default=None):
        # Ensure allowed formats is a list
        try:
            valid_formats = sorted(valid_formats, key=attrgetter('name'))
        except TypeError:
            valid_formats = [valid_formats]
        else:
            if not valid_formats:
                raise ArcanaError(
                    "'{}' spec doesn't have any allowed formats".format(name))
        self._valid_formats = tuple(valid_formats)
        BaseFileset.__init__(self, name, valid_formats[0], frequency)
        BaseAcquiredSpec.__init__(self, name, desc, optional=optional,
                                  default=default)

    @property
    def valid_formats(self):
        return self._valid_formats

    def __eq__(self, other):
        return (BaseFileset.__eq__(self, other) and
                BaseAcquiredSpec.__eq__(self, other) and
                self._valid_formats == other._valid_formats)

    def __hash__(self):
        return (BaseFileset.__hash__(self) ^
                BaseAcquiredSpec.__hash__(self) and
                hash(self.valid_formats))

    def initkwargs(self):
        dct = BaseFileset.initkwargs(self)
        del dct['format']
        dct.update(BaseAcquiredSpec.initkwargs(self))
        dct['valid_formats'] = self._valid_formats
        return dct

    def __repr__(self):
        return ("{}(name='{}', valid_formats={}, frequency={}, default={}, "
                "optional={})"
                .format(type(self).__name__, self.name,
                        list(f.name for f in self.valid_formats),
                        self.frequency, self.default, self.optional))

    def find_mismatch(self, other, indent=''):
        sub_indent = indent + '  '
        mismatch = BaseFileset.find_mismatch(self, other, indent)
        mismatch += BaseAcquiredSpec.find_mismatch(self, other, indent)
        if self.valid_formats != other.valid_formats:
            mismatch += ('\n{}pipeline: self={} v other={}'
                         .format(sub_indent, list(self.valid_formats),
                                 list(other.valid_formats)))
        return mismatch


class FilesetSpec(BaseFileset, BaseSpec):
    """
    A specification for a fileset within a study to be derived from a
    processing pipeline.

    Parameters
    ----------
    name : str
        The name of the fileset
    format : FileFormat
        The file format used to store the fileset. Can be one of the
        recognised formats
    pipeline_getter : str
        Name of the method in the study that constructs a pipeline to derive
        the fileset
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_study',
        specifying whether the fileset is present for each session, subject,
        visit or project.
    desc : str
        Description of what the field represents
    """

    is_spec = True
    CollectionClass = FilesetCollection

    def __init__(self, name, format, pipeline_getter, frequency='per_session',  # @ReservedAssignment @IgnorePep8 
                 desc=None):
        BaseFileset.__init__(self, name, format, frequency)
        BaseSpec.__init__(self, name, pipeline_getter, desc)

    def __eq__(self, other):
        return (BaseFileset.__eq__(self, other) and
                BaseSpec.__eq__(self, other))

    def __hash__(self):
        return (BaseFileset.__hash__(self) ^
                BaseSpec.__hash__(self))

    def initkwargs(self):
        dct = BaseFileset.initkwargs(self)
        dct.update(BaseSpec.initkwargs(self))
        return dct

    def __repr__(self):
        return ("FilesetSpec(name='{}', format={}, pipeline_getter={}, "
                "frequency={})".format(
                    self.name, self.format, self.pipeline_getter,
                    self.frequency))

    def find_mismatch(self, other, indent=''):
        mismatch = BaseFileset.find_mismatch(self, other, indent)
        mismatch += BaseSpec.find_mismatch(self, other, indent)
        return mismatch

    def _bind_node(self, node, **kwargs):
        try:
            fileset = node.fileset(self.name, from_study=self.study.name)
        except ArcanaNameError:
            # For filesets that can be generated by the analysis
            fileset = Fileset(self.name, format=self.format,
                              frequency=self.frequency, path=None,
                              subject_id=node.subject_id,
                              visit_id=node.visit_id,
                              repository=self.study.repository,
                              from_study=self.study.name,
                              exists=False,
                              **kwargs)
        return fileset

    @property
    def _specific_collection_kwargs(self):
        return {'format': self.format}


class AcquiredFieldSpec(BaseField, BaseAcquiredSpec):
    """
    An abstract base class representing an acquired field

    Parameters
    ----------
    name : str
        The name of the fileset
    dtype : type
        The datatype of the value. Can be one of (float, int, str)
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_study',
        specifying whether the fileset is present for each session, subject,
        visit or project.
    desc : str
        Description of what the field represents
    optional : bool
        Whether the specification is optional or not. Only valid for
        "acquired" fileset specs.
    default : FieldCollection | callable
        The default value to be passed as an input to this spec if none are
        provided. Can either be an explicit FieldCollection or any object
        with a 'collection' property that will return a default collection.
        This object should also implement a 'bind(self, study)' method to
        allow the study to be bound to it.
    """

    is_spec = True
    CollectionClass = FieldCollection

    def __init__(self, name, dtype, frequency='per_session', desc=None,
                 optional=False, default=None, array=False):
        BaseField.__init__(self, name, dtype, frequency, array=array)
        BaseAcquiredSpec.__init__(self, name, desc, optional=optional,
                                  default=default)

    def __eq__(self, other):
        return (BaseField.__eq__(self, other) and
                BaseAcquiredSpec.__eq__(self, other))

    def __hash__(self):
        return (BaseField.__hash__(self) ^ BaseAcquiredSpec.__hash__(self))

    def find_mismatch(self, other, indent=''):
        mismatch = BaseField.find_mismatch(self, other, indent)
        mismatch += BaseAcquiredSpec.find_mismatch(self, other, indent)
        return mismatch

    def __repr__(self):
        return ("{}(name='{}', dtype={}, frequency={}, array={})".format(
            self.__class__.__name__, self.name, self.dtype,
            self.frequency, self.array))

    def initkwargs(self):
        dct = BaseField.initkwargs(self)
        dct.update(BaseAcquiredSpec.initkwargs(self))
        return dct


class FieldSpec(BaseField, BaseSpec):
    """
    An abstract base class representing the specification for a derived
    fileset.

    Parameters
    ----------
    name : str
        The name of the fileset
    dtype : type
        The datatype of the value. Can be one of (float, int, str)
    pipeline_getter : str
        Name of the method that constructs pipelines to derive the field
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_study',
        specifying whether the fileset is present for each session, subject,
        visit or project.
    desc : str
        Description of what the field represents
    """

    is_spec = True
    CollectionClass = FieldCollection

    def __init__(self, name, dtype, pipeline_getter=None,
                 frequency='per_session', desc=None, array=False):
        BaseField.__init__(self, name, dtype, frequency, array=array)
        BaseSpec.__init__(self, name, pipeline_getter, desc)

    def __eq__(self, other):
        return (BaseField.__eq__(self, other) and
                BaseSpec.__eq__(self, other))

    def __hash__(self):
        return (BaseField.__hash__(self) ^ BaseSpec.__hash__(self))

    def find_mismatch(self, other, indent=''):
        mismatch = BaseField.find_mismatch(self, other, indent)
        mismatch += BaseSpec.find_mismatch(self, other, indent)
        return mismatch

    def __repr__(self):
        return ("{}(name='{}', dtype={}, pipeline_getter={}, "
                "frequency={}, array={})".format(
                    self.__class__.__name__, self.name, self.dtype,
                    self.pipeline_getter, self.frequency, self.array))

    def initkwargs(self):
        dct = BaseField.initkwargs(self)
        dct.update(BaseSpec.initkwargs(self))
        return dct

    def _bind_node(self, node, **kwargs):
        try:
            field = node.field(self.name, from_study=self.study.name)
        except ArcanaNameError:
            # For fields to be generated by the analysis
            field = Field(self.name, dtype=self.dtype,
                          frequency=self.frequency,
                          subject_id=node.subject_id,
                          visit_id=node.visit_id,
                          repository=self.study.repository,
                          from_study=self.study.name,
                          array=self.array,
                          exists=False, **kwargs)
        return field

    @property
    def _specific_collection_kwargs(self):
        return {'dtype': self.dtype,
                'array': self.array}
