from past.builtins import basestring
from builtins import object
from itertools import chain
import sys
import types
from logging import getLogger
from collections import defaultdict
from arcana.exception import (
    ArcanaMissingDataException, ArcanaNameError,
    ArcanaError, ArcanaUsageError,
    ArcanaMissingInputError, ArcanaNoConverterError,
    ArcanaCantPickleStudyError)
from arcana.pipeline import Pipeline
from arcana.dataset import (
    BaseDatum, BaseMatch, BaseDataset, BaseField, DatasetSpec)
from nipype.pipeline import engine as pe
from arcana.option import Option
from arcana.interfaces.iterators import (
    InputSessions, InputSubjects)

logger = getLogger('Arcana')


class Study(object):
    """
    Abstract base study class from which all study derive.

    Parameters
    ----------
    name : str
        The name of the study.
    archive : Archive
        An Archive object that provides access to a DaRIS, XNAT or local file
        system
    runner : Runner
        A Runner to process the pipelines required to generate the
        requested derived datasets.
    inputs : Dict[str, DatasetMatch | DatasetSpec | FieldMatch | FieldSpec] | List[DatasetMatch | DatasetSpec | FieldMatch | FieldSpec]
        Either a list or a dictionary containing DatasetMatch,
        FieldMatch, DatasetSpec, or FieldSpec objects, which specify the
        names of input datasets to the study, i.e. those that won't
        be generated by this study (although can be derived by the parent
        MultiStudy)
    options : List[Option] | Dict[str, (int|float|str)]
        Options that are passed to pipelines when they are constructed
        either as a dictionary of key-value pairs or as a list of
        'Option' objects. The name and dtype must match OptionSpecs in
        the _option_spec class attribute (see 'add_option_specs').
    subject_ids : List[(int|str)]
        List of subject IDs to restrict the analysis to
    visit_ids : List[(int|str)]
        List of visit IDs to restrict the analysis to
    enforce_inputs : bool
        Whether to check the inputs to see if any acquired datasets
        are missing
    reprocess : bool
        Whether to reprocess dataset|fields that have been created with
        different parameters and/or pipeline-versions. If False then
        and exception will be thrown if the archive already contains
        matching datasets|fields created with different parameters.


    Class Attrs
    -----------
    add_data_specs : List[DatasetSpec|FieldSpec]
        Adds data specs to the '_data_specs' class attribute,
        which is a dictionary that maps the names of datasets that are
        used and generated by the study to (Dataset|Field)Spec objects.
    add_option_specs : List[OptionSpec]
        Adds data specs to the '_option_specs' class attribute,
        which is a dictionary that maps the names of datasets that are
        used and generated by the study to OptionSpec objects.
    """

    _data_specs = {}
    _option_specs = {}

    implicit_cls_attrs = ['_data_specs', '_option_specs']

    def __init__(self, name, archive, runner, inputs, options=None,
                 subject_ids=None, visit_ids=None,
                 enforce_inputs=True, reprocess=False):
        try:
            if not issubclass(type(self).__dict__['__metaclass__'],
                              StudyMetaClass):
                raise KeyError
        except KeyError:
            raise ArcanaUsageError(
                "Need to have StudyMetaClass (or a sub-class) as "
                "the metaclass of all classes derived from Study")
        self._name = name
        self._archive = archive
        self._runner = runner.bind(self)
        self._inputs = {}
        self._subject_ids = subject_ids
        self._visit_ids = visit_ids
        self._tree_cache = None
        self._reprocess = reprocess
        # Record options accessed before a pipeline is created
        # so they can be attributed to the pipeline after creation
        self._pre_options = defaultdict(list)
        # Convert inputs to a dictionary if passed in as a list/tuple
        if not isinstance(inputs, dict):
            inputs = {i.name: i for i in inputs}
        # Add each "input dataset" checking to see whether the given
        # dataset_spec name is valid for the study types
        for inpt_name, inpt in list(inputs.items()):
            try:
                spec = self.data_spec(inpt_name)
            except ArcanaNameError:
                raise ArcanaNameError(
                    inpt.name,
                    "Input name '{}' isn't in data specs of {} ('{}')"
                    .format(
                        inpt.name, self.__class__.__name__,
                        "', '".join(self._data_specs)))
            else:
                if isinstance(spec, DatasetSpec):
                    if isinstance(inpt, BaseField):
                        raise ArcanaUsageError(
                            "Passed field ({}) as input to dataset spec"
                            " {}".format(inpt, spec))
                    try:
                        spec.format.converter_from(inpt.format)
                    except ArcanaNoConverterError as e:
                        raise ArcanaNoConverterError(
                            "{}, which is requried to convert:\n{} "
                            "to\n{}.".format(e, inpt, spec))
                elif not isinstance(inpt, BaseField):
                    raise ArcanaUsageError(
                        "Passed dataset ({}) as input to field spec {}"
                        .format(inpt, spec))
            self._inputs[inpt_name] = inpt.bind(self)
        # "Bind" data specs in the class to the current study object
        # this will allow them to prepend the study name to the name
        # of the dataset
        self._bound_specs = {}
        for spec in self.data_specs():
            if spec.name not in self.input_names:
                if not spec.derived:
                    # Emit a warning if an acquired dataset has not been
                    # provided for an "acquired dataset"
                    msg = (" acquired dataset '{}' was not given as"
                           " an input of {}.".format(spec.name, self))
                    if spec.optional:
                        logger.info('Optional' + msg)
                    else:
                        if enforce_inputs:
                            raise ArcanaMissingInputError(
                                'Non-optional' + msg + " Pipelines "
                                "depending on this dataset will not "
                                "run")
                else:
                    self._bound_specs[spec.name] = spec.bind(self)
        if options is None:
            options = {}
        elif not isinstance(options, dict):
            # Convert list of options into dictionary
            options = {o.name: o for o in options}
        self._options = {}
        # Set options
        for opt_name, opt in list(options.items()):
            if not isinstance(opt, Option):
                opt = Option(opt_name, opt)
            try:
                opt_spec = self._option_specs[opt_name]
            except KeyError:
                raise ArcanaNameError(
                    "Provided option '{}' is not present in the "
                    "allowable options for {} classes ('{}')"
                    .format(opt_name, type(self).__name__,
                            "', '".join(self.default_options)))
            if opt.value is not None and not isinstance(opt.value,
                                                        opt_spec.dtype):
                raise ArcanaUsageError(
                    "Incorrect datatype for '{}' option provided "
                    "to {}(name='{}') (as '{}'), ({}). Should be {}"
                    .format(opt.name, type(self).__name__, name,
                            opt_name, type(opt.value), opt_spec.dtype))
            if (opt_spec.choices is not None and
                    opt.value not in opt_spec.choices):
                raise ArcanaUsageError(
                    "Provided value for '{}' option in {}(name='{}') "
                    "(as '{}') , {}, is not a valid choice. Can be one "
                    "of {}"
                    .format(opt.name, type(self).__name__, name,
                            opt_name, opt.value,
                            ','.join(opt_spec.choices)))
            self._options[opt_name] = opt

    def __repr__(self):
        """String representation of the study"""
        return "{}(name='{}')".format(self.__class__.__name__,
                                      self.name)

    def __reduce__(self):
        """
        Control how study classes are pickled to allow some generated
        classes (those that don't define additional methods) to be
        generated
        """
        cls = type(self)
        module = sys.modules[cls.__module__]
        try:
            # Check whether the study class is generated or not by
            # seeing if it exists in its module
            if cls is not getattr(module, cls.__name__):
                raise AttributeError
        except AttributeError:
            cls_dct = {}
            for name, attr in list(cls.__dict__.items()):
                if isinstance(attr, types.FunctionType):
                    try:
                        if not attr.auto_added:
                            raise ArcanaCantPickleStudyError()
                    except (AttributeError, ArcanaCantPickleStudyError):
                        raise ArcanaCantPickleStudyError(
                            "Cannot pickle auto-generated study class "
                            "as it contains non-auto-added method "
                            "{}:{}".format(name, attr))
                elif name not in self.implicit_cls_attrs:
                    cls_dct[name] = attr
            pkld = (pickle_reconstructor,
                    (cls.__metaclass__, cls.__name__, cls.__bases__,
                     cls_dct), self.__dict__)
        else:
            # Use standard pickling if not a generated class
            pkld = object.__reduce__(self)
        return pkld

    @property
    def tree(self):
        if self._tree_cache is None:
            self._tree_cache = self.archive.get_tree(
                subject_ids=self._subject_ids,
                visit_ids=self._visit_ids)
        return self._tree_cache

    def reset_tree(self):
        self._tree_cache = None

    @property
    def runner(self):
        return self._runner

    @property
    def inputs(self):
        return list(self._inputs.values())

    @property
    def input_names(self):
        return list(self._inputs.keys())

    def input(self, name):
        try:
            return self._inputs[name]
        except KeyError:
            raise ArcanaNameError(
                name,
                "{} doesn't have an input named '{}'"
                .format(self, name))

    @property
    def subject_ids(self):
        if self._subject_ids is None:
            return [s.id for s in self.tree.subjects]
        return self._subject_ids

    @property
    def visit_ids(self):
        if self._visit_ids is None:
            return [v.id for v in self.tree.visits]
        return self._visit_ids

    @property
    def prefix(self):
        """The study name as a prefix for dataset names"""
        return self.name + '_'

    @property
    def name(self):
        """Accessor for the unique study name"""
        return self._name

    @property
    def reprocess(self):
        return self._reprocess

    @property
    def archive(self):
        "Accessor for the archive member (e.g. Daris, XNAT, MyTardis)"
        return self._archive

    def create_pipeline(self, *args, **kwargs):
        """
        Creates a Pipeline object, passing the study (self) as the first
        argument
        """
        pipeline = Pipeline(self, *args, **kwargs)
        # Register options used before the pipeline was created
        try:
            pipeline._used_options.update(
                self._pre_options.pop(pipeline.name))
        except KeyError:
            pass
        if self._pre_options:
            raise ArcanaUsageError(
                "Orphanned pre-options for '{}' pipeline(s) remain in "
                "'{}' {} after creating '{}' pipeline. Please check "
                "pipeline generation code".format(
                    "', '".join(list(self._pre_options.keys())),
                    self.name, type(self).__name__, pipeline.name))
        return pipeline

    def _get_option(self, name):
        try:
            option = self._options[name]
        except KeyError:
            try:
                option = self._option_specs[name]
            except KeyError:
                raise ArcanaNameError(
                    name,
                    "{} does not have an option named '{}'".format(
                        self, name))
        return option

    def pre_option(self, name, pipeline_name, name_prefix='', **kwargs):  # @UnusedVariable @IgnorePep8
        """
        Retrieves the value of the option provided to the
        study and "pre-registers" the option as being used by the
        pipeline matching the name provided.

        It is used to access options that before a pipeline is created
        that affect how the pipeline is created. An error is thrown if
        the next pipeline created doesn't match 'pipeline_name'.

        Parameters
        ----------
        name : str
            The name of the option to retrieve
        pipeline_name : str
            The name of the pipeline to attribute the option to
        """
        option = self._get_option(name)
        # Register option as being used by the pipeline
        self._pre_options[name_prefix + pipeline_name].append(option)
        return option.value

    @property
    def options(self):
        for opt_name in self._option_specs:
            yield self._get_option(opt_name)

    def data(self, name, subject_id=None, visit_id=None):
        """
        Returns the Dataset or Field associated with the name,
        generating derived datasets as required. Multiple names in a
        list can be provided, in which case their workflows are
        joined into a single workflow.

        Parameters
        ----------
        name : str | List[str]
            The name of the DatasetSpec|FieldSpec to retried the
            datasets for
        subject_id : int | str | List[int|str] | None
            The subject ID or subject IDs to return. If None all are
            returned
        visit_id : int | str | List[int|str] | None
            The visit ID or visit IDs to return. If None all are
            returned

        Returns
        -------
        data : Dataset | Field | List[Dataset | Field] | List[List[Dataset | Field]]
            If a single name is provided then data is either a single
            Dataset or field if a single subject_id and visit_id are
            provided, otherwise a list of datasets or fields
            corresponding to the given name. If muliple names are
            provided then a list is returned containing the data for
            each provided name.
        """
        if isinstance(name, basestring):
            single_name = True
            names = [name]
        else:
            names = name
            single_name = False
        def is_single_id(id_):  # @IgnorePep8
            return isinstance(id_, (str, int))
        subject_ids = ([subject_id]
                       if is_single_id(subject_id) else subject_id)
        visit_ids = ([visit_id] if is_single_id(visit_id) else visit_id)
        # Work out which pipelines need to be run
        pipelines = []
        for name in names:
            try:
                pipelines.append(self.spec(name).pipeline)
            except AttributeError:
                pass  # Match objects don't have pipelines
        # Run all pipelines together
        if pipelines:
            self.runner.run(
                *pipelines, subject_ids=subject_ids,
                visit_ids=visit_ids)
        all_data = []
        for name in names:
            spec = self.spec(name)
            if isinstance(spec, BaseMatch):
                data = spec.matches
            else:
                if isinstance(spec, BaseDataset):
                    data = chain(*(
                        (d for d in n.datasets
                         if d.name == spec.prefixed_name)
                        for n in self.tree.nodes(spec.frequency)))
                elif isinstance(spec, BaseField):
                    data = chain(*(
                        (f for f in n.fields
                         if f.name == spec.prefixed_name)
                        for n in self.tree.nodes(spec.frequency)))
                else:
                    assert False
            if subject_ids is not None and spec.frequency in (
                    'per_session', 'per_subject'):
                data = [d for d in data if d.subject_id in subject_ids]
            if visit_ids is not None and spec.frequency in (
                    'per_session', 'per_visit'):
                data = [d for d in data if d.visit_id in visit_ids]
            if not data:
                raise ArcanaUsageError(
                    "No matching data found (subject_id={}, visit_id={})"
                    .format(subject_id, visit_id))
            if is_single_id(subject_id) and is_single_id(visit_id):
                assert len(data) == 1
                data = data[0]
            else:
                data = list(data)
            if single_name:
                return data
            all_data.append(data)
        return all_data

    def spec(self, name):
        """
        Returns either the input corresponding to a dataset or field
        field spec or a spec or option that has either
        been passed to the study as an input or can be derived.

        Parameters
        ----------
        name : Str | BaseDatum | Option
            An option, dataset or field or name of one
        """
        if isinstance(name, (BaseDatum, Option)):
            name = name.name
        try:
            spec = self._inputs[name]
        except KeyError:
            try:
                spec = self._bound_specs[name]
            except KeyError:
                if name in self._data_specs:
                    raise ArcanaMissingDataException(
                        "Acquired (i.e. non-generated) dataset '{}' "
                        "was not supplied when the study '{}' was "
                        "initiated".format(name, self.name))
                else:
                    try:
                        spec = self._option_specs[name]
                    except KeyError:
                        raise ArcanaNameError(
                            name,
                            "'{}' is not a recognised spec name "
                            "for {} studies:\n{}."
                            .format(name, self.__class__.__name__,
                                    '\n'.join(chain(
                                        sorted(self.data_spec_names()),
                                        sorted(self.option_spec_names())))))
        return spec

    @classmethod
    def data_spec(cls, name):
        """
        Return the dataset_spec, i.e. the template of the dataset expected to
        be supplied or generated corresponding to the dataset_spec name.

        Parameters
        ----------
        name : Str
            Name of the dataset_spec to return
        """
        if isinstance(name, BaseDatum):
            name = name.name
        try:
            return cls._data_specs[name]
        except KeyError:
            raise ArcanaNameError(
                name,
                "No dataset spec named '{}' in {} (available: "
                "'{}')".format(name, cls.__name__,
                               "', '".join(list(cls._data_specs.keys()))))

    @classmethod
    def option_spec(cls, name):
        try:
            return cls._option_specs[name]
        except KeyError:
            raise ArcanaNameError(
                name,
                "No option spec named '{}' in {} (available: "
                "'{}')".format(name, cls.__name__,
                               "', '".join(list(cls._option_specs.keys()))))

    @classmethod
    def data_specs(cls):
        """Lists all data_specs defined in the study class"""
        return iter(cls._data_specs.values())

    @classmethod
    def option_specs(cls):
        return iter(cls._option_specs.values())

    @classmethod
    def data_spec_names(cls):
        """Lists the names of all data_specs defined in the study"""
        return iter(cls._data_specs.keys())

    @classmethod
    def option_spec_names(cls):
        """Lists the names of all option_specs defined in the study"""
        return iter(cls._option_specs.keys())

    @classmethod
    def spec_names(cls):
        return chain(cls.data_spec_names(), cls.option_spec_names())

    @classmethod
    def acquired_data_specs(cls):
        """
        Lists all data_specs defined in the study class that are
        provided as inputs to the study
        """
        return (c for c in cls.data_specs() if not c.derived)

    @classmethod
    def derived_data_specs(cls):
        """
        Lists all data_specs defined in the study class that are typically
        generated from other data_specs (but can be overridden by input
        datasets)
        """
        return (c for c in cls.data_specs() if c.derived)

    @classmethod
    def derived_data_spec_names(cls):
        """Lists the names of generated data_specs defined in the study"""
        return (c.name for c in cls.derived_data_specs())

    @classmethod
    def acquired_data_spec_names(cls):
        "Lists the names of acquired data_specs defined in the study"
        return (c.name for c in cls.acquired_data_specs())

    def cache_inputs(self):
        """
        Runs the Study's archive source node for each of the inputs
        of the study, thereby caching any data required from remote
        archives. Useful when launching many parallel jobs that will
        all try to concurrently access the remote archive, and probably
        lead to timeout errors.
        """
        workflow = pe.Workflow(name='cache_download',
                               base_dir=self.runner.work_dir)
        subjects = pe.Node(InputSubjects(), name='subjects')
        sessions = pe.Node(InputSessions(), name='sessions')
        subjects.iterables = ('subject_id', tuple(self.subject_ids))
        sessions.iterables = ('visit_id', tuple(self.visit_ids))
        source = self.archive.source(self.inputs, study_name='cache')
        workflow.connect(subjects, 'subject_id', sessions, 'subject_id')
        workflow.connect(sessions, 'subject_id', source, 'subject_id')
        workflow.connect(sessions, 'visit_id', source, 'visit_id')
        workflow.run()


class StudyMetaClass(type):
    """
    Metaclass for all study classes that collates data specs from
    bases and checks pipeline method names.
    """

    def __new__(metacls, name, bases, dct):  # @NoSelf @UnusedVariable
        if not any(issubclass(b, Study) for b in bases):
            raise ArcanaUsageError(
                "StudyMetaClass can only be used for classes that "
                "have Study as a base class")
        try:
            add_data_specs = dct['add_data_specs']
        except KeyError:
            add_data_specs = []
        try:
            add_option_specs = dct['add_option_specs']
        except KeyError:
            add_option_specs = []
        combined_attrs = set()
        combined_data_specs = {}
        combined_option_specs = {}
        for base in reversed(bases):
            # Get the combined class dictionary including base dicts
            # excluding auto-added properties for data and option specs
            combined_attrs.update(
                a for a in dir(base) if (not issubclass(base, Study) or
                                         a not in base.spec_names()))
            try:
                combined_data_specs.update(
                    (d.name, d) for d in base.data_specs())
            except AttributeError:
                pass
            try:
                combined_option_specs.update(
                    (o.name, o) for o in base.option_specs())
            except AttributeError:
                pass
        combined_attrs.update(list(dct.keys()))
        combined_data_specs.update((d.name, d) for d in add_data_specs)
        combined_option_specs.update(
            (o.name, o) for o in add_option_specs)
        # Check that the pipeline names in data specs correspond to a
        # pipeline method in the class
        for spec in add_data_specs:
            pipe_name = spec.pipeline_name
            if pipe_name is not None and pipe_name not in combined_attrs:
                raise ArcanaUsageError(
                    "Pipeline to generate '{}', '{}', is not present"
                    " in '{}' class".format(
                        spec.name, spec.pipeline_name, name))
        # Check for name clashes between data and option specs
        spec_name_clashes = (set(combined_data_specs) &
                             set(combined_option_specs))
        if spec_name_clashes:
            raise ArcanaUsageError(
                "'{}' name both data and option specs in '{}' class"
                .format("', '".join(spec_name_clashes), name))
        dct['_data_specs'] = combined_data_specs
        dct['_option_specs'] = combined_option_specs
        if '__metaclass__' not in dct:
            dct['__metaclass__'] = metacls
        return type(name, bases, dct)


def pickle_reconstructor(metacls, name, bases, cls_dict):
    obj = DummyObject()
    obj.__class__ = metacls(name, bases, cls_dict)
    return obj


class DummyObject(object):
    pass
