from abc import ABCMeta
from nipype.interfaces.utility import IdentityInterface
from nianalysis.exceptions import (
    NiAnalysisMissingDatasetError, NiAnalysisNameError)
from nianalysis.pipeline import Pipeline
from nianalysis.exceptions import NiAnalysisUsageError
from .base import Study, StudyMetaClass


class MultiStudy(Study):
    """
    Abstract base class for all studies that combine multiple studies into a
    a combined study

    Parameters
    ----------
    name : str
        The name of the combined study.
    project_id: str
        The ID of the archive project from which to access the data from. For
        DaRIS it is the project id minus the proceeding 1008.2. For XNAT it
        will be the project code. For local archives name of the directory.
    archive : Archive
        An Archive object that provides access to a DaRIS, XNAT or local file
        system
    inputs : Dict[str, base.Dataset]
        A dict containing the a mapping between names of study data_specs
        and existing datasets (typically primary from the scanner but can
        also be replacements for generated data_specs)

    Required Sub-Class attributes
    -----------------------------
    sub_study_specs : list[SubStudySpec]
        Subclasses of MultiStudy are expected to have a 'sub_study_specs'
        class member, which defines the sub-studies that make up the combined
        study and the mapping of their dataset names. The key of the outer
        dictionary will be the name of the sub-study, and the value is a tuple
        consisting of the class of the sub-study and a map of dataset names
        from the combined study to the sub-study e.g.

            _sub_study_specs = set_specs(
                SubStudySpec('t1_study', MRIStudy, {'t1': 'mr_scan'}),
                SubStudySpec('t2_study', MRIStudy, {'t2': 'mr_scan'}))

            _data_specs = set_specs(
                DatasetSpec('t1', nifti_gz_format'),
                DatasetSpec('t2', nifti_gz_format'))
    """

    __metaclass__ = ABCMeta

    def __init__(self, name, archive, runner, inputs, **kwargs):
        try:
            assert issubclass(self.__metaclass__, MultiStudyMetaClass)
        except AttributeError:
            assert ("Need to set MultiStudyMetaClass as the metaclass "
                    "of all classes derived from MultiStudy")
        super(MultiStudy, self).__init__(name, archive, runner, inputs,
                                         **kwargs)
        self._sub_studies = {}
        for sub_study_spec in self.sub_study_specs:
            # Create copies of the input datasets to pass to the
            # __init__ method of the generated sub-studies
            mapped_inputs = []
            for inpt in inputs:
                try:
                    mapped_inputs.append(
                        inpt.renamed(sub_study_spec.map(inpt.name)))
                except NiAnalysisNameError:
                    pass  # Ignore datasets not required for sub-study
            # Create sub-study
            sub_study = sub_study_spec.study_class(
                name + '_' + sub_study_spec.name,
                archive, runner, mapped_inputs,
                check_inputs=False)
            # Set sub-study as attribute
            setattr(self, sub_study_spec.name, sub_study)
            # Append to dictionary of sub_studies
            if sub_study_spec.name in self._sub_studies:
                raise NiAnalysisNameError(
                    sub_study_spec.name,
                    "Duplicate sub-study names '{}'"
                    .format(sub_study_spec.name))
            self._sub_studies[sub_study_spec.name] = sub_study

    @property
    def sub_studies(self):
        return self._sub_studies.itervalues()

    def sub_study(self, name):
        try:
            return self._sub_studies[name]
        except KeyError:
            raise NiAnalysisNameError(
                name,
                "'{}' not found in sub-studes ('{}')"
                .format(name, "', '".join(self._sub_studies)))

    @classmethod
    def sub_study_spec(cls, name):
        try:
            return cls._sub_study_specs[name]
        except KeyError:
            raise NiAnalysisNameError(
                name,
                "'{}' not found in sub-studes ('{}')"
                .format(name, "', '".join(cls._sub_study_specs)))

    def __repr__(self):
        return "{}(name='{}', study='{}')".format(
            self.__class__.__name__, self.name, self.study.name)

    @classmethod
    def translate(cls, sub_study_name, pipeline_name, add_inputs=None,
                  add_outputs=None):
        """
        A "decorator" (although not intended to be used with @) for
        translating pipeline getter methods from a sub-study of a
        MultiStudy. Returns a new method that calls the getter on
        the specified sub-study then translates the pipeline to the
        MultiStudy.

        Parameters
        ----------
        sub_study_name : str
            Name of the sub-study
        pipeline_name : str
            Unbound method used to create the pipeline in the sub-study
        add_inputs : list[str]
            List of additional inputs to add to the translated pipeline
            to be connected manually in combined-study getter (i.e. not
            using translate_getter decorator).
        add_outputs : list[str]
            List of additional outputs to add to the translated pipeline
            to be connected manually in combined-study getter (i.e. not
            using translate_getter decorator).
        """
        try:
            assert isinstance(sub_study_name, basestring)
            assert isinstance(pipeline_name, basestring)
        except:
            raise
        def translated_getter(self, name_prefix='',  # @IgnorePep8
                              add_inputs=add_inputs,
                              add_outputs=add_outputs):
            trans_pipeline = TranslatedPipeline(
                self, self.sub_study(sub_study_name),
                pipeline_name, name_prefix=name_prefix,
                add_inputs=add_inputs, add_outputs=add_outputs)
            trans_pipeline.assert_connected()
            return trans_pipeline
        return translated_getter


class SubStudySpec(object):
    """
    Specify a study to be included in a MultiStudy class

    Parameters
    ----------
    name : str
        Name for the sub-study
    study_class : type (sub-classed from Study)
        The class of the sub-study
    name_map : dict[str, str]
        A mapping of dataset/field names from the MultiStudy scope to
        the scopy of the sub-study (i.e. the _data_specs dict in the
        class of the sub-study). All data-specs that are not explicitly
        provided in this mapping are auto-translated using the sub-study
        prefix.
    """

    def __init__(self, name, study_class, name_map=None):
        self._name = name
        self._study_class = study_class
        # Fill dataset map with default values before overriding with
        # argument provided to constructor
        self._name_map = name_map if name_map is not None else {}
        self._inv_map = dict((v, k) for k, v in self._name_map.items())

    @property
    def name(self):
        return self._name

    def __repr__(self):
        return "{}(name='{}', cls={}, name_map={}".format(
            type(self).__name__, self.name, self.study_class,
            self._name_map)

    @property
    def study_class(self):
        return self._study_class

    @property
    def name_map(self):
        nmap = dict((self.apply_prefix(s.name), s.name)
                    for s in self.auto_specs)
        nmap.update(self._name_map)
        return nmap

    def map(self, name):
        try:
            return self._name_map[name]
        except KeyError:
            mapped = self.strip_prefix(name)
            if mapped not in self.study_class.data_spec_names():
                raise NiAnalysisNameError(
                    name,
                    "'{}' has a matching prefix '{}_' but '{}' doesn't"
                    " match any data_sets in the study class {} ('{}')"
                    .format(name, self.name, mapped,
                            self.study_class.__name__,
                            "', '".join(
                                self.study_class.data_spec_names())))
            return mapped

    def inverse_map(self, name):
        try:
            return self._inv_map[name]
        except KeyError:
            if name not in self.study_class.data_spec_names():
                raise NiAnalysisNameError(
                    name,
                    "'{}' doesn't match any data_sets in the study "
                    "class {} ('{}')"
                    .format(name, self.study_class.__name__,
                            "', '".join(
                                self.study_class.data_spec_names())))
            return self.apply_prefix(name)

    def apply_prefix(self, name):
        return self.name + '_' + name

    def strip_prefix(self, name):
        if not name.startswith(self.name + '_'):
            raise NiAnalysisNameError(
                name,
                "'{}' is not explicitly provided in SubStudySpec "
                "name map and doesn't start with the SubStudySpec "
                "prefix '{}_'".format(name, self.name))
        return name[len(self.name) + 1:]

    @property
    def auto_specs(self):
        """
        Specs in the sub-study class that are not explicitly provided
        in the name map
        """
        for spec in self.study_class.data_specs():
            if spec.name not in self._inv_map:
                yield spec


class TranslatedPipeline(Pipeline):
    """
    A pipeline that is translated from a sub-study to the combined
    study.

    Parameters
    ----------
    name : str
        Name of the translated pipeline
    pipeline : Pipeline
        Sub-study pipeline to translate
    combined_study : MultiStudy
        Study to translate the pipeline to
    name_prefix : str
        Prefix to prepend to the pipeline name to avoid name clashes
    add_inputs : list[str]
        List of additional inputs to add to the translated pipeline
        to be connected manually in combined-study getter (i.e. not
        using translate_getter decorator).
    add_outputs : list[str]
        List of additional outputs to add to the translated pipeline
        to be connected manually in combined-study getter (i.e. not
        using translate_getter decorator).
    """

    def __init__(self, combined_study, sub_study, pipeline_name,
                  name_prefix='', add_inputs=None, add_outputs=None):
        # Get the relative name of the sub-study (i.e. without the
        # combined study name prefixed)
        ss_name = sub_study.name[(len(combined_study.name) + 1):]
        name_prefix += ss_name + '_'
        # Create pipeline and overriding its name to include prefix
        # Copy across default options and override with extra
        # provided
        pipeline_getter = getattr(sub_study, pipeline_name)
        pipeline = pipeline_getter(name_prefix=name_prefix)
        try:
            assert isinstance(pipeline, Pipeline)
        except Exception:
            raise
        self._name = pipeline.name
        self._study = combined_study
        self._workflow = pipeline.workflow
        sub_study_spec = combined_study.sub_study_spec(ss_name)
        assert isinstance(pipeline.study, sub_study_spec.study_class)
        # Translate inputs from sub-study pipeline
        try:
            self._inputs = [
                i.renamed(sub_study_spec.inverse_map(i.name))
                for i in pipeline.inputs]
        except NiAnalysisNameError as e:
            raise NiAnalysisMissingDatasetError(
                "'{}' input required for pipeline '{}' in '{}' study "
                " is not present in inverse dataset map:\n{}".format(
                    e.name, pipeline.name, ss_name,
                    sorted(sub_study_spec.name_map.values())))
        # Add additional inputs
        self._unconnected_inputs = set()
        if add_inputs is not None:
            self._check_spec_names(add_inputs, 'additional inputs')
            self._inputs.extend(add_inputs)
            self._unconnected_inputs.update(i.name
                                            for i in add_inputs)
        # Create new input node
        self._inputnode = self.create_node(
            IdentityInterface(fields=list(self.input_names)),
            name="{}_inputnode_wrapper".format(ss_name))
        # Connect to sub-study input node
        for input_name in pipeline.input_names:
            self.workflow.connect(
                self._inputnode,
                sub_study_spec.inverse_map(input_name),
                pipeline.inputnode, input_name)
        # Translate outputs from sub-study pipeline
        self._outputs = {}
        for mult in pipeline.mutliplicities:
            try:
                self._outputs[mult] = [
                    o.renamed(sub_study_spec.inverse_map(o.name))
                    for o in pipeline.multiplicity_outputs(mult)]
            except NiAnalysisNameError as e:
                raise NiAnalysisMissingDatasetError(
                    "'{}' output required for pipeline '{}' in '{}' "
                    "study is not present in inverse dataset map:\n{}"
                    .format(
                        e.name, pipeline.name, ss_name,
                        sorted(sub_study_spec.name_map.values())))
        # Add additional outputs
        self._unconnected_outputs = set()
        if add_outputs is not None:
            self._check_spec_names(add_outputs, 'additional outputs')
            for output in add_outputs:
                combined_study.data_spec(output).multiplicity
                self._outputs[mult].append(output)
            self._unconnected_outputs.update(o.name
                                             for o in add_outputs)
        # Create output nodes for each multiplicity
        self._outputnodes = {}
        for mult in pipeline.mutliplicities:
            self._outputnodes[mult] = self.create_node(
                IdentityInterface(
                    fields=list(
                        self.multiplicity_output_names(mult))),
                name="{}_{}_outputnode_wrapper".format(ss_name,
                                                       mult))
            # Connect sub-study outputs
            for output_name in pipeline.multiplicity_output_names(mult):
                self.workflow.connect(
                    pipeline.outputnode(mult),
                    output_name,
                    self._outputnodes[mult],
                    sub_study_spec.inverse_map(output_name))
        # Copy additional info fields
        self._citations = pipeline._citations
        self._description = pipeline._description
        self._used_options = set()


class MultiStudyMetaClass(StudyMetaClass):
    """
    Metaclass for "multi" study classes that automatically adds
    translated data specs and pipelines from sub-study specs if they
    are not explicitly mapped in the spec.
    """

    def __new__(metacls, name, bases, dct):  # @NoSelf @UnusedVariable
        if not any(issubclass(b, MultiStudy) for b in bases):
            raise NiAnalysisUsageError(
                "MultiStudyMetaClass can only be used for classes that "
                "have MultiStudy as a base class")
        try:
            sub_study_specs = dct['sub_study_specs']
        except KeyError:
            raise NiAnalysisUsageError(
                "MultiStudy class '{}' doesn't not have required "
                "'sub_study_specs' class attribute"
                .format(name))
        try:
            add_data_specs = dct['add_data_specs']
        except AttributeError:
            add_data_specs = dct['add_data_specs'] = []
        explicitly_added = [s.name for s in add_data_specs]
        for sub_study_spec in sub_study_specs:
            # Loop through all data specs that haven't been explicitly
            # mapped and add a data spec in the multi class.
            for data_spec in sub_study_spec.auto_specs:
                trans_sname = sub_study_spec.apply_prefix(
                    data_spec.name)
                if trans_sname not in explicitly_added:
                    initkwargs = data_spec.initkwargs()
                    initkwargs['name'] = trans_sname
                    if data_spec.pipeline_name is not None:
                        trans_pname = sub_study_spec.apply_prefix(
                            data_spec.pipeline_name)
                        initkwargs['pipeline_name'] = trans_pname
                        # Check to see whether pipeline has already been
                        # translated or always existed in the class (when
                        # overriding default options for example)
                        if trans_pname not in dct:
                            dct[trans_pname] = MultiStudy.translate(
                                sub_study_spec.name,
                                data_spec.pipeline_name)
                    add_data_specs.append(type(data_spec)(**initkwargs))
        dct['_sub_study_specs'] = dict(
            (s.name, s) for s in sub_study_specs)
        return StudyMetaClass(name, bases, dct)
