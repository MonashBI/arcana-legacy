from abc import ABCMeta
from logging import getLogger
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from nianalysis.exceptions import (
    NiAnalysisDatasetNameError, NiAnalysisMissingDatasetError)
from nianalysis.pipeline import Pipeline


logger = getLogger('NiAnalysis')


class Study(object):
    """
    Abstract base study class from which all study derive.

    Parameters
    ----------
    name : str
        The name of the study.
    project_id: str
        The ID of the archive project from which to access the data from. For
        DaRIS it is the project id minus the proceeding 1008.2. For XNAT it
        will be the project code. For local archives name of the directory.
    archive : Archive
        An Archive object that provides access to a DaRIS, XNAT or local file
        system
    input_datasets : Dict[str, base.Dataset]
        A dict containing the a mapping between names of study dataset_specs
        and existing datasets (typically acquired from the scanner but can
        also be replacements for generated dataset_specs)
    """

    __metaclass__ = ABCMeta

    def __init__(self, name, project_id, archive, input_datasets):
        self._name = name
        self._project_id = project_id
        self._input_datasets = {}
        # Add each "input dataset" checking to see whether the given
        # dataset_spec name is valid for the study type
        for dataset_name, dataset in input_datasets.iteritems():
            if dataset_name not in self._dataset_specs:
                raise NiAnalysisDatasetNameError(
                    "Input dataset dataset_spec name '{}' doesn't match any "
                    "dataset_specs in {} studies".format(
                        dataset_name, self.__class__.__name__))
            self._input_datasets[dataset_name] = dataset
        # Emit a warning if an acquired dataset_spec has not been provided for
        # an "acquired dataset_spec"
        for spec in self.acquired_dataset_specs():
            if spec.name not in self._input_datasets:
                logger.warning(
                    "'{}' acquired dataset_spec was not specified in {} '{}' "
                    "(provided '{}'). Pipelines depending on this dataset "
                    "will not run".format(
                        spec.name, self.__class__.__name__, self.name,
                        "', '".join(self._input_datasets)))
        # TODO: Check that every session has the acquired datasets
        self._archive = archive

    def __repr__(self):
        """String representation of the study"""
        return "{}(name='{}')".format(self.__class__.__name__, self.name)

    def dataset(self, name):
        """
        Returns either the dataset that has been passed to the study __init__
        matching the dataset name provided or the processed dataset that is
        to be generated using the pipeline associated with the generated
        dataset_spec

        Parameters
        ----------
        name : Str
            Name of the dataset_spec to the find the corresponding acquired
            dataset or processed dataset to be generated
        """
        try:
            dataset = self._input_datasets[name]
        except KeyError:
            try:
                dataset = self._dataset_specs[name].apply_prefix(self.name +
                                                                 '_')
            except KeyError:
                raise NiAnalysisDatasetNameError(
                    "'{}' is not a recognised dataset_spec name for {} "
                    "studies."
                    .format(name, self.__class__.__name__))
            if not dataset.processed:
                raise NiAnalysisMissingDatasetError(
                    "Acquired (i.e. non-generated) dataset '{}' is required "
                    "for requested pipelines but was not supplied when the "
                    "study was initiated.".format(name))
        return dataset

    @property
    def project_id(self):
        """Accessor for the project id"""
        return self._project_id

    @property
    def name(self):
        """Accessor for the unique study name"""
        return self._name

    @property
    def archive(self):
        """Accessor for the archive member (e.g. Daris, XNAT, MyTardis)"""
        return self._archive

    def _create_pipeline(self, *args, **kwargs):
        """
        Creates a Pipeline object, passing the study (self) as the first
        argument
        """
        return Pipeline(self, *args, **kwargs)

    @classmethod
    def dataset_spec(cls, name):
        """
        Return the dataset_spec, i.e. the template of the dataset expected to
        be supplied or generated corresponding to the dataset_spec name.

        Parameters
        ----------
        name : Str
            Name of the dataset_spec to return
        """
        return cls._dataset_specs[name]

    @classmethod
    def dataset_spec_names(cls):
        """Lists the names of all dataset_specs defined in the study"""
        return cls._dataset_specs.iterkeys()

    @classmethod
    def dataset_specs(cls):
        """Lists all dataset_specs defined in the study class"""
        return cls._dataset_specs.itervalues()

    @classmethod
    def acquired_dataset_specs(cls):
        """
        Lists all dataset_specs defined in the study class that are provided as
        inputs to the study
        """
        return (c for c in cls.dataset_specs() if not c.processed)

    @classmethod
    def generated_dataset_specs(cls):
        """
        Lists all dataset_specs defined in the study class that are typically
        generated from other dataset_specs (but can be overridden in input
        datasets)
        """
        return (c for c in cls.dataset_specs() if c.processed)

    @classmethod
    def generated_dataset_spec_names(cls):
        """Lists the names of generated dataset_specs defined in the study"""
        return (c.name for c in cls.generated_dataset_specs())

    @classmethod
    def acquired_dataset_spec_names(cls):
        """Lists the names of acquired dataset_specs defined in the study"""
        return (c.name for c in cls.acquired_dataset_specs())


class CombinedStudy(Study):
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
    input_datasets : Dict[str, base.Dataset]
        A dict containing the a mapping between names of study dataset_specs
        and existing datasets (typically acquired from the scanner but can
        also be replacements for generated dataset_specs)
    """

    __metaclass__ = ABCMeta

    # NB: Subclasses are expected to have a class member named
    #     'sub_study_specs' that defines the components that make up the
    #     combined study and the mapping of their dataset names

    class TranslatedPipeline(Pipeline):
        """
        A pipeline that is translated from a sub-study to the combined study.
        It takes the untranslated pipeline as an input and destroys it in the
        process

        Parameters
        ----------
        name : str
            Name of the translated pipeline
        pipeline : Pipeline
            Sub-study pipeline to translate
        combined_study : CombinedStudy
            Study to translate the pipeline to
        add_inputs : list[str]
            List of additional inputs to add to the translated pipeline to be
            connected manually in combined-study getter (i.e. not using
            translate_getter decorator).
        add_outputs : list[str]
            List of additional outputs to add to the translated pipeline to be
            connected manually in combined-study getter (i.e. not using
            translate_getter decorator).
        """

        def __init__(self, name, pipeline, combined_study, add_inputs=None,
                     add_outputs=None):
            self._name = name
            self._study = combined_study
            self._workflow = pipeline.workflow
            ss_name = pipeline.study.name[(len(combined_study.name) + 1):]
            ss_cls, dataset_map = combined_study.sub_study_specs[ss_name]
            assert isinstance(pipeline.study, ss_cls)
            # Translate inputs from sub-study pipeline
            try:
                self._inputs = [dataset_map[i] for i in pipeline.inputs]
            except KeyError as e:
                raise NiAnalysisMissingDatasetError(
                    "'{}' input required for pipeline '{}' in '{}' is not "
                    "present in dataset map:\n{}".format(e, pipeline.name,
                                                         ss_name, dataset_map))
            # Add additional inputs
            self._unconnected_inputs = set()
            if add_inputs is not None:
                self._check_spec_names(add_inputs, 'additional inputs')
                self._inputs.extend(add_inputs)
                self._unconnected_inputs.update(add_inputs)
            # Create new input node
            self._inputnode = pe.Node(IdentityInterface(fields=self._inputs),
                                      name="{}_inputnode".format(name))
            # Connect to sub-study input node
            for inpt in pipeline.inputs:
                self.workflow.connect(self._inputnode, dataset_map[inpt],
                                      pipeline.inputnode, inpt)
            # Translate outputs from sub-study pipeline
            self._outputs = {}
            for mult in pipeline.multiplicities:
                try:
                    self._outputs[mult] = [
                        dataset_map[o]
                        for o in pipeline.multiplicity_outputs(mult)]
                except KeyError as e:
                    raise NiAnalysisMissingDatasetError(
                        "'{}' output required for pipeline '{}' in '{}' is not"
                        " present in dataset map:\n{}".format(
                            e, pipeline.name, ss_name, dataset_map))
            # Add additional outputs
            self._unconnected_outputs = set()
            if add_outputs is not None:
                self._check_spec_names(add_outputs, 'additional outputs')
                for output in add_outputs:
                    combined_study.dataset_spec(output).multiplicity
                    self._outputs[mult].append(output)
                self._unconnected_outputs.update(add_outputs)
            # Create output nodes for each multiplicity
            self._outputnodes = {}
            for mult in self.pipeline.multiplicities:
                self._outputnodes[mult] = pe.Node(
                    IdentityInterface(fields=self._outputs[mult]),
                    name="{}_{}_outputnode".format(name, mult))
                # Connect sub-study outputs
                for output in pipeline.multiplicity_outputs[mult]:
                    self.workflow.connect(pipeline.outputnode(mult), output,
                                          self._outputnodes[mult],
                                          dataset_map[output])
            # Copy additional info fields
            self._citations = pipeline.citations
            self._options = pipeline.options
            self._description = pipeline.description
            self._requirements = pipeline.requirements
            self._approx_runtime = pipeline.approx_runtime
            self._min_nthreads = pipeline.min_nthreads
            self._max_nthreads = pipeline.max_nthreads

    def __init__(self, name, project_id, archive, input_datasets):
        super(CombinedStudy, self).__init__(name, project_id, archive,
                                            input_datasets)
        self._sub_studies = {}
        for (sub_study_name,
             (cls, dataset_map)) in self.component_specs.iteritems():
            # Create copies of the input datasets to pass to the __init__
            # method of the generated components
            mapped_inputs = []
            for dataset in input_datasets:
                try:
                    mapped_inputs.append(
                        dataset.renamed_copy(dataset_map[dataset.name]))
                except KeyError:
                    pass
            # Create sub-component
            sub_component = cls(name + '_' + sub_study_name, project_id,
                                archive, mapped_inputs)
            # Set component as attribute
            setattr(self, sub_study_name, sub_component)
            # Append to dictionary of sub_studies
            assert sub_study_name not in self._sub_studies, (
                "duplicate component names '{}'".format(sub_study_name))
            self._sub_studies[sub_study_name] = sub_component

    @property
    def sub_studies(self):
        return self._sub_studies.itervalues()

    @classmethod
    def translate_getter(cls, sub_study_name, pipeline_getter):
        """
        A "decorator" (although not intended to be used with @) for translating
        pipeline getter methods from a sub-component of a CombinedStudy.
        Returns a new method that calls the getter on the specified sub-
        component then translates the pipeline to the CombinedStudy.

        Parameters
        ----------
        sub_study_name : str
            Name of the component
        pipeline_getter : Study.method
            Unbound method used to create the pipeline in the sub-study
        """
        def translated_getter(self, **kwargs):
            pipeline = pipeline_getter(self._sub_studies[sub_study_name],
                                       **kwargs)
            trans_pipeline = self.TranslatedPipeline(
                sub_study_name + '_' + pipeline.name, pipeline, self)
            trans_pipeline.assert_connected()
            return trans_pipeline
        return translated_getter


def set_dataset_specs(*comps, **kwargs):
    """
    Used to set the dataset specs in every Study class.

    Parameters
    ----------
    specs : list(DatasetSpec)
        List of dataset specs to set into the class
    inherit_from : list(
        The dataset specs from which to inherit *before* the explicitly added
        specs. Used to include dataset specs from base classes and then
        selectively override them.
    """
    dct = {}
    for comp in comps:
        if comp.name in dct:
            assert False, ("Multiple values for '{}' found in component list"
                           .format(comp.name))
        dct[comp.name] = comp
    if 'inherit_from' in kwargs:
        combined = set_dataset_specs(*set(kwargs['inherit_from']))
        # Allow the current components to override the inherited ones
        combined.update(dct)
        dct = combined
    return dct
