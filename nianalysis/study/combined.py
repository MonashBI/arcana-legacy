from abc import ABCMeta
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from nianalysis.exceptions import NiAnalysisMissingDatasetError
from nianalysis.pipeline import Pipeline
from .base import Study


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
    #     'sub_study_specs' that defines the sub-studies that make up the
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
            for mult in pipeline.mutliplicities:
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
            for mult in pipeline.mutliplicities:
                self._outputnodes[mult] = pe.Node(
                    IdentityInterface(fields=self._outputs[mult]),
                    name="{}_{}_outputnode".format(name, mult))
                # Connect sub-study outputs
                for output in pipeline.multiplicity_outputs(mult):
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
             (cls, dataset_map)) in self.sub_study_specs.iteritems():
            # Create copies of the input datasets to pass to the __init__
            # method of the generated sub-studies
            mapped_inputs = {}
            for dataset_name, dataset in input_datasets.iteritems():
                try:
                    mapped_inputs[dataset_map[dataset_name]] = dataset
                except KeyError:
                    pass  # Ignore datasets that are not required for sub-study
            # Create sub-study
            sub_study = cls(name + '_' + sub_study_name, project_id,
                            archive, mapped_inputs, check_input_datasets=False)
            # Set sub-study as attribute
            setattr(self, sub_study_name, sub_study)
            # Append to dictionary of sub_studies
            assert sub_study_name not in self._sub_studies, (
                "duplicate sub-study names '{}'".format(sub_study_name))
            self._sub_studies[sub_study_name] = sub_study

    @property
    def sub_studies(self):
        return self._sub_studies.itervalues()

    @classmethod
    def translate(cls, sub_study_name, pipeline_getter):
        """
        A "decorator" (although not intended to be used with @) for translating
        pipeline getter methods from a sub-study of a CombinedStudy.
        Returns a new method that calls the getter on the specified sub-
        sub-study then translates the pipeline to the CombinedStudy.

        Parameters
        ----------
        sub_study_name : str
            Name of the sub-study
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
