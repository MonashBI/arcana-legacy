from abc import ABCMeta
from copy import copy
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from logging import Logger
from nianalysis.exceptions import NiAnalysisError
from nianalysis.interfaces.mrtrix import MRConvert
from nianalysis.exceptions import (
    NiAnalysisScanNameError, NiAnalysisMissingScanError)
from nianalysis.archive import Session


logger = Logger('NiAnalysis')


class Dataset(object):
    """
    Base dataset class from which all derive.

    Parameters
    ----------
    project_name : str
        The name of the project. For DaRIS it is the project
        id minus the proceeding 1008.2. For XNAT it will be
        the project code. For local files it is the full path
        to the directory.
    archive : Archive
        An Archive object referring either to a DaRIS, XNAT or local file
        system project
    input_scans : Dict[str,base.Scan]
        A dict containing the a mapping between names of dataset components
        and existing scans (typically acquired from the scanner but can
        also be replacements for generated components)
    """

    __metaclass__ = ABCMeta

    def __init__(self, name, project_id, archive, input_scans):
        self._name = name
        self._project_id = project_id
        self._input_scans = {}
        # Add each "input scan" checking to see whether the given component
        # name is valid for the dataset type
        for comp_name, scan in input_scans.iteritems():
            if comp_name not in self._components:
                raise NiAnalysisScanNameError(
                    "Input scan component name '{}' doesn't match any "
                    "components in {} datasets".format(
                        comp_name, self.__class__.__name__))
            self._input_scans[comp_name] = scan
        # Emit a warning if an acquired component has not been provided for an
        # "acquired component"
        for scan in self.acquired_components():
            if scan.name not in self._input_scans:
                logger.warning(
                    "'{}' acquired component was not specified in {} '{}' "
                    "(provided '{}'). Pipelines depending on this component "
                    "will not run".format(
                        scan.name, self.__class__.__name__, self.name,
                        "', '".join(self._input_scans)))
        # TODO: Check that every session has the acquired scans
        self._archive = archive

    def __repr__(self):
        """String representation of the dataset"""
        return "{}(name='{}')".format(self.__class__.__name__, self.name)

    def scan(self, name):
        """
        Returns either the scan that has been passed to the dataset __init__
        matching the component name provided or the processed scan that is to
        be generated using the pipeline associated with the generated component

        Parameters
        ----------
        scan : Str
            Name of the component to the find the corresponding acquired scan
            or processed scan to be generated
        """
        try:
            scan = self._input_scans[name]
        except KeyError:
            try:
                scan = self._components[name].apply_prefix(self.name + '_')
            except KeyError:
                raise NiAnalysisScanNameError(
                    "'{}' is not a recognised component name for {} datasets."
                    .format(name, self.__class__.__name__))
            if not scan.processed:
                raise NiAnalysisMissingScanError(
                    "Acquired (i.e. non-generated) scan '{}' is required for "
                    "requested pipelines but was not supplied when the dataset"
                    "was initiated.".format(name))
        return scan

    def run_pipeline(self, pipeline, sessions=None, work_dir=None,
                     reprocess=False, study_id=None):
        """
        Gets a data source and data sink from the archive for the requested
        sessions, connects them to the pipeline's NiPyPE workflow and runs
        the pipeline


        Parameters
        ----------
        pipeline : Pipeline
            The pipeline to run
        sessions : List[Session|int]
            A list (or iterable) of Session objects or ints, which will be
            interpreted as subject ids for the first (unprocessed) study
        work_dir : str
            A directory in which to run the nipype workflows
        reprocess: True|False|'all'
            A flag which determines whether to rerun the processing for this
            step. If set to 'all' then pre-requisite pipelines will also be
            reprocessed.
        study_ids: int|List[int]|None
            Id or ids of studies of which to return sessions for. If None all
            are returned
        """
        # Check all inputs and outputs are connected
        pipeline.assert_connected()
        # If subject_ids is none use all associated with the project
        if sessions is None:
            sessions = self._archive.all_sessions(self._project_id,
                                                  study_id=study_id)
        elif study_id is not None:
            raise NiAnalysisError(
                "study_id is only relevant if sessions argument is None")
        # Ensure all sessions are session objects and they are unique
        sessions = set(Session(session) for session in sessions)
        if not reprocess:
            # If the pipeline can be run independently for each session check
            # to see the sessions which have already been completed and omit
            # them from the sessions to be processed
            if not any(self.component(o).multiplicity == 'per_project'
                       for o in pipeline.outputs):
                # Check which sessions already have all the required output
                # files in the archive and don't rerun for those
                # subjects/studies
                completed_sessions = copy(sessions)
                # TODO: Should be able to provide list of outputs required by
                #       the upstream pipeline, so if only the outputs that are
                #       required are present then the pipeline doesn't need to
                #       be rerun
                for output in pipeline.outputs:
                    completed_sessions &= set(self._archive.sessions_with_file(
                        self.scan(output), self.project_id))
                sessions -= completed_sessions
                if not sessions:
                    logger.info(
                        "Pipeline '{}' wasn't run as all requested sessions "
                        "were present")
                    return  # No sessions need to be rerun
        # Run prerequisite pipelines and save their results into the archive
        for prereq in pipeline.prerequisities:
            # NB: Even if reprocess==True, the prerequisite pipelines are not
            #     re-processed, they are only reprocessed if reprocess == 'all'
            self.run_pipeline(prereq, sessions, work_dir,
                              (reprocess if reprocess == 'all' else False))
        # Set up workflow to run the pipeline, loading and saving from the
        # archive
        complete_workflow = pe.Workflow(name=pipeline.name, base_dir=work_dir)
        # Generate an input node for the sessions iterable
        inputnode = pe.Node(IdentityInterface(['session']),
                            name='session_input')
        inputnode.iterables = ('session',
                               [(s.subject_id, s.study_id) for s in sessions])
        # Create source and sinks from the archive
        source = self.archive.source(self._project_id,
                                     (self.scan(i) for i in pipeline.inputs))
        sink = self.archive.sink(self._project_id)
        sink.inputs.description = pipeline.description
        sink.inputs.name = self.name
        # Add all extra nodes and the pipelines workflow to a wrapper workflow
        complete_workflow.add_nodes((inputnode, source, pipeline.workflow,
                                     sink))
        # Connect the nodes of the wrapper workflow
        complete_workflow.connect(inputnode, 'session', source, 'session')
        complete_workflow.connect(inputnode, 'session', sink, 'session')
        for inpt in pipeline.inputs:
            # Get the scan corresponding to the pipeline's input
            scan = self.scan(inpt)
            # Get the component (scan template) corresponding to the pipeline's
            # input
            comp = self.component(inpt)
            # If the scan is not in the required format for the dataset
            # user MRConvert to convert it
            if scan.format != comp.format:
                conversion = pe.Node(MRConvert(),
                                     name=(comp.name + '_input_conversion'))
                conversion.inputs.out_ext = comp.format.extension
                conversion.inputs.quiet = True
                complete_workflow.connect(source, scan.filename,
                                          conversion, 'in_file')
                scan_source = conversion
                scan_name = 'out_file'
            else:
                scan_source = source
                scan_name = scan.filename
            # Connect the scan to the pipeline input
            complete_workflow.connect(scan_source, scan_name,
                                      pipeline.inputnode, inpt)
        # Connect all outputs to the archive sink
        for output in pipeline.outputs:
            scan = self.scan(output)
            if scan.processed:  # Skip scans which are already input scans
                complete_workflow.connect(
                    pipeline.outputnode(scan.name), scan.name, sink, scan.name)
        # Run the workflow
        complete_workflow.run()

    @property
    def project_id(self):
        """Accessor for the project id"""
        return self._project_id

    @property
    def name(self):
        """Accessor for the unique dataset name"""
        return self._name

    @property
    def archive(self):
        """Accessor for the archive member (e.g. Daris, XNAT, MyTardis)"""
        return self._archive

    def _create_pipeline(self, *args, **kwargs):
        """
        Creates a Pipeline object, passing the dataset (self) as the first
        argument
        """
        return Pipeline(self, *args, **kwargs)

    @classmethod
    def component(cls, name):
        """
        Return the component, i.e. the template of the scan expected to be
        supplied or generated corresponding to the component name.

        Parameters
        ----------
        name : Str
            Name of the component to return
        """
        return cls._components[name]

    @classmethod
    def component_names(cls):
        """Lists the names of all components defined in the dataset"""
        return cls._components.iterkeys()

    @classmethod
    def components(cls):
        """Lists all components defined in the dataset class"""
        return cls._components.itervalues()

    @classmethod
    def acquired_components(cls):
        """
        Lists all components defined in the dataset class that are provided as
        inputs to the dataset
        """
        return (c for c in cls.components() if not c.processed)

    @classmethod
    def generated_components(cls):
        """
        Lists all components defined in the dataset class that are typically
        generated from other components (but can be overridden in input scans)
        """
        return (c for c in cls.components() if c.processed)

    @classmethod
    def generated_component_names(cls):
        """Lists the names of generated components defined in the dataset"""
        return (c.name for c in cls.generated_components())

    @classmethod
    def acquired_component_names(cls):
        """Lists the names of acquired components defined in the dataset"""
        return (c.name for c in cls.acquired_components())


class Pipeline(object):
    """
    Basically a wrapper around a NiPype workflow to keep track of the inputs
    and outputs a little better and provide some convenience functions related
    to the Dataset objects.

    Parameters
    ----------
    name : str
        The name of the pipeline
    dataset : Dataset
        The dataset from which the pipeline was created
    workflow : nipype.Workflow
        The NiPype workflow to run
    inputs : List[BaseFile]
        The list of input files required for the pipeline
        un/processed files, and the options used to generate them for
        unprocessed files
    outputs : List[ProcessedFile]
        The list of outputs (hard-coded names for un/processed scans/files)
    options : Dict[str, *]
        Options that effect the output of the pipeline
    citations : List[Citation]
        List of citations that describe the workflow and should be cited in
        publications
    requirements : List[Requirement]
        List of external package requirements (e.g. FSL, MRtrix) required
        by the pipeline
    approx_runtime : float
        Approximate run time in minutes. Should be conservative so that
        it can be used to set time limits on HPC schedulers
    min_nthreads : int
        The minimum number of threads the pipeline requires to run
    max_nthreads : int
        The maximum number of threads the pipeline can use effectively.
        Use None if there is no effective limit
    """

    def __init__(self, dataset, name, inputs, outputs, description,
                 options, citations, requirements, approx_runtime,
                 min_nthreads=1, max_nthreads=1):
        # Check for unrecognised inputs/outputs
        unrecog_inputs = set(n for n in inputs
                             if n not in dataset.component_names())
        assert not unrecog_inputs, (
            "'{}' are not valid inputs names for {} dataset ('{}')"
            .format("', '".join(unrecog_inputs), dataset.__class__.__name__,
                    "', '".join(dataset.component_names())))
        self._inputs = inputs
        unrecog_outputs = set(n for n in outputs
                              if n not in dataset.generated_component_names())
        assert not unrecog_outputs, (
            "'{}' are not valid output names for {} dataset ('{}')"
            .format("', '".join(unrecog_outputs), dataset.__class__.__name__,
                    "', '".join(dataset.generated_component_names())))
        self._name = name
        self._dataset = dataset
        self._workflow = pe.Workflow(name=name)
        self._outputs = outputs
        # Create sets of unconnected inputs/outputs
        self._unconnected_inputs = set(inputs)
        self._unconnected_outputs = set(outputs)
        assert len(inputs) == len(self._unconnected_inputs), (
            "Duplicate inputs found in '{}'".format("', '".join(inputs)))
        assert len(outputs) == len(self._unconnected_outputs), (
            "Duplicate outputs found in '{}'".format("', '".join(outputs)))
        self._inputnode = pe.Node(IdentityInterface(fields=inputs),
                                  name="{}_inputnode".format(name))
        self._outputnode = pe.Node(IdentityInterface(fields=outputs),
                                   name="{}_outputnode".format(name))
        self._citations = citations
        self._options = options
        self._description = description
        self._requirements = requirements
        self._approx_runtime = approx_runtime
        self._min_nthreads = min_nthreads
        self._max_nthreads = max_nthreads

    def __repr__(self):
        return "Pipeline(name='{}')".format(self.name)

    def __eq__(self, other):
        # NB: Workflows should be the same for pipelines of the same name so
        #     may not need to be checked.
        return (
            self._name == other._name and
            self._dataset == other._dataset and
            self._workflow == other._workflow and
            self._inputs == other._inputs and
            self._outputs == other._outputs and
            self._options == other._options)

    def __ne__(self, other):
        return not (self == other)

    def run(self, sessions=None, work_dir=None):
        """
        Run a pipeline on the dataset it is bound to for the sessions provided,
        where a "session" is a particular study (or pipeline output) for a
        subject

        Parameters
        ----------
        sessions : Session
            The list of subject/studies to run the pipeline on
        """
        self._dataset.run_pipeline(self, sessions, work_dir=work_dir)

    @property
    def prerequisities(self):
        """
        Recursively append prerequisite pipelines along with their
        prerequisites onto the list of pipelines if they are not already
        present
        """
        # Loop through the inputs to the pipeline and add the instancemethods
        # for the pipelines to generate each of the processed inputs
        pipelines = set()
        for input in self.inputs:  # @ReservedAssignment
            comp = self._dataset.component(input)
            if comp.processed:
                pipelines.add(comp.pipeline)
        # Call pipeline instancemethods to dataset with provided options
        return (p(self._dataset, **self.options) for p in pipelines)

    def connect(self, *args, **kwargs):
        """
        Performs the connection in the wrapped NiPype workflow
        """
        self._workflow.connect(*args, **kwargs)

    def connect_input(self, input, node, node_input):  # @ReservedAssignment
        assert input in self._inputs, (
            "'{}' is not a valid input for '{}' pipeline ('{}')"
            .format(input, self.name, "', '".join(self._inputs)))
        self._workflow.connect(self._inputnode, input, node, node_input)
        if input in self._unconnected_inputs:
            self._unconnected_inputs.remove(input)

    def connect_output(self, output, node, node_output):
        assert output in self._outputs, (
            "'{}' is not a valid output for '{}' pipeline ('{}')"
            .format(output, self.name, "', '".join(self._outputs)))
        assert output in self._unconnected_outputs, (
            "'{}' output has been connected already")
        self._workflow.connect(node, node_output, self._outputnode, output)
        self._unconnected_outputs.remove(output)

    @property
    def name(self):
        return self._name

    @property
    def workflow(self):
        return self._workflow

    @property
    def inputs(self):
        return self._inputs

    @property
    def outputs(self):
        return self._outputs

    @property
    def options(self):
        return self._options

    @property
    def description(self):
        return self._description

    @property
    def inputnode(self):
        return self._inputnode

    @property
    def outputnode(self):
        return self._outputnode

    @property
    def approx_runtime(self):
        return self._approx_runtime

    @property
    def min_nthreads(self):
        return self._min_nthreads

    @property
    def max_nthreads(self):
        return self._max_nthreads

    @property
    def suffix(self):
        """
        A suffixed appended to output filenames when they are archived to
        identify the options used to generate them
        """
        return '__'.join('{}_{}'.format(k, v)
                         for k, v in self.options.iteritems())

    def assert_connected(self):
        """
        Check for unconnected inputs and outputs after pipeline construction
        """
        assert not self._unconnected_inputs, (
            "'{}' input{} not connected".format(
                "', '".join(self._unconnected_inputs),
                ('s are' if len(self._unconnected_inputs) > 1 else ' is')))
        assert not self._unconnected_outputs, (
            "'{}' output{} not connected".format(
                "', '".join(self._unconnected_outputs),
                ('s are' if len(self._unconnected_outputs) > 1 else ' is')))


def _create_component_dict(*comps, **kwargs):
    dct = {}
    for comp in comps:
        if comp.name in dct:
            assert False, ("Multiple values for '{}' found in component list"
                           .format(comp.name))
        dct[comp.name] = comp
    if 'inherit_from' in kwargs:
        combined = _create_component_dict(*kwargs['inherit_from'])
        combined.update(dct)
        dct = combined
    return dct
