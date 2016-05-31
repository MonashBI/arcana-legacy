from abc import ABCMeta
from copy import copy
from itertools import chain
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from logging import Logger
from neuroanalysis.exception import (
    AcquiredComponentException, NoMatchingPipelineException,
    NeuroAnalysisError)
from .interfaces.mrtrix import MRConvert
from neuroanalysis.exception import (
    NeuroAnalysisScanNameError, NeuroAnalysisMissingScanError)
from .archive import Scan, Session


logger = Logger('NeuroAnalysis')


class Dataset(object):

    __metaclass__ = ABCMeta

    def __init__(self, name, project_id, archive, input_scans):
        """
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
        self._name = name
        self._project_id = project_id
        self._input_scans = {}
        for scan_name, scan in input_scans.iteritems():
            self.set_input_scan(scan_name, scan)
        for scan_name in self.acquired_components:
            if scan_name not in self._input_scans:
                logger.warning(
                    "'{}' acquired component was not specified in {} '{}' "
                    "(provided '{}'). Pipelines depending on this component "
                    "will not run".format(
                        scan_name, self.__class__.__name__, self.name,
                        "', '".join(self._input_scans)))
        # TODO: Check that every session has the acquired scans
        self._archive = archive

    def __repr__(self):
        return "{}(name='{}'".format(self.__class__.__name__, self.name)

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
            raise NeuroAnalysisError(
                "study_id is only relevant if sessions argument is None")
        # Ensure all sessions are session objects and they are unique
        sessions = set(Session(session) for session in sessions)
        if not reprocess:
            # Check which sessions already have all the required output files
            # in the archive and don't rerun for those subjects/studies
            complete_sessions = copy(sessions)
            # TODO: Should be able to provide list of outputs required by
            #       upstream pipeline, so if only the outputs that are required
            #       are present then the pipeline doesn't need to be rerun
            for output in pipeline.outputs:
                complete_sessions &= set(self._archive.sessions_with_file(
                    self.scan(output), self.project_id))
            sessions -= complete_sessions
            if not sessions:
                logger.info(
                    "Pipeline '{}' wasn't run as all requested sessions were "
                    "present")
                return  # No sessions need to be rerun
        # Run prerequisite pipelines and save their results into the archive
        for prereq in pipeline.prerequisities:
            # If reprocess is True, prerequisite pipelines are not reprocessed,
            # only if reprocess == 'all'
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
        complete_workflow.add_nodes(
            (inputnode, source, pipeline.workflow, sink))
        # Connect the nodes of the wrapper workflow
        complete_workflow.connect(inputnode, 'session',
                                  source, 'session')
        complete_workflow.connect(inputnode, 'session',
                                  sink, 'session')
        for inpt in pipeline.inputs:
            scan = self.scan(inpt)
            if scan.input and scan.converted_format != scan.format:
                # If the scan is not in the required format for the dataset
                # user MRConvert to convert it
                conversion = pe.Node(
                    MRConvert(), name=(scan.name + '_input_conversion'))
                conversion.inputs.out_ext = scan.converted_format.extension
                conversion.inputs.quiet = True
                complete_workflow.connect(
                    source, scan.filename, conversion, 'in_file')
                scan_source = conversion
                scan_name = 'out_file'
            else:
                scan_source = source
                scan_name = scan.filename
            # Connect the scan to the pipeline input
            complete_workflow.connect(
                scan_source, scan_name, pipeline.inputnode, inpt)
        # Connect all outputs to the archive sink
        for output in pipeline.outputs:
            scan = self.scan(output)
            complete_workflow.connect(
                pipeline.outputnode, output, sink, scan.filename)
        # Run the workflow
        complete_workflow.run()

    def is_generated(self, input_name):
        # generated_components should be defined by the derived class
        return input_name in self.generated_components

    def generating_pipeline(self, scan_name, **options):
        """
        Looks up the pipeline that generates the given file (as
        determined by the 'generated_components dict class member) and creates
        a pipeline for the dataset with the given pipeline options

        Parameters
        ----------
        scan : ProcessedFile
            The file for which the pipeline that generates it is to be returned
        """
        try:
            # Get 'getter' method from class dictionary 'generated_components'
            getter = self.generated_components[scan_name][0]
            # Call getter on dataset and generate the pipeline with appropriate
            # options
            return getter(self, **options)
        except KeyError:
            if scan_name in self.acquired_components:
                raise AcquiredComponentException(scan_name)
            else:
                raise NoMatchingPipelineException(scan_name)

    def set_input_scan(self, name, scan):
        """
        Saves a copy of the scan in the "input scans" (i.e. the scans that
        exist already and don't need to be generated by the dataset) dictionary
        of the dataset, along with their mappings to the standard names used by
        the Dataset class to refer to them. See members 'acquired_components'
        and 'generated_components' of non-abstract Dataset classes.

        Parameters
        ----------
        name : Str
            The name used by the Dataset class to refer to the scan
        scan : Scan
            A scan object referring to a path and format of an existing scan
        """
        # Create a copy of the scan, setting 'input' to true
        try:
            required_format = self.acquired_components[name]
        except KeyError:
            try:
                required_format = self.generated_components[name][1]
                logger.warning(
                    "Providing input scan for generated component '{}' ({})"
                    .format(name, scan))
            except KeyError:
                raise NeuroAnalysisScanNameError(
                    "'{}' does not name an expected acquired ('{}') or "
                    "generated  ('{}')".format(
                        name, "', '".join(self.acquired_components),
                        "', '".join(self.generated_components)))
        self._input_scans[name] = Scan(
            scan.name, format=scan.format, processed=scan.processed,
            input=True, required_format=required_format)

    def scan(self, name):
        # If an input scan has been mapped to a component of the dataset return
        # a Scan object pointing to it
        try:
            scan = self._input_scans[name]
        except KeyError:
            if name in self.generated_components:
                # Prepend dataset name to distinguish from scans generated from
                # other datasets
                scan = Scan(self.name + '_' + name,
                            self.generated_components[name][1], processed=True)
            elif name in self.acquired_components:
                raise NeuroAnalysisMissingScanError(
                    "Required input scan '{}' was not provided (provided '{}')"
                    .format(name, "', '".join(self._input_scans)))
            else:
                raise NeuroAnalysisScanNameError(
                    "Unrecognised scan name '{}'. It is not present in either "
                    "the acquired or generated components ('{}')"
                    .format(name, "', '".join(self.component_names)))
        return scan

    @property
    def project_id(self):
        return self._project_id

    @property
    def name(self):
        return self._name

    @property
    def archive(self):
        return self._archive

    @property
    def all_component_names(self):
        return chain(self.acquired_components, self.generated_components)

    def _create_pipeline(self, *args, **kwargs):
        """
        Creates a Pipeline object, passing the dataset (self) as the first
        argument
        """
        return Pipeline(self, *args, **kwargs)

    @property
    def component_names(self):
        return chain(self.acquired_components.iterkeys(),
                     self.generated_components.iterkeys())

    def component_format(self, name):
        try:
            return self.acquired_components[name]
        except KeyError:
            return self.generated_components[name][1]


class Pipeline(object):
    """
    Basically a wrapper around a NiPype workflow to keep track of the inputs
    and outputs a little better and provide some convenience functions related
    to the Dataset objects.
    """

    def __init__(self, dataset, name, inputs, outputs, description,
                 options, citations, requirements, approx_runtime,
                 min_nthreads=1, max_nthreads=1):
        """
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
        self._name = name
        self._dataset = dataset
        self._workflow = pe.Workflow(name=name)
        # Convert input names into files
        unrecog_inputs = set(n for n in inputs
                             if n not in dataset.all_component_names)
        if unrecog_inputs:
            raise NeuroAnalysisScanNameError(
                "'{}' are not valid inputs names for {} dataset ('{}')"
                .format("', '".join(unrecog_inputs),
                        dataset.__class__.__name__,
                        "', '".join(dataset.all_component_names)))
        self._inputs = inputs
        unrecog_outputs = set(n for n in outputs
                              if n not in dataset.generated_components)
        if unrecog_outputs:
            raise NeuroAnalysisScanNameError(
                "'{}' are not valid output names for {} dataset ('{}')"
                .format("', '".join(unrecog_outputs),
                        dataset.__class__.__name__,
                        "', '".join(dataset.generated_components.keys())))
        self._outputs = outputs
        self._unconnected_inputs = set(inputs)
        self._unconnected_outputs = set(outputs)
        if len(inputs) != len(self._unconnected_inputs):
            raise NeuroAnalysisError(
                "Duplicate inputs found in '{}'".format("', '".join(inputs)))
        if len(outputs) != len(self._unconnected_outputs):
            raise NeuroAnalysisError(
                "Duplicate outputs found in '{}'".format("', '".join(outputs)))
        self._inputnode = pe.Node(IdentityInterface(fields=inputs),
                                  name="{}_inputnode".format(name))
        self._outputnode = pe.Node(IdentityInterface(fields=outputs),
                                   name="{}_outputnode".format(name))
        self._citations = citations
        self._options = options
        self._description = description
        # FIXME: Should check whether these requirements are satisfied at this
        #        point
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

        Parameters
        ----------
        pipelines : List[Pipeline]
            A collection of prerequisite pipelines that is built up via
            recursion
        """
        prereqs = []
        # Loop through the inputs to the pipeline and add the getter method
        # for the pipeline to generate each one
        for input_ in self.inputs:
            try:
                pipeline = self._dataset.generating_pipeline(input_)
                if pipeline not in prereqs:
                    prereqs.append(pipeline)
            except AcquiredComponentException:
                pass
        return prereqs

    def connect(self, *args, **kwargs):
        """
        Performs the connection in the wrapped NiPype workflow
        """
        self._workflow.connect(*args, **kwargs)

    def connect_input(self, input, node, node_input):  # @ReservedAssignment
        if input not in self._inputs:
            raise NeuroAnalysisScanNameError(
                "'{}' is not a valid input for '{}' pipeline ('{}')"
                .format(input, self.name, "', '".join(self._inputs)))
        self._workflow.connect(self._inputnode, input, node, node_input)
        if input in self._unconnected_inputs:
            self._unconnected_inputs.remove(input)

    def connect_output(self, output, node, node_output):
        if output not in self._outputs:
            raise NeuroAnalysisScanNameError(
                "'{}' is not a valid output for '{}' pipeline ('{}')"
                .format(output, self.name, "', '".join(self._outputs)))
        if output not in self._unconnected_outputs:
            raise NeuroAnalysisError(
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
        if self._unconnected_inputs:
            raise NeuroAnalysisError(
                "'{}' input{} not connected".format(
                    "', '".join(self._unconnected_inputs),
                    ('s are' if len(self._unconnected_inputs) > 1 else ' is')))
        if self._unconnected_outputs:
            raise NeuroAnalysisError(
                "'{}' output{} not connected".format(
                    "', '".join(self._unconnected_outputs),
                    ('s are' if len(self._unconnected_outputs) > 1
                     else ' is')))
