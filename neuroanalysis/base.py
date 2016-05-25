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
from neuroanalysis.exception import NeuroAnalysisScanNameError
from .archive import Scan, Session


logger = Logger('NeuroAnalysis')


class Dataset(object):

    __metaclass__ = ABCMeta

    def __init__(self, name, project_id, archive, scans):
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
        scans_dict : Dict[str:base.Scan]
            A dict containing the a mapping between names of
            dataset components and the acquired scans, e.g.
            {'diffusion':'ep2d_diff_mrtrix_33_dir_3_inter_b0_p_RL',
             'distortion_correct': 'PRE DWI L-R DIST CORR 36 DIR MrTrix'}
        """
        self._project_id = project_id
        self._scans = scans
        if set(scans.keys()) != set(self.acquired_components.keys()):
            raise NeuroAnalysisScanNameError(
                "Dataset scans ('{}') do not match acquired components of "
                "{} ('{}')".format(
                    "', '".join(set(scans.keys())), self.__class__.__name__,
                    "', '".join(set(self.acquired_components.keys()))))
        self._archive = archive
        self._name = name

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
            if inpt in self.acquired_components:
                if scan.to_be_converted:
                    conversion = pe.Node(
                        MRConvert(), name=(scan.name +
                                           '_input_conversion'))
                    conversion.inputs.out_filename = scan.converted_filename
                    complete_workflow.connect(
                        source, scan.name, conversion, 'in_file')
                    converted_source = conversion
                    scan_name = 'out_file'
                else:
                    converted_source = source
                    scan_name = scan.name
                complete_workflow.connect(
                    converted_source, scan_name, pipeline.inputnode, inpt)
            elif inpt in self.generated_components:
                complete_workflow.connect(
                    source, scan.name, pipeline.inputnode, inpt)
            else:
                assert False
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

    def scan(self, name):
        if name in self.acquired_components:
            scan = copy(self._scans[name])
            scan.convert_to(self.acquired_components[name])
        elif name in self.generated_components:
            # Prepend dataset name to distinguish from scans generated from
            # other datasets
            scan = Scan(self.name + '_' + name,
                        self.generated_components[name][1], processed=True)
        else:
            raise NeuroAnalysisScanNameError(
                "Unrecognised scan name '{}'. It is not present in either "
                "the acquired or generated components".format(name))
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


class Pipeline(object):
    """
    Basically a wrapper around a NiPype workflow to keep track of the inputs
    and outputs a little better and provide some convenience functions related
    to the Dataset objects.
    """

    def __init__(self, dataset, name, inputs, outputs, description,
                 options, citations, requirements):
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
        other_kwargs : Dict[str, *]
            Other kwargs passed to the pipeline that do not effect the output
            of the pipeline (but may effect prequisite pipelines)
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
        # TODO: Should check whether these requirements are satisfied at this
        #       point
        self._requirements = requirements

    def __repr__(self):
        return "Pipeline(name='{}')".format(self.name)

    def __eq__(self, other):
        # NB: Workflows should be the same for pipelines of the same name so
        #     may not need to be checked.
        return (
            self._name == other.name and
            self._dataset == other.dataset and
            self._workflow == other.workflow and
            self._inputs == other.inputs and
            self._outputs == other.outputs and
            self._options == other.options)

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
        self._workflow(*args, **kwargs)

    def connect_input(self, input, node, node_input):  # @ReservedAssignment
        if input not in self._inputs:
            raise NeuroAnalysisScanNameError(
                "'{}' is not a valid input for '{}' pipeline ('{}')"
                .format(input, self.name, "', '".join(self._inputs)))
        if input not in self._unconnected_inputs:
            raise NeuroAnalysisError(
                "'{}' input has been connected already")
        self._workflow.connect(self._inputnode, input, node, node_input)
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
