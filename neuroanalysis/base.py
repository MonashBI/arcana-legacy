from abc import ABCMeta, abstractmethod
from copy import copy
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from logging import Logger
from neuroanalysis.exception import (
    AcquiredComponentException, NoMatchingPipelineException,
    NeuroAnalysisError)


logger = Logger('NeuroAnalysis')


class Dataset(object):

    __metaclass__ = ABCMeta

    def __init__(self, project_id, archive, scan_names):
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
        scan_names : List[str]
            A dict containing the a mapping between names of
            dataset components and the acquired scans, e.g.
            {'diffusion':'ep2d_diff_mrtrix_33_dir_3_inter_b0_p_RL',
             'distortion_correct': 'PRE DWI L-R DIST CORR 36 DIR MrTrix'}
        """
        self._project_id = project_id
        self._scan_names = scan_names
        assert set(scan_names.keys()) == self.acquired_components
        self._archive = archive

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
        # If subject_ids is none use all associated with the project
        if sessions is None:
            sessions = self._archive.all_sessions(self._project_id,
                                                  study_id=study_id)
        elif study_id is not None:
            raise NeuroAnalysisError(
                "study_id is only relevant if sessions argument is None")
        # Ensure all sessions are session objects and they are unique
        sessions = set(Session(session for session in sessions))
        if not reprocess:
            # Check which sessions already have all the required output files
            # in the archive and don't rerun for those subjects/studies
            complete_sessions = copy(sessions)
            for output in pipeline.outputs:
                complete_sessions &= self._archive.sessions_with_file(
                    output.filename())
            sessions -= complete_sessions
            if not sessions:
                logger.info(
                    "Pipeline '{}' wasn't run as all requested sessions were "
                    "present")
                return  # No sessions need to be rerun
        # Run prerequisite pipelines and save their results into the archive
        for prereq in pipeline.prequisites:
            # If reprocess is True, prerequisite pipelines are not reprocessed,
            # only if reprocess == 'all'
            self.run_pipeline(prereq, sessions, work_dir,
                              (reprocess if reprocess == 'all' else False))
        # Set up workflow to run the pipeline, loading and saving from the
        # archive
        complete_workflow = pe.Workflow(name=self._name, base_dir=work_dir)
        # Generate an input node for the sessions iterable
        inputnode = pe.Node(IdentityInterface(['session']),
                            name='session_input')
        inputnode.iterables = ('session',
                               [(s.subject_id, s.study_id) for s in sessions])
        # Create source and sinks from the archive
        source = self._archive.source(self._project_id, pipeline.inputs)
        sink = self._dataset.archive_sink(self._project_id, pipeline.outputs)
        sink.inputs.description = pipeline.description
        sink.inputs.name = pipeline.name + pipeline.suffix
        # Add all extra nodes and the pipelines workflow to a wrapper workflow
        complete_workflow.add_nodes(
            (inputnode, source, self._workflow, sink))
        # Connect the nodes of the wrapper workflow
        complete_workflow.connect(inputnode, 'session',
                                  source, 'session')
        for input_ in pipeline.inputs:
            complete_workflow.connect(
                source, input_.filename(self._scan_names),
                pipeline.workflow.inputnode, input_.name)
        for output in pipeline.outputs:
            complete_workflow.connect(
                pipeline.workflow.outputnode, output.name,
                sink, output.filename())
        # Run the workflow
        complete_workflow.run()

    def is_generated(self, input_name):
        # generated_components should be defined by the derived class
        return input_name in self.generated_components

    def generating_pipeline(self, file_):
        """
        Looks up the pipeline that generates the given file (as
        determined by the 'generated_components dict class member) and creates
        a pipeline for the dataset with the given pipeline options

        Parameters
        ----------
        file_ : ProcessedFile
            The file for which the pipeline that generates it is to be returned
        """
        try:
            # Get 'getter' method from class dictionary 'generated_components'
            getter = self._dataset.generated_components[file_.name]
            # Call getter on dataset and generate the pipeline with appropriate
            # options
            return getter(self, **file_.options)
        except KeyError:
            if file_.name in self._dataset.acquired_components:
                raise AcquiredComponentException(file_.name)
            else:
                raise NoMatchingPipelineException(file_.name)


class Pipeline(object):
    """
    Basically a wrapper around a NiPype workflow to keep track of the inputs
    and outputs a little better and provide some convenience functions related
    to the Dataset objects.
    """

    def __init__(self, name, dataset, workflow, inputs, outputs, options,
                 description):
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
        self._workflow = workflow
        self._inputs = inputs
        self._outputs = outputs
        self._options = options
        self._description = description

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
    def suffix(self):
        """
        A suffixed appended to output filenames when they are archived to
        identify the options used to generate them
        """


class Session(object):
    """
    A small wrapper class used to define the subject_id and study_id
    """

    def __init__(self, subject_id, study_id=1):
        if isinstance(subject_id, self.__class__):
            # If subject_id is actually another Session just copy values
            self._subject_id = subject_id.subject_id
            self._study_id = subject_id.study_id
        else:
            self._subject_id = subject_id
            self._study_id = study_id

    def __eq__(self, other):
        return (self.subject_id == other.subject_id and
                self.study_id == other.study_id)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return hash((self.subject_id, self.study_id))

    @property
    def subject_id(self):
        return self._subject_id

    @property
    def study_id(self):
        return self._study_id


class BaseFile(object):
    """
    Abstract base class for AcquiredFile and ProcessedFile classes
    """

    __metaclass__ = ABCMeta

    def __init__(self, name, file_format='nii.gz'):
        self._name = name
        self._format = file_format

    @property
    def name(self):
        return self._name

    @property
    def format(self):
        return self._format


class AcquiredFile(BaseFile):

    def __init__(self, name, filename, file_format='nii.gz'):
        super(AcquiredFile, self).__init__(name, file_format)
        self._filename = filename

    @property
    def filename(self):
        return self._filename

    @property
    def processed(self):
        return False


class ProcessedFile(BaseFile):

    def __init__(self, name, options, file_format='nii.gz'):
        BaseFile.__init__(self, name, file_format)
        self._options = options

    @property
    def options(self):
        return self._options

    @property
    def filename(self):
        return "{}{}.{}".format(
            self._name, ''.join('__{}={}'.format(n, v)
                                 for n, v in self._options.iteritems()),
            self._format)

    @property
    def processed(self):
        return True
