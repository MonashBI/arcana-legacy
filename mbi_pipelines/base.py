from abc import ABCMeta
from copy import copy
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from logging import Logger
from collections import deque


logger = Logger('MBIPipelines')


class Dataset(object):

    __metaclass__ = ABCMeta

    def __init__(self, project_id, archive, scan_names):
        """
        project_name -- The name of the project. For DaRIS it is the project
                         id minus the proceeding 1008.2. For XNAT it will be
                         the project code. For local files it is the full path
                         to the directory.
        archive      -- A sub-class of the abstract RIS (Research Informatics
                        System)
        scan_names   -- A dict containing the a mapping between names of
                        dataset components and the acquired scans, e.g.
                        {'diffusion':'ep2d_diff_mrtrix_33_dir_3_inter_b0_p_RL',
                         'distortion_correct':
                           'PRE DWI L-R DIST CORR 36 DIR MrTrix'}
        """
        self._project_id = project_id
        self._scan_names = scan_names
        assert set(scan_names.keys()) == self.acquired_components
        self._archive = archive

    def run_pipeline(self, pipeline, sessions=None, work_dir=None,
                     reprocess=False):
        """
        Gets a data grabber for the requested subject_ids and a data sink from
        the dataset the pipeline belongs to and then combines them together
        with the wrapped workflow and runs the pipeline

          `pipeline`  -- the pipeline to run
          `sessions`  --  iterable of Session objects or ints, which will be
                         interpreted as subject ids for the first (unprocessed)
                         study
          `work_dir`  -- directory in which to run the nipype workflows
          `reprocess` -- whether to rerun the processing for this step. If
                         set to 'all' then pre-requisite pipelines will also be
                         reprocessed.
        """
        # If subject_ids is none use all associated with the project
        if sessions is None:
            sessions = self._archive.subject_ids(self._project_id)
        # Ensure all sessions are session objects and they are unique
        sessions = set(Session(session for session in sessions))
        if not reprocess:
            # Check which sessions already have the required outputs in the
            # archive and don't rerun for those subjects/studies
            complete_sessions = copy(sessions)
            for output in pipeline.outputs:
                complete_sessions &= self._archive.sessions_with_dataset(
                    output + pipeline.suffix)
            sessions -= complete_sessions
            if not sessions:
                logger.info(
                    "Pipeline '{}' wasn't run as all requested sessions were "
                    "present")
                return  # No sessions need to be rerun
        # Run prerequisite pipelines and save into archive
        for inpt in pipeline.inputs:
            if inpt in self.generated_components:
                prereq_pipeline = self.generated_components[inpt](
                    **pipeline.other_kwargs)
                self.run_pipeline(prereq_pipeline, sessions, work_dir,
                                  ('all' if reprocess == 'all' else False))
        # Set up workflow to run the pipeline, loading and saving from the
        # archive
        complete_workflow = pe.Workflow(name=self._name, base_dir=work_dir)
        # Generate extra nodes
        inputnode = pe.Node(IdentityInterface(), name='session_input')
        inputnode.iterables = ('sessions', tuple(sessions))
        source = self._archive.source(self._project_id)
        sink = self._dataset.archive_sink(self._project_id)
        complete_workflow.add_nodes(
            (inputnode, source, self._workflow, sink))
        complete_workflow.connect(inputnode, 'sessions',
                                  source, 'sessions')
        for inpt in pipeline.inputs:
            archive_input = (
                self._scan_names[inpt] if inpt in self.generated_components
                else inpt)
            complete_workflow.connect(
                source, archive_input, pipeline.workflow.inputnode, inpt)
        for output in pipeline.outputs:
            complete_workflow.connect(pipeline.workflow.outputnode, output,
                                      sink, output)
        complete_workflow.run()

    def is_generated(self, input_name):
        # generated_components should be defined by the derived class
        return input_name in self.generated_components


class Pipeline(object):
    """
    Basically a wrapper around a NiPype workflow to keep track of the inputs
    and outputs a little better and provide some convenience functions related
    to the Dataset objects.
    """

    def __init__(self, name, dataset, workflow, inputs, outputs, options,
                 other_kwargs):
        """
        Parameters
        ----------
        name : str
            The name of the pipeline
        dataset : Dataset
            The dataset from which the pipeline was created
        workflow : nipype.Workflow
            The NiPype workflow to run
        inputs : List[str]
            The list of inputs (hard-coded names for un/processed scans/files)
        outputs : List[str]
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
        self._other_kwargs = other_kwargs

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

    def prepend_prequisites(self, pipelines):
        """
        Prepend prerequisite pipelines, and also their prerequisites onto the
        pipelines deque if they are not already present

        Parameters
        ----------
        pipelines : collections.deque
            A collection of prequisite pipelines that is built
        """
        for inpt in self.inputs:
            try:
                pipeline = self._dataset.generated_components[inpt]
                if pipeline not in pipelines:
                    pipelines.appendleft(pipeline)
                    pipeline.prepend_prequisities(pipelines)
            except KeyError:
                assert inpt in self._dataset.acquired_components

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
    def other_kwargs(self):
        return self._other_kwargs

    @property
    def suffix(self):
        """
        A suffixed appended to output filenames when they are archived to
        identify the options used to generate them
        """
        return ''.join('__{}={}'.format(n, v)
                       for n, v in self._options.iteritems())


class Session(object):
    """
    A small wrapper class used to define the subject_id, study_id and whether
    the scan is processed or not
    """

    def __init__(self, subject_id, study_id=1, processed=False):
        if isinstance(subject_id, self.__class__):
            # If subject_id is actually another Session just copy values
            self._subject_id = subject_id.subject_id
            self._study_id = subject_id.study_id
            self._processed = subject_id.processed
        else:
            self._subject_id = subject_id
            self._study_id = study_id
            self._processed = processed

    def __eq__(self, other):
        return (
            self.subject_id == other.subject_id and
            self.study_id == other.study_id and
            self.processed == other.processed)

    def __ne__(self, other):
        return self != other

    def __hash__(self):
        return hash((self.subject_id, self.study_id, self.processed))

    @property
    def subject_id(self):
        return self._subject_id

    @property
    def study_id(self):
        return self._study_id

    @property
    def processed(self):
        return self._processed
