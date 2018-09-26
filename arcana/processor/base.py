from builtins import str
from builtins import object
from copy import copy, deepcopy
from logging import getLogger
from collections import defaultdict
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from arcana.node import JoinNode
from arcana.interfaces.utils import Merge
from arcana.requirement import RequirementManager
from arcana.exception import (
    ArcanaError, ArcanaMissingDataException,
    ArcanaNoRunRequiredException, ArcanaNoConverterError,
    ArcanaUsageError)
from arcana.data.base import BaseFileset, BaseField, FilesetSpec
# from arcana.interfaces.iterators import (
#     InputSessions, PipelineReport, InputSubjects, SubjectReport,
#     VisitReport, SubjectSessionReport, SessionReport)
from arcana.utils import PATH_SUFFIX, FIELD_SUFFIX


logger = getLogger('arcana')


WORKFLOW_MAX_NAME_LEN = 100


class BaseProcessor(object):
    """
    A thin wrapper around the NiPype LinearPlugin used to connect
    runs pipelines on the local workstation

    Parameters
    ----------
    work_dir : str
        A directory in which to run the nipype workflows
    max_process_time : float
        The maximum time allowed for the process
    reprocess: True|False|'all'
        A flag which determines whether to rerun the processing for this
        step. If set to 'all' then pre-requisite pipelines will also be
        reprocessed.
    """

    default_plugin_args = {}

    def __init__(self, work_dir, requirement_manager=None,
                 max_process_time=None, reprocess=False, **kwargs):
        self._work_dir = work_dir
        self._max_process_time = max_process_time
        self._reprocess = reprocess
        self._plugin_args = copy(self.default_plugin_args)
        self._plugin_args.update(kwargs)
        self._init_plugin()
        self._study = None
        self._requirement_manager = (
            requirement_manager if requirement_manager is not None
            else RequirementManager())

    def __repr__(self):
        return "{}(work_dir={})".format(
            type(self).__name__, self._work_dir)

    def __eq__(self, other):
        try:
            return (
                self._work_dir == other._work_dir and
                self._max_process_time == other._max_process_time and
                self._requirement_manager == other._requirement_manager and
                self._reprocess == other._reprocess and
                self._plugin_args == other._plugin_args)
        except AttributeError:
            return False

    def _init_plugin(self):
        self._plugin = self.nipype_plugin_cls(**self._plugin_args)

    @property
    def study(self):
        return self._study

    @property
    def requirement_manager(self):
        return self._requirement_manager

    def requirements_satisfiable(self, *requirements, **kwargs):
        self.requirement_manager.satisfiable(*requirements, **kwargs)

    def load_requirements(self, *requirements, **kwargs):
        self.requirement_manager.load(*requirements, **kwargs)

    def unload_requirements(self, *requirements, **kwargs):
        self.requirement_manager.unload(*requirements, **kwargs)

    def bind(self, study):
        cpy = deepcopy(self)
        cpy._study = study
        return cpy

    def run(self, *pipelines, **kwargs):
        """
        Connects all pipelines to that study's repository and runs them
        in the same NiPype workflow

        Parameters
        ----------
        pipeline(s) : Pipeline, ...
            The pipeline to connect to repository
        subject_ids : List[str]
            The subset of subject IDs to process. If None all available will be
            reprocessed
        visit_ids: List[str]
            The subset of visit IDs for each subject to process. If None all
            available will be reprocessed

        Returns
        -------
        report : ReportNode
            The final report node, which can be connected to subsequent
            pipelines
        """
        if not pipelines:
            raise ArcanaUsageError(
                "No pipelines provided to {}.run"
                .format(self))
        # Create name by combining pipelines
        name = '_'.join(p.name for p in pipelines)
        # Trim the end of very large names to avoid problems with
        # work-dir paths exceeding system limits.
        name = name[:WORKFLOW_MAX_NAME_LEN]
        workflow = pe.Workflow(name=name, base_dir=self.work_dir)
        already_connected = {}
        for pipeline in pipelines:
            try:
                self._connect_to_repository(
                    pipeline, workflow,
                    already_connected=already_connected, **kwargs)
            except ArcanaNoRunRequiredException:
                logger.info("Not running '{}' pipeline as its outputs "
                            "are already present in the repository"
                            .format(pipeline.name))
        # Reset the cached tree of filesets in the repository as it will
        # change after the pipeline has run.
        self.study.repository.clear_cache()
        return workflow.run(plugin=self._plugin)

    def _connect_to_repository(self, pipeline, complete_workflow,
                               subject_ids=None, visit_ids=None,
                               already_connected=None):
        if already_connected is None:
            already_connected = {}
        try:
            (prev_connected, report) = already_connected[pipeline.name]
            if prev_connected == pipeline:
                return report
            else:
                raise ArcanaError(
                    "Name clash between {} and {} non-matching "
                    "prerequisite pipelines".format(prev_connected,
                                                    pipeline))
        except KeyError:
            pass
        # Check all inputs and outputs are connected
        pipeline.assert_connected()
        # Get list of sessions that need to be processed (i.e. if
        # they don't contain the outputs of this pipeline)
        sessions_to_process = self._to_process(
            pipeline, subject_ids=subject_ids, visit_ids=visit_ids)
        if not sessions_to_process:
            raise ArcanaNoRunRequiredException(
                "All outputs of '{}' are already present in project "
                "repository, skipping".format(pipeline.name))
        # Set up workflow to run the pipeline, loading and saving from the
        # repository
        complete_workflow.add_nodes([pipeline._workflow])
        # Get iterator nodes over subjects and sessions to be processed
        subjects, visits, sessions = self._subject_and_session_iterators(
            pipeline, sessions_to_process, complete_workflow)
        # Prepend prerequisite pipelines to complete workflow if required
        if pipeline.has_prerequisites:
            reports = []
            prereq_subject_ids = list(
                set(s.subject_id for s in sessions_to_process))
            prereq_visit_ids = list(
                set(s.visit_id for s in sessions_to_process))
            for prereq in pipeline.prerequisites:
                # NB: Even if reprocess==True, the prerequisite pipelines
                # are not re-processed, they are only reprocessed if
                # reprocess == 'all'
                try:
                    prereq_report = self._connect_to_repository(
                        prereq, complete_workflow=complete_workflow,
                        subject_ids=prereq_subject_ids,
                        visit_ids=prereq_visit_ids,
                        already_connected=already_connected)
                    reports.append(prereq_report)
                except ArcanaNoRunRequiredException:
                    logger.info(
                        "Not running '{}' pipeline as a "
                        "prerequisite of '{}' as the required "
                        "outputs are already present in the repository"
                        .format(prereq.name, pipeline.name))
            if reports:
                prereq_reports = pipeline.create_node(
                    Merge(len(reports)), 'prereq_reports')
                for i, report in enumerate(reports, 1):
                    # Connect the output summary of the prerequisite to the
                    # pipeline to ensure that the prerequisite is run first.
                    complete_workflow.connect(
                        report, 'subject_session_pairs',
                        prereq_reports, 'in{}'.format(i))
                complete_workflow.connect(prereq_reports, 'out', subjects,
                                          'prereq_reports')
        try:
            # Create source and sinks from the repository
            source = self.study.source(
                pipeline.inputs,
                name='{}_source'.format(pipeline.name))
        except ArcanaMissingDataException as e:
            raise ArcanaMissingDataException(
                str(e) + ", which is required for pipeline '{}'".format(
                    pipeline.name))
        # Map the subject and visit IDs to the input node of the pipeline
        # for use in connect_subject_id and connect_visit_id
        complete_workflow.connect(sessions, 'subject_id',
                                  pipeline.inputnode, 'subject_id')
        complete_workflow.connect(sessions, 'visit_id',
                                  pipeline.inputnode, 'visit_id')
        # Connect the nodes of the wrapper workflow
        complete_workflow.connect(sessions, 'subject_id',
                                  source, 'subject_id')
        complete_workflow.connect(sessions, 'visit_id',
                                  source, 'visit_id')
        for freq in pipeline.input_frequencies:
            inputs = pipeline.frequency_inputs(freq)
            inputnode = pipeline.inputnode(freq)
            source = self.study.source(
                [o.name for o in inputs], frequency=freq,
                name='{}_{}_source'.format(pipeline.name, freq))
            if pipeline.iterates_over(freq, pipeline.SUBJECT_ITERFIELD):
                complete_workflow.connect(subjects, pipeline.SUBJECT_ITERFIELD,
                                          source, pipeline.SUBJECT_ITERFIELD)
            if pipeline.iterates_over(freq, pipeline.VISIT_ITERFIELD):
                complete_workflow.connect(visits, pipeline.VISIT_ITERFIELD,
                                          source, pipeline.VISIT_ITERFIELD)
            for input in inputs:  # @ReservedAssignment
                in_name = input.name + (
                    PATH_SUFFIX if isinstance(input, FilesetSpec) else
                    FIELD_SUFFIX)
                complete_workflow.connect(
                    source, in_name,
                    inputnode, input.name)
        # Create a report node for holding a summary of all the sessions/
        # subjects that were sunk. This is used to connect with dependent
        # pipelines into one large connected pipeline.
        report = pipeline.create_node(PipelineReport(), 'report')
        # Connect all outputs to the repository sink, creating a new sink for
        # each frequency level (i.e 'per_session', 'per_subject', 'per_visit',
        # or 'per_study')
        for freq in pipeline.output_frequencies:
            outputs = pipeline.frequency_outputs(freq)
            outputnode = pipeline.outputnode(freq)
            sink = self.study.sink(
                [o.name for o in outputs], frequency=freq,
                name='{}_{}_sink'.format(pipeline.name, freq))
            if pipeline.iterates_over(freq, pipeline.SUBJECT_ITERFIELD):
                complete_workflow.connect(subjects, pipeline.SUBJECT_ITERFIELD,
                                          sink, pipeline.SUBJECT_ITERFIELD)
            if pipeline.iterates_over(freq, pipeline.VISIT_ITERFIELD):
                complete_workflow.connect(visits, pipeline.VISIT_ITERFIELD,
                                          sink, pipeline.VISIT_ITERFIELD)
            for output in outputs:
                if output.is_spec:  # Skip outputs that are study inputs
                    out_name = output.name + (
                        PATH_SUFFIX if isinstance(output, FilesetSpec) else
                        FIELD_SUFFIX)
                    complete_workflow.connect(
                        outputnode, output.name,
                        sink, out_name)
            self._connect_to_reports(
                pipeline, sink, report, freq, subjects, sessions,
                complete_workflow)
        # Register pipeline as being connected to prevent duplicates
        already_connected[pipeline.name] = (pipeline, report)
        return report

    def _subject_and_session_iterators(self, pipeline,
                                       sessions_to_process, workflow):
        """
        Generate an input node that iterates over the sessions and subjects
        that need to be processed.
        """
        # Create nodes to control the iteration over subjects and sessions in
        # the project
        subjects = pipeline.add(
            'subjects', IdentityInterface([pipeline.VISIT_ITERFIELD]),
            wall_time=10, memory=1000)
        visits = pipeline.add(
            'visits', IdentityInterface([pipeline.VISIT_ITERFIELD]),
            wall_time=10, memory=1000)
        sessions = pipeline.add(
            'sessions', IdentityInterface([pipeline.ITERFIELDS]),
            wall_time=10, memory=4000)
        # Construct iterable over all subjects to process
        subjects_to_process = set(s.subject for s in sessions_to_process)
        subject_ids_to_process = set(s.id for s in subjects_to_process)
        subjects.iterables = ('subject_id',
                              tuple(s.id for s in subjects_to_process))
        # Determine whether the visit ids are the same for every subject,
        # in which case they can be set as a constant, otherwise they will
        # need to be specified for each subject separately
        session_subjects = defaultdict(set)
        for session in sessions_to_process:
            session_subjects[session.visit_id].add(session.subject_id)
        if all(ss == subject_ids_to_process
               for ss in session_subjects.values()):
            # All sessions are to be processed in every node, a simple second
            # layer of iterations on top of the subject iterations will
            # suffice. This allows re-combining on visit_id across subjects
            sessions.iterables = ('visit_id', list(session_subjects.keys()))
        else:
            # visit IDs to be processed vary between subjects and so need
            # to be specified explicitly
            subject_sessions = defaultdict(list)
            for session in sessions_to_process:
                subject_sessions[session.subject.id].append(session.visit_id)
            sessions.itersource = ('{}_subjects'.format(pipeline.name),
                                   'subject_id')
            sessions.iterables = ('visit_id', subject_sessions)
        # Connect subject and session nodes together
        workflow.connect(subjects, 'subject_id', sessions, 'subject_id')
        return subjects, visits, sessions

    def _connect_to_reports(self, pipeline, sink, output_summary, freq,
                            subjects, sessions, workflow):
        """
        Connects the sink of the pipeline to an "Output Summary", which lists
        the subjects and sessions that were processed for the pipeline. There
        should be only one summary node instance per pipeline so it can be
        used to feed into the input of subsequent pipelines to ensure that
        they are executed afterwards.
        """
        if freq == 'per_session':
            session_outputs = JoinNode(
                SessionReport(), joinsource=sessions,
                joinfield=['subjects', 'sessions'],
                name=pipeline.name + '_session_outputs', wall_time=20,
                memory=4000)
            subject_session_outputs = JoinNode(
                SubjectSessionReport(), joinfield='subject_session_pairs',
                joinsource=subjects,
                name=pipeline.name + '_subject_session_outputs', wall_time=20,
                memory=4000)
            workflow.connect(sink, 'subject_id', session_outputs, 'subjects')
            workflow.connect(sink, 'visit_id', session_outputs, 'sessions')
            workflow.connect(session_outputs, 'subject_session_pairs',
                             subject_session_outputs, 'subject_session_pairs')
            workflow.connect(
                subject_session_outputs, 'subject_session_pairs',
                output_summary, 'subject_session_pairs')
        elif freq == 'per_subject':
            subject_output_summary = JoinNode(
                SubjectReport(), joinsource=subjects, joinfield='subjects',
                name=pipeline.name + '_subject_summary_outputs', wall_time=20,
                memory=4000)
            workflow.connect(sink, 'subject_id',
                             subject_output_summary, 'subjects')
            workflow.connect(subject_output_summary, 'subjects',
                             output_summary, 'subjects')
        elif freq == 'per_visit':
            visit_output_summary = JoinNode(
                VisitReport(), joinsource=sessions, joinfield='sessions',
                name=pipeline.name + '_visit_summary_outputs', wall_time=20,
                memory=4000)
            workflow.connect(sink, 'visit_id',
                             visit_output_summary, 'sessions')
            workflow.connect(visit_output_summary, 'sessions',
                             output_summary, 'visits')
        elif freq == 'per_study':
            # Only required to ensure that the report is run after the
            # sink
            workflow.connect(sink, 'project_id', output_summary,
                             'project')

    def _to_process(self, pipeline, subject_ids=None,
                    visit_ids=None, reprocess=False):
        """
        Check whether the outputs of the pipeline are present in all sessions
        in the project repository, and make a list of the sessions and subjects
        that need to be reprocessed if they aren't.

        Parameters
        ----------
        pipeline : Pipeline
            The pipeline to determine the sessions to process
        subject_ids : list(str)
            Filter the subject IDs to process
        visit_ids : list(str)
            Filter the visit IDs to process
        reprocess : bool
            Whether to reprocess the pipeline outputs even if they
            exist.
        """
        tree = self.study.tree
        non_per_session = [
            o for o in pipeline.outputs
            if self.study.spec(o).frequency != 'per_session']
        if non_per_session and list(tree.incomplete_subjects):
            raise ArcanaUsageError(
                "Can't process '{}' pipeline as it has non-'per_session'"
                "outputs ({}) and subjects ({}) that are missing one "
                "or more visits ({}). Please restrict the subject/visit "
                "IDs in the study __init__ to continue the analysis"
                .format(
                    self.name,
                    ', '.join(non_per_session),
                    ', '.join(s.id for s in tree.incomplete_subjects),
                    ', '.join(v.id for v in tree.incomplete_visits)))
        sessions = set()
        if reprocess:
            sessions.extend(tree.sessions)
        for output in pipeline.outputs:
            items = self.study.spec(output).collection
            if items.frequency == 'per_study':
                if subject_ids is not None:
                    logger.warning(
                        "Cannot restrict processing to subject "
                        "IDs ({}) for '{}' pipeline as it has a "
                        "'per_study' output ('{}')"
                        .format(', '.join(str(i) for i in subject_ids),
                                pipeline.name, output))
                if visit_ids is not None:
                    logger.warning(
                        "Cannot restrict processing to visit "
                        "IDs ({}) for '{}' pipeline as it has a "
                        "'per_study' output ('{}')"
                        .format(', '.join(str(i) for i in visit_ids),
                                pipeline.name, output))
                # If there is a project output that doesn't exists then
                # all subjects and sessions need to be reprocessed
                if not next(iter(items)).exists:
                    return list(tree.sessions)
            elif items.frequency == 'per_subject':
                if visit_ids is not None:
                    logger.warning(
                        "Cannot restrict processing to visit "
                        "IDs ({}) for '{}' pipeline as it has a "
                        "'per_subject' output ('{}')"
                        .format(', '.join(str(i) for i in visit_ids),
                                pipeline.name, output))
                for item in items:
                    if (not item.exists and
                        (subject_ids is None or
                         item.subject_id in subject_ids)):
                        sessions.update(tree.subject(item.subject_id).sessions)
            elif items.frequency == 'per_visit':
                if subject_ids is not None:
                    logger.warning(
                        "Cannot restrict processing to subject "
                        "IDs ({}) for '{}' pipeline as it has a "
                        "'per_visit' output ('{}')"
                        .format(', '.join(str(i) for i in subject_ids),
                                pipeline.name, output))
                for item in items:
                    if (not item.exists and
                        (visit_ids is None or
                         item.visit_id in visit_ids)):
                        sessions.update(tree.visit(item.visit_id).sessions)
            elif items.frequency == 'per_session':
                for item in items:
                    if (not item.exists and
                        (subject_ids is None or
                         item.subject_id in subject_ids) and
                        (visit_ids is None or
                         item.visit_id in visit_ids)):
                        sessions.add(tree.session(item.subject_id,
                                                  item.visit_id))
            else:
                assert False, ("Unrecognised frequency of {}"
                               .format(output))
        return list(sessions)

    @property
    def work_dir(self):
        return self._work_dir

    def __getstate__(self):
        dct = copy(self.__dict__)
        # Delete the NiPype plugin as it can be regenerated
        del dct['_plugin']
        return dct

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._init_plugin()
