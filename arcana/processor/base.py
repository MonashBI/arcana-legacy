from builtins import object
from past.builtins import basestring
import os.path as op
import shutil
from copy import copy, deepcopy
from logging import getLogger
import numpy as np
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface, Merge
from arcana.exception import (
    ArcanaError, ArcanaMissingDataException,
    ArcanaNoRunRequiredException, ArcanaUsageError, ArcanaDesignError,
    ArcanaProvenanceRecordMismatchError)
from arcana.data import BaseFileset
from arcana.utils import PATH_SUFFIX, FIELD_SUFFIX
from arcana.interfaces.repository import (RepositorySource,
                                          RepositorySink)


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
    clean_work_dir_between_runs : bool
        Whether to clean the working directory between runs (can avoid problems
        if debugging the analysis but may take longer to reach the same point)
    """

    DEFAULT_WALL_TIME = 20
    DEFAULT_MEM_GB = 4096

    default_plugin_args = {}

    def __init__(self, work_dir, max_process_time=None, reprocess=False,
                 clean_work_dir_between_runs=True,
                 default_wall_time=DEFAULT_WALL_TIME,
                 default_mem_gb=DEFAULT_MEM_GB, **kwargs):
        self._work_dir = work_dir
        self._max_process_time = max_process_time
        self._reprocess = reprocess
        self._plugin_args = copy(self.default_plugin_args)
        self._default_wall_time = default_wall_time
        self._deffault_mem_gb = default_mem_gb
        self._plugin_args.update(kwargs)
        self._init_plugin()
        self._study = None
        self._clean_work_dir_between_runs = clean_work_dir_between_runs

    def __repr__(self):
        return "{}(work_dir={})".format(
            type(self).__name__, self._work_dir)

    def __eq__(self, other):
        try:
            return (
                self._work_dir == other._work_dir and
                self._max_process_time == other._max_process_time and
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
    def default_mem_gb(self):
        return self._deffault_mem_gb

    @property
    def default_wall_time(self):
        return self._default_wall_time

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
        subject_ids : list[str]
            The subset of subject IDs to process. If None all available will be
            processed. Note this is not a duplication of the study
            and visit IDs passed to the Study __init__, as they define the
            scope of the analysis and these simply limit the scope of the
            current run (e.g. to break the analysis into smaller chunks and
            run separately). Therefore, if the analysis joins over subjects,
            then all subjects will be processed and this parameter will be
            ignored.
        visit_ids : list[str]
            The same as 'subject_ids' but for visit IDs
        session_ids : list[str,str]
            The same as 'subject_ids' and 'visit_ids', except specifies a set
            of specific combinations in tuples of (subject ID, visit ID).
        force : bool | 'all'
            A flag to force the reprocessing of all sessions in the filter
            array, regardless of whether the parameters|pipeline used
            to generate them matches the current ones. NB: if True only the
            final pipeline will be reprocessed (prerequisite pipelines won't
            run unless they don't match provenance). To process all
            prerequisite pipelines 'all' should be passed to force.

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
        # Get filter kwargs  (NB: in Python 3 they could be in the arg list)
        subject_ids = kwargs.pop('subject_ids', [])
        visit_ids = kwargs.pop('visit_ids', [])
        session_ids = kwargs.pop('session_ids', [])
        clean_work_dir = kwargs.pop('clean_work_dir',
                                    self._clean_work_dir_between_runs)
        # Create name by combining pipelines
        name = '_'.join(p.name for p in pipelines)
        # Clean work dir if required
        if clean_work_dir:
            workflow_work_dir = op.join(self.work_dir, name)
            if op.exists(workflow_work_dir):
                shutil.rmtree(workflow_work_dir)
        # Trim the end of very large names to avoid problems with
        # workflow names exceeding system limits.
        name = name[:WORKFLOW_MAX_NAME_LEN]
        workflow = pe.Workflow(name=name, base_dir=self.work_dir)
        already_connected = {}
        # Generate filter array to optionally restrict the run to certain
        # subject and visit IDs.
        tree = self.study.tree
        # Create maps from the subject|visit IDs to an index used to represent
        # them in the filter array
        subject_inds = {s.id: i for i, s in enumerate(tree.subjects)}
        visit_inds = {v.id: i for i, v in enumerate(tree.visits)}
        if not subject_ids and not visit_ids and not session_ids:
            # No filters applied so create a full filter array
            filter_array = np.ones((len(subject_inds), len(visit_inds)),
                                   dtype=bool)
        else:
            # Filters applied so create an empty filter array and populate
            # from filter lists
            filter_array = np.zeros((len(subject_inds), len(visit_inds)),
                                    dtype=bool)
            for subj_id in subject_ids:
                filter_array[subject_inds[subj_id], :] = True
            for visit_id in visit_ids:
                filter_array[:, visit_inds[visit_id]] = True
            for subj_id, visit_id in session_ids:
                filter_array[subject_inds[subj_id],
                             visit_inds[visit_id]] = True
            if not filter_array.any():
                raise ArcanaUsageError(
                    "Provided filters:\n" +
                    ("  subject_ids: {}\n".format(', '.join(subject_ids))
                     if subject_ids is not None else '') +
                    ("  visit_ids: {}\n".format(', '.join(visit_ids))
                     if visit_ids is not None else '') +
                    ("  session_ids: {}\n".format(', '.join(session_ids))
                     if session_ids is not None else '') +
                    "Did not match any sessions in the project:\n" +
                    "  subject_ids: {}\n".format(', '.join(subject_inds)) +
                    "  visit_ids: {}\n".format(', '.join(visit_inds)))
        for pipeline in pipelines:
            try:
                self._connect_pipeline(pipeline, workflow,
                                       subject_inds, visit_inds, filter_array,
                                       already_connected=already_connected,
                                       **kwargs)
            except ArcanaNoRunRequiredException:
                logger.info("Not running '{}' pipeline as its outputs "
                            "are already present in the repository"
                            .format(pipeline.name))
        # Reset the cached tree of filesets in the repository as it will
        # change after the pipeline has run.
        self.study.clear_binds()
#         workflow.write_graph(graph2use='flat', format='svg')
#         print('Graph saved in {} directory'.format(os.getcwd()))
        return workflow.run(plugin=self._plugin)

    def _connect_pipeline(self, pipeline, workflow, subject_inds, visit_inds,
                          filter_array, already_connected=None, force=False):
        """
        Connects a pipeline to a overarching workflow that sets up iterators
        over subjects|visits present in the repository (if required) and
        repository source and sink nodes

        Parameters
        ----------
        pipeline : Pipeline
            The pipeline to connect
        workflow : nipype.pipeline.engine.Workflow
            The overarching workflow to connect the pipeline to
        subject_inds : dct[str, int]
            A mapping of subject ID to row index in the filter array
        visit_inds : dct[str, int]
            A mapping of visit ID to column index in the filter array
        filter_array : 2-D numpy.array[bool]
            A two-dimensional boolean array, where rows correspond to
            subjects and columns correspond to visits in the repository. True
            values represent a combination of subject & visit ID to include
            in the current round of processing. Note that if the 'force'
            flag is not set, sessions won't be reprocessed unless the
            save provenance doesn't match that of the given pipeline.
        already_connected : dict[str, Pipeline]
            A dictionary containing all pipelines that have already been
            connected to avoid the same pipeline being connected twice.
        force : bool
            A flag to force the processing of all sessions in the filter
            array, regardless of whether the parameters|pipeline used
            to generate existing data matches the given pipeline
        """
        if already_connected is None:
            already_connected = {}
        try:
            prev_connected, final = already_connected[pipeline.name]
        except KeyError:
            # Pipeline hasn't been connected already, continue to connect
            # the pipeline to repository
            pass
        else:
            if prev_connected == pipeline:
                return final
            else:
                raise ArcanaError(
                    "Name clash between {} and {} non-matching "
                    "prerequisite pipelines".format(prev_connected, pipeline))
        # Get list of sessions that need to be processed (i.e. if
        # they don't contain the outputs of this pipeline)
        to_process = self._to_process(pipeline, filter_array, subject_inds,
                                      visit_inds, force)
        # Set up workflow to run the pipeline, loading and saving from the
        # repository
        workflow.add_nodes([pipeline._workflow])
        # Prepend prerequisite pipelines to complete workflow if required
        final_nodes = []
        for prereq in pipeline.prerequisites:
            # NB: Even if reprocess==True, the prerequisite pipelines
            # are not re-processed, they are only reprocessed if
            # reprocess == 'all'
            try:
                final_nodes.append(self._connect_pipeline(
                    prereq, workflow, subject_inds, visit_inds,
                    filter_array=to_process,
                    already_connected=already_connected,
                    force=(force if force == 'all' else False)))
            except ArcanaNoRunRequiredException:
                logger.info(
                    "Not running '{}' pipeline as a "
                    "prerequisite of '{}' as the required "
                    "outputs are already present in the repository"
                    .format(prereq.name, pipeline.name))
            except ArcanaMissingDataException as e:
                raise ArcanaMissingDataException(
                    "{},\n which in turn is required as an input of the '{}' "
                    "pipeline to produce '{}'"
                    .format(e, pipeline.name,
                            "', '".join(pipeline.required_outputs)))
        # If prerequisite pipelines need to be processed, connect their
        # "final" nodes to the initial node of this pipeline to ensure that
        # they are all processed before this pipeline is run.
        if final_nodes:
            prereqs = pipeline.add('prereqs', Merge(len(final_nodes)))
            for i, final_node in enumerate(final_nodes, start=1):
                workflow.connect(final_node, 'out', prereqs, 'in{}'.format(i))
        else:
            prereqs = None
        # Construct iterator structure over subjects and sessions to be
        # processed
        iterators = self._iterate(pipeline, to_process, subject_inds,
                                  visit_inds)
        # Loop through each frequency present in the pipeline inputs and
        # create a corresponding source node
        for freq in pipeline.input_frequencies:
            inputs = list(pipeline.frequency_inputs(freq))
            inputnode = pipeline.inputnode(freq)
            try:
                source = pipeline.add(
                    '{}_source'.format(freq),
                    RepositorySource(
                        self.study.spec(i).collection for i in inputs))
            except ArcanaMissingDataException as e:
                raise ArcanaMissingDataException(
                    str(e) + ", which is required for pipeline '{}'".format(
                        pipeline.name))
            # Connect source node to initial node of pipeline to ensure
            # they are run after any prerequisites
            if prereqs is not None:
                workflow.connect(prereqs, 'out', source, 'prereqs')
            # Connect iterators to source and input nodes
            for iterfield in pipeline.iterfields(freq):
                workflow.connect(iterators[iterfield], iterfield, source,
                                 iterfield)
                if freq in ('per_subject', 'per_visit'):
                    workflow.connect(iterators[iterfield], iterfield,
                                     inputnode, iterfield)
            for input in inputs:  # @ReservedAssignment
                in_name = input.name + (
                    PATH_SUFFIX if isinstance(input, BaseFileset) else
                    FIELD_SUFFIX)
                workflow.connect(source, in_name, inputnode, input.name)
        deiterators = {}

        def deiterator_sort_key(ifield):
            """
            If there are two iterators (i.e. both subject and visit ID) and
            one depends on the other (i.e. if the visit IDs per subject
            vary and vice-versa) we need to ensure that the dependent
            iterator is deiterated (joined) first.
            """
            return iterators[ifield].itersource is None

        # Connect all outputs to the repository sink, creating a new sink for
        # each frequency level (i.e 'per_session', 'per_subject', 'per_visit',
        # or 'per_study')
        for freq in pipeline.output_frequencies:
            outputs = list(pipeline.frequency_outputs(freq))
            if pipeline.iterfields(freq) - pipeline.iterfields():
                raise ArcanaDesignError(
                    "Doesn't make sense to output '{}', which are of '{}' "
                    "frequency, when the pipeline only iterates over '{}'"
                    .format("', '".join(o.name for o in outputs), freq,
                            "', '".join(pipeline.iterfields())))
            outputnode = pipeline.outputnode(freq)
            sink = pipeline.add(
                '{}_sink'.format(freq),
                RepositorySink(self.study.spec(o).collection for o in outputs))
            for iterfield in pipeline.iterfields():
                workflow.connect(iterators[iterfield], iterfield, sink,
                                 iterfield)
            for output in outputs:
                if output.is_spec:  # Skip outputs that are study inputs
                    out_name = output.name + (
                        PATH_SUFFIX if isinstance(output, BaseFileset) else
                        FIELD_SUFFIX)
                    workflow.connect(outputnode, output.name, sink, out_name)
            # Join over iterated fields to get back to single child node
            # by the time we connect to the final node of the pipeline

            # Set the sink and subject_id as the default deiterator if there
            # are no deiterates (i.e. per_study) or to use as the upstream
            # node to connect the first deiterator for every frequency
            deiterators[freq] = sink
            for iterfield in sorted(pipeline.iterfields(freq),
                                    key=deiterator_sort_key):
                deiterator = pipeline.add(
                    '{}_{}_deiterator'.format(freq, iterfield),
                    IdentityInterface(['combined']),
                    joinsource=iterfield, joinfield='combined')
                # Connect to previous deiterator or sink
                upstream = deiterators[freq]
                pipeline.connect(upstream, 'combined', deiterator,
                                 'combined')
                deiterators[freq] = deiterator
        # Create a final node, which is used to connect with dependent
        # pipelines into large workflows
        final = pipeline.add('final', Merge(len(deiterators)))
        for i, deiterator in enumerate(deiterators.values(), start=1):
            # Connect the output summary of the prerequisite to the
            # pipeline to ensure that the prerequisite is run first.
            workflow.connect(deiterator, 'combined', final, 'in{}'.format(i))
        # Register pipeline as being connected to prevent duplicates
        already_connected[pipeline.name] = (pipeline, final)
        return final

    def _iterate(self, pipeline, to_process, subject_inds, visit_inds):
        """
        Generate nodes that iterate over subjects and visits in the study that
        need to be processed by the pipeline

        Parameters
        ----------
        pipeline : Pipeline
            The pipeline to add iterators for
        to_process : 2-D numpy.array[bool]
            A two-dimensional boolean array, where rows correspond to
            subjects and columns correspond to visits in the repository. True
            values represent a combination of subject & visit ID to process
            the session for
        subject_inds : dct[str, int]
            A mapping of subject ID to row index in the 'to_process' array
        visit_inds : dct[str, int]
            A mapping of visit ID to column index in the 'to_process' array

        Returns
        -------
        iterators : dict[str, Node]
            A dictionary containing the iterators required for the pipeline
            process all sessions that need processing.
        """
        # Check to see whether the subject/visit IDs to process (as specified
        # by the 'to_process' array) can be factorized into indepdent nodes,
        # i.e. all subjects to process have the same visits to process and
        # vice-versa.
        factorizable = True
        if len(list(pipeline.iterfields())) == 2:
            nz_rows = to_process[to_process.any(axis=1), :]
            ref_row = nz_rows[0, :]
            factorizable = all((r == ref_row).all() for r in nz_rows)
        # If the subject/visit IDs to process cannot be factorized into
        # indepdent iterators, determine which to make make dependent on the
        # other in order to avoid/minimise duplicatation of download attempts
        dependent = None
        if not factorizable:
            input_freqs = list(pipeline.input_frequencies)
            # By default pick iterator the one with the most IDs to
            # iterate to be the dependent in order to reduce the number of
            # nodes created and any duplication of download attempts across
            # the nodes (if both 'per_visit' and 'per_subject' inputs are
            # required
            num_subjs, num_visits = nz_rows[:, nz_rows.any(axis=0)].shape
            if num_subjs > num_visits:
                dependent = self.study.SUBJECT_ID
            else:
                dependent = self.study.VISIT_ID
            if 'per_visit' in input_freqs:
                if 'per_subject' in input_freqs:
                    logger.warning(
                        "Cannot factorize sessions to process into independent"
                        " subject and visit iterators and both 'per_visit' and"
                        " 'per_subject' inputs are used by pipeline therefore"
                        " per_{} inputs may be cached twice".format(
                            dependent[:-1]))
                else:
                    dependent = self.study.SUBJECT_ID
            elif 'per_subject' in input_freqs:
                dependent = self.study.VISIT_ID
        # Invert the index dictionaries to get index-to-ID maps
        inv_subj_inds = {v: k for k, v in subject_inds.items()}
        inv_visit_inds = {v: k for k, v in visit_inds.items()}
        # Create iterator for subjects
        iterators = {}
        if self.study.SUBJECT_ID in pipeline.iterfields():
            fields = [self.study.SUBJECT_ID]
            if dependent == self.study.SUBJECT_ID:
                fields.append(self.study.VISIT_ID)
            # Add iterator node named after subject iterfield
            subj_it = pipeline.add(self.study.SUBJECT_ID,
                                   IdentityInterface(fields))
            if dependent == self.study.SUBJECT_ID:
                # Subjects iterator is dependent on visit iterator (because of
                # non-factorizable IDs)
                subj_it.itersource = ('{}_{}'.format(pipeline.name,
                                                     self.study.VISIT_ID),
                                      self.study.VISIT_ID)
                subj_it.iterables = [(
                    self.study.SUBJECT_ID,
                    {inv_visit_inds[n]: [inv_subj_inds[m]
                                         for m in col.nonzero()[0]]
                     for n, col in enumerate(to_process.T)})]
            else:
                subj_it.iterables = (
                    self.study.SUBJECT_ID,
                    [inv_subj_inds[n]
                     for n in to_process.any(axis=1).nonzero()[0]])
            iterators[self.study.SUBJECT_ID] = subj_it
        # Create iterator for visits
        if self.study.VISIT_ID in pipeline.iterfields():
            fields = [self.study.VISIT_ID]
            if dependent == self.study.VISIT_ID:
                fields.append(self.study.SUBJECT_ID)
            # Add iterator node named after visit iterfield
            visit_it = pipeline.add(self.study.VISIT_ID,
                                    IdentityInterface(fields))
            if dependent == self.study.VISIT_ID:
                visit_it.itersource = ('{}_{}'.format(pipeline.name,
                                                      self.study.SUBJECT_ID),
                                       self.study.SUBJECT_ID)
                visit_it.iterables = [(
                    self.study.VISIT_ID,
                    {inv_subj_inds[m]:[inv_visit_inds[n]
                                       for n in row.nonzero()[0]]
                     for m, row in enumerate(to_process)})]
            else:
                visit_it.iterables = (
                    self.study.VISIT_ID,
                    [inv_visit_inds[n]
                     for n in to_process.any(axis=0).nonzero()[0]])
            iterators[self.study.VISIT_ID] = visit_it
        if dependent == self.study.SUBJECT_ID:
            pipeline.connect(visit_it, self.study.VISIT_ID,
                             subj_it, self.study.VISIT_ID)
        if dependent == self.study.VISIT_ID:
            pipeline.connect(subj_it, self.study.SUBJECT_ID,
                             visit_it, self.study.SUBJECT_ID)
        return iterators

    def _to_process(self, pipeline, filter_array, subject_inds, visit_inds,
                    force):
        """
        Check whether the outputs of the pipeline are present in all sessions
        in the project repository and were generated with matching provenance.
        Return an 2D boolean array (subjects: rows, visits: cols) with the
        sessions to process marked True.

        Parameters
        ----------
        pipeline : Pipeline
            The pipeline to determine the sessions to process
        filter_array : 2-D numpy.array[bool]
            A two-dimensional boolean array, where rows and columns correspond
            correspond to subjects and visits in the repository tree. True
            values represent a subject/visit ID pairs to include
            in the current round of processing. Note that if the 'force'
            flag is not set, sessions won't be reprocessed unless the
            parameters and pipeline version saved in the provenance doesn't
            match that of the given pipeline.
        subject_inds : dict[str,int]
            Mapping from subject ID to index in filter|to_process arrays
        visit_inds : dict[str,int]
            Mapping from visit ID to index in filter|to_process arrays
        force : bool
            Whether to force reprocessing of all (filtered) sessions or not.
            Note that if 'force' is true we can't just return the filter array
            as it might be dilated by summary outputs (i.e. of frequency
            'per_visit', 'per_subject' or 'per_study'). So we still loop
            through all outputs and treat them like they don't exist

        Returns
        -------
        to_process : 2-D numpy.array[bool]
            A two-dimensional boolean array, where rows correspond to
            subjects and columns correspond to visits in the repository. True
            values represent subject/visit ID pairs to run the pipeline for
        """
        # Reference the study tree in local variable for convenience
        tree = self.study.tree
        # Check to see if the pipeline has any low frequency outputs, because
        # if not then each session can be processed indepdently. Otherwise,
        # the "session matrix" (as defined by subject_ids and visit_ids
        # passed to the Study class) needs to be complete, i.e. a session
        # exists (with the full complement of requird inputs) for each
        # subject/visit ID pair.
        summary_outputs = [
            o.name for o in pipeline.outputs if o.frequency != 'per_session']
        # Set of frequencies present in pipeline outputs
        output_freqs = pipeline.output_frequencies
        if summary_outputs:
            if list(tree.incomplete_subjects):
                raise ArcanaUsageError(
                    "Can't process '{}' pipeline as it has low frequency "
                    " outputs (i.e. outputs that aren't of 'per_session' "
                    "frequency) ({}) and subjects ({}) that are missing one "
                    "or more visits ({}). Please restrict the subject/visit "
                    "IDs in the study __init__ to continue the analysis"
                    .format(
                        self.name,
                        ', '.join(summary_outputs),
                        ', '.join(s.id for s in tree.incomplete_subjects),
                        ', '.join(v.id for v in tree.incomplete_visits)))

            def dialate_array(array):
                """
                'Dialates' an array so all subject/visit ID cells required by
                low frequency outputs (i.e. all subjects per-visit for
                'per_visit', all visits per-subject for 'per_subject', all
                for 'per_study') are included in the array if any need for that
                subject/visit need to be processed.
                """
                if array.all() or not array.any():
                    return array
                dialated = np.copy(array)
                if 'per_study' in output_freqs:
                    dialated[:, :] = True
                elif 'per_subject' in output_freqs:
                    dialated[dialated.any(axis=1), :] = True
                elif 'per_visit' in output_freqs:
                    dialated[:, dialated.any(axis=0)] = True
                return dialated

            dialated_filter = dialate_array(filter_array)
            added = dialated_filter ^ filter_array
            if added.any():
                filter_array = dialated_filter
                # Invert the index dictionaries to get index-to-ID maps
                inv_subject_inds = {v: k for k, v in subject_inds.items()}
                inv_visit_inds = {v: k for k, v in visit_inds.items()}
                logger.warning("Dialated filter array used to process '{}' "
                               "pipeline to to include {} subject|visit IDs "
                               "due to its low frequency outputs ({})"
                               .format(pipeline.name),
                               ', '.join(
                                   '{}|{}'.format(i) for i in [
                                       (inv_subject_inds[s], inv_visit_inds[v])
                                       for s, v in zip(*np.nonzero(added))]),
                               ', '.join(str(o) for o in summary_outputs))

        # Initialise the array to return that represents the sessions to
        # process
        to_process = np.zeros((len(subject_inds), len(visit_inds)), dtype=bool)
        # Check for sessions for missing outputs
        for output in pipeline.outputs:
            for item in output.collection:
                if not item.exists or force:
                    # Get row and column indices, if low-frequency then just
                    # mark the first cell in row|column as it will be
                    # "dialated" afterwards (see local method "dialate array")
                    to_process[subject_inds.get(item.subject_id, 0),
                               visit_inds.get(item.visit_id, 0)] = True
        if summary_outputs:
            to_process = dialate_array(to_process)
        # Filter sessions to process by those requested (either explicitly by
        # the user or downstream pipelines)
        to_process *= filter_array
        # Get list of sessions for which all outputs exist, which we should
        # check to see if the saved provenance matches what the given pipeline
        # expects
        outputs_exist = np.invert(to_process) * filter_array
        if outputs_exist.any() and self.study.reprocess != 'ignore':
            # Get list of sessions, subjects, visits, tree objects to check
            # their provenance against that of the pipeline
            to_check = [s for s in tree.sessions
                        if outputs_exist[subject_inds[s.subject_id],
                                         visit_inds[s.visit_id]]]
            if 'per_subject' in output_freqs:
                # We can just test the first col of outputs_exist as rows
                # should be either all True or all False
                to_check.extend(s for s in tree.subjects
                                if outputs_exist[subject_inds[s.id], 0])
            if 'per_visit' in output_freqs:
                # We can just test the first row of outputs_exist as cols
                # should be either all True or all False
                to_check.extend(v for v in tree.visits
                                if outputs_exist[0, visit_inds[v.id]])
            if 'per_study' in output_freqs:
                to_check.append(tree)
            # Parse study reprocess flag to determine whether to ignore
            # software versions when reprocessing
            if (isinstance(self.study.reprocess, basestring) and
                    self.study.reprocess.startswith('ignore_versions')):
                ignore_versions = True
                reprocess = self.study.reprocess.endswith('true')
            else:
                ignore_versions = False
                reprocess = self.study.reprocess
            for node in to_check:
                try:
                    node.check_provenance(pipeline, ignore_versions)
                except ArcanaProvenanceRecordMismatchError:
                    if reprocess:
                        to_process[subject_inds.get(node.subject_id, 0),
                                   visit_inds.get(node.visit_id, 0)] = True
                        logger.info(
                            "Reprocessing {} with '{}' "
                            "pipeline due to mismatching provenance"
                            .format(node, pipeline.name))
                    else:
                        raise

        if summary_outputs:
            to_process = dialate_array(to_process)
        if not to_process.any():
            raise ArcanaNoRunRequiredException(
                "No sessions to process for '{}' pipeline"
                .format(pipeline.name))
        return to_process

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
