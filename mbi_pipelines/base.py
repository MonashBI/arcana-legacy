from abc import ABCMeta
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from .ris import Daris


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

    def run_pipeline(self, pipeline, subject_ids=None, study_ids=[1],
                     work_dir=None):
        """
        Gets a data grabber for the requested subject_ids and a data sink from
        the dataset the pipeline belongs to and then combines them together
        with the wrapped workflow and runs the pipeline
        """
        complete_workflow = pe.Workflow(name=self._name, base_dir=work_dir)
        # If subject_ids is none use all associated with the project
        if subject_ids is None:
            subject_ids = self._archive.subject_ids(self._project_id)
        # Generate extra nodes
        inputnode = pe.Node(IdentityInterface(), name='subject_input')
        inputnode.iterables = ('subject_id', tuple(subject_ids),
                               'study_id', tuple(study_ids))
        source = self._archive.source(self._project_id)
        sink = self._dataset.archive_sink(self._project_id)
        complete_workflow.add_nodes(
            (inputnode, source, self._workflow, sink))
        complete_workflow.connect(inputnode, 'subject_id',
                                  source, 'subject_id')
        complete_workflow.connect(inputnode, 'study_id',
                                  source, 'study_id')
        for inpt in pipeline.inputs:
            if inpt in self.acquired_components
            complete_workflow.connect(
                source, self.source_scan(inpt),
                pipeline.workflow.inputnode, inpt)
        for output in pipeline.outputs:
            complete_workflow.connect(pipeline.workflow.outputnode, output,
                                      sink, output)
        complete_workflow.run()
        if name in self.acquired_components:
            return self._scan_names[name]


class Pipeline(object):
    """
    Basically a wrapper around a NiPype workflow to keep track of the inputs
    and outputs a little better and provide some convenience functions related
    to the Dataset objects.
    """

    def __init__(self, name, dataset, workflow, inputs, outputs):
        self._name = name
        self._dataset = dataset
        self._workflow = workflow
        self._inputs = inputs
        self._outputs = outputs

    def run(self, subject_ids=None, study_ids=[1], work_dir=None):
        self._dataset.run_pipeline(self, subject_ids=subject_ids,
                                   study_ids=study_ids, work_dir=work_dir)

    @property
    def workflow(self):
        return self._workflow

    @property
    def inputs(self):
        return self._inputs

    @property
    def outputs(self):
        return self._outputs
