from abc import ABCMeta
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface


class Dataset(object):

    __metaclass__ = ABCMeta

    def __init__(self, project, dataset_names, path, scratch, ris='daris'):
        """
        project_name -- The name of the project. For DaRIS it is the project
                         id minus the proceeding 1008.2. For XNAT it will be
                         the project code. For local files it is the full path
                         to the directory.
        scan_names   -- A dict containing the a mapping between names of
                        sub-datasets and the acquired scans in the RIS, e.g.
                        {'diffusion':'ep2d_diff_mrtrix_33_dir_3_inter_b0_p_RL',
                         'distortion_correct':
                           'PRE DWI L-R DIST CORR 36 DIR MrTrix'}
        project_path -- The base directory where the scans will be downloaded
                        to and generated files will be saved
        scratch_path -- The directory used for creating temporary working files
        source       -- Can be one of 'daris', 'xnat', or 'local'
                        (NB: Only 'daris' is currently implemented)
        """
        self._project = project
        self._scan_names = dataset_names
        self._path = path
        self._scrach = scratch
        self._ris = ris


class Pipeline(object):
    """
    Basically a wrapper around a NiPype workflow to keep track of the inputs
    and outputs a little better and provide some convenience functions related
    to the Dataset objects.
    """

    def __init__(self, name, dataset, workflow, outputs):
        self._name = name
        self._dataset = dataset
        self._workflow = workflow
        self._outputs = outputs

    def run(self, subject_ids=None):
        """
        Gets a data grabber for the requested subject_ids and a data sink from
        the dataset the pipeline belongs to and then combines them together
        with the wrapped workflow and runs the pipeline
        """
        complete_workflow = pe.Workflow(name=self._name)
        if subject_ids is None:
            subject_ids = self._dataset.all_subjects
        data_source = self._dataset.ris.source()
        data_sink = self._dataset.ris.sink()
        complete_workflow.add_nodes((data_source, self._workflow, data_sink))
        for inpt in self._dataset.inputs:
            complete_workflow.connect(data_source, inpt,
                                      self._workflow.inputnode, inpt)
        for output in self._outputs:
            complete_workflow.connect(self._workflow.outputnode, output,
                                      data_sink, output)
        complete_workflow.run()
