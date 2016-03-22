import os.path
from copy import copy
from nipype.interfaces.io import DataSink
from nipype.pipeline import engine as pe


class Dataset(object):

    def __init__(self, project_name, scan_names, cache_dir, scratch_dir,
                 source='daris'):
        """
        project_name  -- The name of the project. For DaRIS it is the project
                         id minus the proceeding 1008.2. For XNAT it will be
                         the project code. For local files it is the full path
                         to the directory.
        scan_names  -- The names of the scans to include in the dataset e.g.
                      'Diffusion.nii.gz'
        cache_dir   -- The base directory where the scans will be downloaded to
        scratch_dir -- The directory where the processed data will be created
        source      -- Can be one of 'daris', 'xnat', or 'local'
                       (NB: Only 'daris' is currently implemented)
        """
        self._project_name = project_name
        self._scan_names = scan_names
        self._cache_dir = cache_dir
        self._scratch_dir = scratch_dir
        self._source = source

    def data_grabber(self, subject_ids):
        raise NotImplementedError

    def data_sink(self, pipeline_name, outputs):
        raise NotImplementedError


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
        data_grabber = self._dataset.data_grabber(subject_ids)
        data_sink = self._dataset.data_sink(self._name, self._outputs)
        complete_workflow.add_nodes((data_grabber, self._workflow, data_sink))
        for inpt in self._dataset.inputs:
            complete_workflow.connect(data_grabber, inpt,
                                      self._workflow.inputnode, inpt)
        for output in self._outputs:
            complete_workflow.connect(self._workflow.outputnode, output,
                                      data_sink, output)
        complete_workflow.run()
