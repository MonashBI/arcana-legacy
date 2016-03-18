import os.path
from copy import copy
from nipype.interfaces.io import DataSink
from nipype.pipeline import engine as pe


class MRIDataset(object):

    def __init__(self, data_accessor):
        self._data_accessor = data_accessor

    def run_workflow(self, workflow_name, inputs_dct, output_dir=None,
                     **kwargs):
        """
        Process the specified workflow, mapping the inputs dictionary to the
        inputnode of the worfklow and copying results to the output_dir

        Inputs:
            * workflow_name - the name of the workflow to run
            * inputs_dct    - a dictionary containing all the inputs required
                              by the workflow
            * output_dir    - the path to the output directory to write the
                              outputs
            * kwargs        - all kwargs passed to the workflow constructor
        """
        if output_dir is None:
            output_dir = self._output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        workflow, _, outputs = getattr(self, workflow_name + '_workflow')(
            **kwargs)
        for input_name, inpt in inputs_dct.iteritems():
            setattr(workflow.inputnode.inputs, input_name, inpt)
        overall = pe.Workflow(name='overall')
        datasink = pe.Node(DataSink(), name='datasink')
        datasink.inputs.base_directory = output_dir
        overall.add_nodes((workflow, datasink))
        for i, output in enumerate(outputs):
            overall.connect(workflow.outputnode, output, datasink,
                            workflow_name + '.@{}'.format(i))
        overall.run()

    def process_subject(self, workflow_name, subject_id, **input_kwargs):
        scan_path = self._data_accessor.get_scan(subject_id, self._scan_name)
        inputs_dict = copy(input_kwargs)
        inputs_dict['image'] = scan_path
        self.run_workflow(workflow_name, inputs_dict)
