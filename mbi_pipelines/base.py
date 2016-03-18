import os.path
from nipype.interfaces.io import DataSink
from nipype.pipeline import engine as pe


class MRIDataset(object):

    def __init__(self, data_accessor):
        self._data_accessor = data_accessor

    def run_workflow(self, workflow_name, inputs_dct, output_dir, **kwargs):
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
        for output in outputs:
            overall.connect(workflow.outputnode, output, datasink, 'output')
        overall.run()
