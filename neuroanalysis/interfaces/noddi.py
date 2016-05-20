import os.path
from nipype.interfaces.base import (
    BaseInterface, File, TraitedSpec, traits)
from nipype.interfaces.matlab import MatlabCommand, MatlabInputSpec


class NODDICreateROIInputSpec(MatlabInputSpec):

    in_file = traits.File(  # @UndefinedVariable
        exists=True, desc="Input diffusion file to create the ROI for",
        argstr='%s', position=0, mandatory=True)

    brain_mask = traits.File(  # @UndefinedVariable
        exists=True, desc="Whole brain mask", argstr='%s', position=1,
        mandatory=True)

    out_filename = traits.Str(  # @UndefinedVariable
        gen_file=True, desc="The name of the output file to be generated")


class NODDICreateROIOutputSpec(TraitedSpec):

    out_file = File(exists=True, desc='ROI for NODDI processing')


class NODDICreateROI(BaseInterface):

    input_spec = NODDICreateROIInputSpec
    output_spec = NODDICreateROIInputSpec

    def _run_interface(self, runtime):  # @UnusedVariable
        script = "CreateROI('{}', '{}', '{}');".format(
            self.inputs.in_file, self.inputs.brain_mask,
            self.inputs.out_filename)
        mlab = MatlabCommand(script=script, mfile=True)
        result = mlab.run()
        return result.runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['out_file'] = os.path.abspath(self.inputs.out_filename)
        return outputs

    def _gen_filename(self, name):
        base, ext = os.path.split(name)
        return base + '_ROI' + ext
