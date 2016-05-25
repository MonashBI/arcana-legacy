import os.path
from nipype.interfaces.base import (
    BaseInterface, File, TraitedSpec, traits, isdefined,
    BaseInterfaceInputSpec)
from nipype.interfaces.matlab import MatlabCommand


class CreateROIInputSpec(BaseInterfaceInputSpec):

    in_file = traits.File(  # @UndefinedVariable
        exists=True, desc="Input diffusion file to create the ROI for",
        argstr='%s', position=0, mandatory=True)

    brain_mask = traits.File(  # @UndefinedVariable
        exists=True, desc="Whole brain mask", argstr='%s', position=1,
        mandatory=True)

    out_file = traits.File(  # @UndefinedVariable
        genfile=True, argstr="%s", hash_files=False, position=2,
        desc="The name of the ROI file to be generated")


class CreateROIOutputSpec(TraitedSpec):

    out_file = File(exists=True, desc='ROI for NODDI processing')


class CreateROI(BaseInterface):

    input_spec = CreateROIInputSpec
    output_spec = CreateROIInputSpec

    def _run_interface(self, runtime):  # @UnusedVariable
        script = "CreateROI('{}', '{}', '{}');".format(
            self.inputs.in_file, self.inputs.brain_mask,
            self._gen_outfilename())
        mlab = MatlabCommand(script=script, mfile=True)
        result = mlab.run()
        return result.runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['out_file'] = self._gen_outfilename()
        return outputs

    def _gen_filename(self, name):
        if name == 'out_file':
            gen_name = self._gen_outfilename()
        else:
            assert False
        return gen_name

    def _gen_outfilename(self):
        if isdefined(self.inputs.out_file):
            out_name = self.inputs.out_file
        else:
            base, ext = os.path.splitext(
                os.path.basename(self.inputs.in_file))
            out_name = os.path.join(
                os.getcwd(), "{}_ROI{}".format(base, ext))
        return out_name
