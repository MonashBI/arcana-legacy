import os.path
from nipype.interfaces.base import (
    CommandLineInputSpec, CommandLine, File, TraitedSpec, isdefined)


class ExtractMRtrixGradientsInputSpec(CommandLineInputSpec):
    in_file = File(exists=True, argstr='%s', mandatory=True, position=-2,
                   desc="Diffusion weighted images with graident info")
    out_filename = File(genfile=True, argstr='-grad %s', position=-1,
                        desc="Extracted gradient encodings filename")


class ExtractMRtrixGradientsOutputSpec(TraitedSpec):
    out_file = File(exists=True, desc='Extracted encoding gradients')


class ExtractMRtrixGradients(CommandLine):
    """
    Extracts the gradient information in MRtrix format from a DWI image
    """
    _cmd = 'mrinfo'
    input_spec = ExtractMRtrixGradientsInputSpec
    output_spec = ExtractMRtrixGradientsOutputSpec

    def _list_outputs(self):
        outputs = self.output_spec().get()
        outputs['out_file'] = self.inputs.out_filename
        if not isdefined(outputs['out_file']):
            outputs['out_file'] = os.path.abspath(self._gen_outfilename())
        else:
            outputs['out_file'] = os.path.abspath(outputs['out_file'])
        return outputs

    def _gen_filename(self, name):
        if name is 'out_filename':
            return self._gen_outfilename()
        else:
            return None

    def _gen_outfilename(self):
        in_file = os.path.splitext(os.path.split(self.inputs.in_file)[1])[0]
        return in_file + '.b'
