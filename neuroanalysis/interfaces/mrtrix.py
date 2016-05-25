import os.path
from nipype.interfaces.base import (
    CommandLineInputSpec, CommandLine, File, TraitedSpec, isdefined, traits)


# =============================================================================
# Extract MR gradients
# =============================================================================

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


# =============================================================================
# MR Convert
# =============================================================================


class MRConvertInputSpec(CommandLineInputSpec):
    in_file = File(
        mandatory=True, exists=True, argstr='%s', position=-2,
        desc="Input file")
    out_filename = File(
        mandatory=True, argstr='%s', position=-1,
        desc="Output (converted) file")
    coord = traits.Str(  # @UndefinedVariable
        mandatory=False, argstr='-coord %s',
        desc=("extract data from the input image only at the coordinates "
              "specified."))
    vox = traits.Str(  # @UndefinedVariable
        mandatory=False, argstr='-vox %s',
        desc=("change the voxel dimensions of the output image. The new sizes "
              "should be provided as a comma-separated list of values. Only "
              "those values specified will be changed. For example: 1,,3.5 "
              "will change the voxel size along the x & z axes, and leave the "
              "y-axis voxel size unchanged."))
    axes = traits.Str(  # @UndefinedVariable
        mandatory=False, argstr='-axes %s',
        desc=("specify the axes from the input image that will be used to form"
              " the output image. This allows the permutation, ommission, or "
              "addition of axes into the output image. The axes should be "
              "supplied as a comma-separated list of axes. Any ommitted axes "
              "must have dimension 1. Axes can be inserted by supplying -1 at "
              "the corresponding position in the list."))
    scaling = traits.Str(  # @UndefinedVariable
        mandatory=False, argstr='-scaling %s',
        desc=("specify the data scaling parameters used to rescale the "
              "intensity values. These take the form of a comma-separated "
              "2-vector of floating-point values, corresponding to offset & "
              "scale, with final intensity values being given by offset + "
              "scale * stored_value. By default, the values in the input "
              "image header are passed through to the output image header "
              "when writing to an integer image, and reset to 0,1 (no scaling)"
              " for floating-point and binary images. Note that his option has"
              " no effect for floating-point and binary images."))
    stride = traits.Str(  # @UndefinedVariable
        mandatory=False, argstr='-stride %s',
        desc=("specify the strides of the output data in memory, as a "
              "comma-separated list. The actual strides produced will depend "
              "on whether the output image format can support it."))
    dataset = traits.Str(  # @UndefinedVariable
        mandatory=False, argstr='-dataset %s',
        desc=("specify output image data type. Valid choices are: float32, "
              "float32le, float32be, float64, float64le, float64be, int64, "
              "uint64, int64le, uint64le, int64be, uint64be, int32, uint32, "
              "int32le, uint32le, int32be, uint32be, int16, uint16, int16le, "
              "uint16le, int16be, uint16be, cfloat32, cfloat32le, cfloat32be, "
              "cfloat64, cfloat64le, cfloat64be, int8, uint8, bit."))
    grad = traits.Str(  # @UndefinedVariable
        mandatory=False, argstr='-grad %s',
        desc=("specify the diffusion-weighted gradient scheme used in the  "
              "acquisition. The program will normally attempt to use the  "
              "encoding stored in the image header. This should be supplied  "
              "as a 4xN text file with each line is in the format [ X Y Z b ],"
              " where [ X Y Z ] describe the direction of the applied  "
              "gradient, and b gives the b-value in units of s/mm^2."))
    fslgrad = traits.Str(  # @UndefinedVariable
        mandatory=False, argstr='-fslgrad %s',
        desc=("specify the diffusion-weighted gradient scheme used in the "
              "acquisition in FSL bvecs/bvals format."))
    export_grad_mrtrix = traits.Str(  # @UndefinedVariable
        mandatory=False, argstr='-export_grad_mrtrix %s',
        desc=("export the diffusion-weighted gradient table to file in MRtrix "
              "format"))
    export_grad_fsl = traits.Str(  # @UndefinedVariable
        mandatory=False, argstr='-export_grad_fsl %s',
        desc=("export the diffusion-weighted gradient table to files in FSL "
              "(bvecs / bvals) format"))


class MRConvertOutputSpec(TraitedSpec):
    out_file = File(exists=True, desc='Extracted encoding gradients')


class MRConvert(CommandLine):

    _cmd = 'mrconvert'
    input_spec = MRConvertInputSpec
    output_spec = MRConvertOutputSpec

    def _list_outputs(self):
        outputs = self.output_spec().get()
        outputs['out_file'] = self.inputs.out_filename
        return outputs


class DWIPreprocInputSpec(CommandLineInputSpec):
    # Arguments
    pe_dir = traits.Str(  # @UndefinedVariable
        mandatory=True, argstr='%s', desc=(
            "The phase encode direction; can be a signed axis "
            "number (e.g. -0, 1, +2) or a code (e.g. AP, LR, IS)"),
        position=-3)
    in_file = traits.Str(  # @UndefinedVariable
        mandatory=True, argstr='%s',
        desc=("The input DWI series to be corrected"), position=-2)
    out_file = File(
        genfile=True, argstr='%s', position=-1, hash_files=False,
        desc="Output preprocessed filename")
    forward_rpe = traits.Str(  # @UndefinedVariable
        argstr='-rpe_pair %s',
        desc=("forward reverse Provide a pair of images to use for "
              "inhomogeneity field estimation; note that the FIRST of these "
              "two images must have the same phase"),
        position=0)
    reverse_rpe = traits.Str(  # @UndefinedVariable
        argstr=' %s',
        desc=("forward reverse Provide a pair of images to use for "
              "inhomogeneity field estimation; note that the FIRST of these "
              "two images must have the same phase"),
        position=1)


class DWIPreprocOutputSpec(TraitedSpec):
    out_file = File(exists=True, desc='Pre-processed DWI dataset')


class DWIPreproc(CommandLine):

    _cmd = 'dwipreproc'
    input_spec = DWIPreprocInputSpec
    output_spec = DWIPreprocOutputSpec

    def _list_outputs(self):
        outputs = self.output_spec().get()
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
                os.getcwd(), "{}_preprocessed{}".format(base, ext))
        return out_name


class MRCatInputSpec(CommandLineInputSpec):

    first_scan = traits.File(  # @UndefinedVariable
        exists=True, mandatory=True, desc="First input image", argstr="%s",
        position=-3)

    second_scan = traits.File(  # @UndefinedVariable
        exists=True, mandatory=True, desc="Second input image", argstr="%s",
        position=-2)

    out_file = traits.File(  # @UndefinedVariable
        genfile=True, desc="Output filename", position=-1, hash_files=False,
        argstr="%s")

    axis = traits.Str(  # @UndefinedVariable
        desc="The axis along which the scans will be concatenated",
        argstr="-axis %s")


class MRCatOutputSpec(TraitedSpec):

    out_file = File(exists=True, desc='Pre-processed DWI dataset')


class MRCat(CommandLine):

    _cmd = 'mrcat'
    input_spec = MRCatInputSpec
    output_spec = MRCatOutputSpec

    def _list_outputs(self):
        outputs = self.output_spec().get()
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
            first, ext = os.path.splitext(
                os.path.basename(self.inputs.first_scan))
            second, _ = os.path.splitext(
                os.path.basename(self.inputs.second_scan))
            out_name = os.path.join(
                os.getcwd(), "{}_{}_concatenated{}".format(first, second, ext))
        return out_name
