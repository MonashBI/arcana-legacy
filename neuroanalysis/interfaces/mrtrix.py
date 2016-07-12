import os.path
from nipype.interfaces.base import (
    CommandLineInputSpec, CommandLine, File, TraitedSpec, isdefined, traits)
from neuroanalysis.utils import split_extension


# TODO: Write MRtrixBaseInputSpec with all the generic options included

# =============================================================================
# Extract MR gradients
# =============================================================================

class ExtractFSLGradientsInputSpec(CommandLineInputSpec):
    in_file = File(exists=True, argstr='%s', mandatory=True, position=0,
                   desc="Diffusion weighted images with graident info")
    bvecs_file = File(genfile=True, argstr='-export_grad_fsl %s', position=1,
                      desc=("Extracted gradient encoding directions in FSL "
                            "format"))
    bvals_file = File(genfile=True, argstr='%s', position=2,
                      desc=("Extracted graident encoding b-values in FSL "
                            "format"))


class ExtractFSLGradientsOutputSpec(TraitedSpec):
    bvecs_file = File(exists=True,
                      desc='Extracted encoding gradient directions')
    bvals_file = File(exists=True,
                      desc='Extracted encoding gradient b-values')


class ExtractFSLGradients(CommandLine):
    """
    Extracts the gradient information in MRtrix format from a DWI image
    """
    _cmd = 'mrinfo'
    input_spec = ExtractFSLGradientsInputSpec
    output_spec = ExtractFSLGradientsOutputSpec

    def _list_outputs(self):
        outputs = self.output_spec().get()
        outputs['bvecs_file'] = self._gen_grad_filename('bvecs')
        outputs['bvals_file'] = self._gen_grad_filename('bvals')
        return outputs

    def _gen_filename(self, name):
        if name == 'bvecs_file':
            fname = self._gen_grad_filename('bvecs')
        elif name == 'bvals_file':
            fname = self._gen_grad_filename('bvals')
        else:
            assert False
        return fname

    def _gen_grad_filename(self, comp):
        if isdefined(self.inputs.bvecs_file):
            filename = getattr(self.inputs, comp + '_file')
        else:
            base, _ = split_extension(os.path.basename(self.inputs.in_file))
            filename = os.path.join(
                os.getcwd(), "{}_{}".format(base, comp))
        return filename


# =============================================================================
# MR math
# =============================================================================

class MRMathInputSpec(CommandLineInputSpec):
    in_file = File(exists=True, argstr='%s', mandatory=True, position=1,
                   desc="Diffusion weighted images with graident info")

    out_file = File(genfile=True, argstr='%s', position=-1,
                    desc="Extracted DW or b-zero images")

    operator = traits.Str(mandatory=True, argstr='%s', position=-2,  # @UndefinedVariable @IgnorePep8
                          desc=("Operand to apply to the files"))

    axis = traits.Int(mandatory=True, argstr="-axis %s", position=0,  # @UndefinedVariable @IgnorePep8
                      desc=("The axis over which to apply the operator"))

    quiet = traits.Bool(  # @UndefinedVariable
        mandatory=False, argstr="-quiet",
        description="Don't display output during operation")


class MRMathOutputSpec(TraitedSpec):
    out_file = File(exists=True, desc='The resultant image')


class MRMath(CommandLine):
    """
    Extracts the gradient information in MRtrix format from a DWI image
    """
    _cmd = 'mrmath'
    input_spec = MRMathInputSpec
    output_spec = MRMathOutputSpec

    def _list_outputs(self):
        outputs = self.output_spec().get()
        outputs['out_file'] = self._gen_outfilename()
        return outputs

    def _gen_filename(self, name):
        if name == 'out_file':
            fname = self._gen_outfilename()
        else:
            assert False
        return fname

    def _gen_outfilename(self):
        if isdefined(self.inputs.out_file):
            filename = self.inputs.out_file
        else:
            base, ext = split_extension(os.path.basename(self.inputs.in_file))
            filename = os.path.join(
                os.getcwd(), "{}_{}.{}".format(base, self.inputs.operator,
                                               ext))
        return filename


# =============================================================================
# MR Crop
# =============================================================================

class MRCropInputSpec(CommandLineInputSpec):
    in_file = File(exists=True, argstr='%s', mandatory=True, position=-2,
                   desc="Diffusion weighted images with graident info")

    out_file = File(genfile=True, argstr='%s', position=-1,
                    desc="Extracted DW or b-zero images")

    axis = traits.Tuple(  # @UndefinedVariable
        traits.Int(desc="index"),  # @UndefinedVariable
        traits.Int(desc="start"),  # @UndefinedVariable
        traits.Int(desc='end'),  # @UndefinedVariable
        mandatory=False, argstr="-axis %s %s %s", # @UndefinedVariable @IgnorePep8
        desc=("crop the input image in the provided axis"))

    mask = File(mandatory=False, exists=True, argstr="-mask %s",
                desc=("Crop the input image according to the spatial extent of"
                      " a mask image"))

    quiet = traits.Bool(  # @UndefinedVariable
        mandatory=False, argstr="-quiet",
        description="Don't display output during operation")


class MRCropOutputSpec(TraitedSpec):
    out_file = File(exists=True, desc='The resultant image')


class MRCrop(CommandLine):
    """
    Extracts the gradient information in MRtrix format from a DWI image
    """
    _cmd = 'mrcrop'
    input_spec = MRCropInputSpec
    output_spec = MRCropOutputSpec

    def _list_outputs(self):
        outputs = self.output_spec().get()
        outputs['out_file'] = self._gen_outfilename()
        return outputs

    def _gen_filename(self, name):
        if name == 'out_file':
            fname = self._gen_outfilename()
        else:
            assert False
        return fname

    def _gen_outfilename(self):
        if isdefined(self.inputs.out_file):
            filename = self.inputs.out_file
        else:
            base, ext = split_extension(os.path.basename(self.inputs.in_file))
            filename = os.path.join(os.getcwd(),
                                    "{}_crop.{}".format(base, ext))
        return filename


# =============================================================================
# MR Pad
# =============================================================================

class MRPadInputSpec(CommandLineInputSpec):
    in_file = File(exists=True, argstr='%s', mandatory=True, position=-2,
                   desc="Diffusion weighted images with graident info")

    out_file = File(genfile=True, argstr='%s', position=-1,
                    desc="Extracted DW or b-zero images")

    axis = traits.Tuple(  # @UndefinedVariable
        traits.Int(desc="index"),  # @UndefinedVariable
        traits.Int(desc="lower"),  # @UndefinedVariable
        traits.Int(desc='upper'),  # @UndefinedVariable
        mandatory=False, argstr="-axis %s %s %s", # @UndefinedVariable @IgnorePep8
        desc=("Pad the input image along the provided axis (defined by index)."
              "Lower and upper define the number of voxels to add to the lower"
              " and upper bounds of the axis"))

    uniform = File(mandatory=False, exists=True, argstr="-uniform %s",
                   desc=("Pad the input image by a uniform number of voxels on"
                         " all sides (in 3D)"))

    quiet = traits.Bool(  # @UndefinedVariable
        mandatory=False, argstr="-quiet",
        description="Don't display output during operation")


class MRPadOutputSpec(TraitedSpec):
    out_file = File(exists=True, desc='The resultant image')


class MRPad(CommandLine):
    """
    Extracts the gradient information in MRtrix format from a DWI image
    """
    _cmd = 'mrpad'
    input_spec = MRPadInputSpec
    output_spec = MRPadOutputSpec

    def _list_outputs(self):
        outputs = self.output_spec().get()
        outputs['out_file'] = self._gen_outfilename()
        return outputs

    def _gen_filename(self, name):
        if name == 'out_file':
            fname = self._gen_outfilename()
        else:
            assert False
        return fname

    def _gen_outfilename(self):
        if isdefined(self.inputs.out_file):
            filename = self.inputs.out_file
        else:
            base, ext = split_extension(os.path.basename(self.inputs.in_file))
            filename = os.path.join(os.getcwd(),
                                    "{}_pad.{}".format(base, ext))
        return filename


# =============================================================================
# Extract b0 or DW images
# =============================================================================

class ExtractDWIorB0InputSpec(CommandLineInputSpec):
    in_file = File(exists=True, argstr='%s', mandatory=True, position=0,
                   desc="Diffusion weighted images with graident info")

    out_file = File(genfile=True, argstr='%s', position=-1,
                    desc="Extracted DW or b-zero images")

    bzero = traits.Bool(argstr='-bzero', position=1,  # @UndefinedVariable
                        desc="Extract b-zero images instead of DDW images")

    quiet = traits.Bool(  # @UndefinedVariable
        mandatory=False, argstr="-quiet",
        description="Don't display output during operation")

    grad = traits.Str(  # @UndefinedVariable
        mandatory=False, argstr='-grad %s',
        desc=("specify the diffusion-weighted gradient scheme used in the  "
              "acquisition. The program will normally attempt to use the  "
              "encoding stored in the image header. This should be supplied  "
              "as a 4xN text file with each line is in the format [ X Y Z b ],"
              " where [ X Y Z ] describe the direction of the applied  "
              "gradient, and b gives the b-value in units of s/mm^2."))

    fslgrad = traits.Tuple(  # @UndefinedVariable
        File(exists=True, desc="gradient directions file (bvec)"),  # @UndefinedVariable @IgnorePep8
        File(exists=True, desc="b-values (bval)"),  # @UndefinedVariable @IgnorePep8
        argstr='-fslgrad %s %s', mandatory=False,
        desc=("specify the diffusion-weighted gradient scheme used in the "
              "acquisition in FSL bvecs/bvals format."))


class ExtractDWIorB0OutputSpec(TraitedSpec):

    out_file = File(exists=True, desc='Extracted DW or b-zero images')


class ExtractDWIorB0(CommandLine):
    """
    Extracts the gradient information in MRtrix format from a DWI image
    """
    _cmd = 'dwiextract'
    input_spec = ExtractDWIorB0InputSpec
    output_spec = ExtractDWIorB0OutputSpec

    def _list_outputs(self):
        outputs = self.output_spec().get()
        outputs['out_file'] = self._gen_outfilename()
        return outputs

    def _gen_filename(self, name):
        if name == 'out_file':
            fname = self._gen_outfilename()
        else:
            assert False
        return fname

    def _gen_outfilename(self):
        if isdefined(self.inputs.out_file):
            filename = self.inputs.out_file
        else:
            base, ext = split_extension(os.path.basename(self.inputs.in_file))
            if isdefined(self.inputs.bzero):
                suffix = 'b0'
            else:
                suffix = 'dw'
            filename = os.path.join(
                os.getcwd(), "{}_{}.{}".format(base, suffix, ext))
        return filename


# =============================================================================
# MR Convert
# =============================================================================


class MRConvertInputSpec(CommandLineInputSpec):
    in_file = File(
        mandatory=True, exists=True, argstr='%s', position=-2,
        desc="Input file")
    out_file = File(
        genfile=True, argstr='%s', position=-1, hash_files=False,
        desc=("Output (converted) file. If no path separators (i.e. '/' on "
              "*nix) are found in the provided output file then the CWD (when "
              "the workflow is run, i.e. the working directory) will be "
              "prepended to the output path."))
    out_ext = traits.Str(  # @UndefinedVariable
        mandatory=False,
        desc=("The extension (and therefore the file format) to use when the "
              "output file path isn't provided explicitly"))
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
    quiet = traits.Bool(  # @UndefinedVariable
        mandatory=False, argstr="-quiet",
        description="Don't display output during conversion")


class MRConvertOutputSpec(TraitedSpec):
    out_file = File(exists=True, desc='Extracted encoding gradients')


class MRConvert(CommandLine):

    _cmd = 'mrconvert'
    input_spec = MRConvertInputSpec
    output_spec = MRConvertOutputSpec

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
            base, orig_ext = split_extension(
                os.path.basename(self.inputs.in_file))
            ext = (self.inputs.out_ext
                   if isdefined(self.inputs.out_ext) else orig_ext)
            out_name = os.path.join(os.getcwd(),
                                    "{}_conv.{}".format(base, ext))
        return out_name


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
            base, ext = split_extension(
                os.path.basename(self.inputs.in_file))
            out_name = os.path.join(
                os.getcwd(), "{}_preproc.{}".format(base, ext))
        return out_name


class DWI2MaskInputSpec(CommandLineInputSpec):
    # Arguments
    bvalue_scaling = File(
        mandatory=False, argstr='-bvalue_scaling %s',
        desc=("specifies whether the b-values should be scaled by the square "
              "of the corresponding DW gradient norm, as often required for "
              "multi-shell or DSI DW acquisition schemes. The default action "
              "can also be set in the MRtrix config file, under the "
              "BValueScaling entry. Valid choices are yes/no, true/false, "
              "0/1 (default: true)."))
    in_file = traits.Str(  # @UndefinedVariable
        mandatory=True, argstr='%s',
        desc=("The input DWI series to be corrected"), position=-2)
    out_file = File(
        genfile=True, argstr='%s', position=-1, hash_files=False,
        desc="Output preprocessed filename")
    grad = traits.Str(  # @UndefinedVariable
        mandatory=False, argstr='-grad %s',
        desc=("specify the diffusion-weighted gradient scheme used in the  "
              "acquisition. The program will normally attempt to use the  "
              "encoding stored in the image header. This should be supplied  "
              "as a 4xN text file with each line is in the format [ X Y Z b ],"
              " where [ X Y Z ] describe the direction of the applied  "
              "gradient, and b gives the b-value in units of s/mm^2."))
    
    fslgrad = traits.Tuple(  # @UndefinedVariable
        File(exists=True, desc="gradient directions file (bvec)"),  # @UndefinedVariable @IgnorePep8
        File(exists=True, desc="b-values (bval)"),  # @UndefinedVariable @IgnorePep8
        argstr='-fslgrad %s %s', mandatory=False,
        desc=("specify the diffusion-weighted gradient scheme used in the "
              "acquisition in FSL bvecs/bvals format."))


class DWI2MaskOutputSpec(TraitedSpec):
    out_file = File(exists=True, desc='Bias-corrected DWI dataset')


class DWI2Mask(CommandLine):

    _cmd = 'dwi2mask'
    input_spec = DWI2MaskInputSpec
    output_spec = DWI2MaskOutputSpec

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
            base, ext = split_extension(
                os.path.basename(self.inputs.in_file))
            out_name = os.path.join(
                os.getcwd(), "{}_biascorrect.{}".format(base, ext))
        return out_name


class DWIBiasCorrectInputSpec(CommandLineInputSpec):
    # Arguments
    mask = File(
        mandatory=True, argstr='-mask %s',
        desc=("Whole brain mask"))
    method = traits.Str(  # @UndefinedVariable
        mandatory=False, argstr='-%s',
        desc=("Method used to correct for biases (either 'fsl' or 'ants')"))
    bias = File(
        mandatory=False, argstr='-bias %s',
        desc=("Output the estimated bias field"))
    in_file = traits.Str(  # @UndefinedVariable
        mandatory=True, argstr='%s',
        desc=("The input DWI series to be corrected"), position=-2)
    out_file = File(
        genfile=True, argstr='%s', position=-1, hash_files=False,
        desc="Output preprocessed filename")
    grad = traits.Str(  # @UndefinedVariable
        mandatory=False, argstr='-grad %s',
        desc=("specify the diffusion-weighted gradient scheme used in the  "
              "acquisition. The program will normally attempt to use the  "
              "encoding stored in the image header. This should be supplied  "
              "as a 4xN text file with each line is in the format [ X Y Z b ],"
              " where [ X Y Z ] describe the direction of the applied  "
              "gradient, and b gives the b-value in units of s/mm^2."))

    fslgrad = traits.Tuple(  # @UndefinedVariable
        File(exists=True, desc="gradient directions file (bvec)"),  # @UndefinedVariable @IgnorePep8
        File(exists=True, desc="b-values (bval)"),  # @UndefinedVariable @IgnorePep8
        argstr='-fslgrad %s %s', mandatory=False,
        desc=("specify the diffusion-weighted gradient scheme used in the "
              "acquisition in FSL bvecs/bvals format."))


class DWIBiasCorrectOutputSpec(TraitedSpec):
    out_file = File(exists=True, desc='Bias-corrected DWI dataset')


class DWIBiasCorrect(CommandLine):

    _cmd = 'dwibiascorrect'
    input_spec = DWIBiasCorrectInputSpec
    output_spec = DWIBiasCorrectOutputSpec

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
            base, ext = split_extension(  
                os.path.basename(self.inputs.in_file))
            out_name = os.path.join(
                os.getcwd(), "{}_biascorrect.{}".format(base, ext))
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

    quiet = traits.Bool(  # @UndefinedVariable
        mandatory=False, argstr="-quiet",
        description="Don't display output during concatenation")


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
            first, ext = split_extension(
                os.path.basename(self.inputs.first_scan))
            second, _ = split_extension(
                os.path.basename(self.inputs.second_scan))
            out_name = os.path.join(
                os.getcwd(), "{}_{}_concat.{}".format(first, second, ext))
        return out_name
