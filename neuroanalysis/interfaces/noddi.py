import os.path
from nipype.interfaces.base import (
    BaseInterface, File, TraitedSpec, traits, isdefined,
    BaseInterfaceInputSpec)
from nipype.interfaces.matlab import MatlabCommand
from neuroanalysis.exception import NeuroAnalysisError


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
            if not self.inputs.out_file.endswith('.mat'):
                raise NeuroAnalysisError(
                    "Output NODDI ROI should be saved with '.mat' extension "
                    "(provided '{}')".format(self.inputs.out_file))
            out_name = self.inputs.out_file
        else:
            base, _ = os.path.splitext(os.path.basename(self.inputs.in_file))
            out_name = os.path.join(os.getcwd(), "{}_ROI.mat".format(base))
        return out_name


class BatchNODDIFittingInputSpec(BaseInterfaceInputSpec):

    roi_file = traits.File(  # @UndefinedVariable
        exists=True, mandatory=True,
        desc="Input ROI to fit the parameters for")

    bvecs_file = File(
        exists=True, mandatory=True,
        desc=("Gradient encoding directions in FSL format"))

    bvals_file = File(
        exists=True, mandatory=True,
        desc="Extracted graident encoding b-values in FSL format")

    model = traits.Str(  # @UndefinedVariable
        'WatsonSHStickTortIsoV_B0', mandatory=False, usedefault=True,
        desc="The NODDI model used (see NODDI docs for alternatives)")

    nthreads = traits.Int(  # @UndefinedVariable
        4, mandatory=False, usedefault=True,
        desc="The number of compute cores to use for the fitting process")

    out_file = traits.File(  # @UndefinedVariable
        genfile=True, hash_files=False,
        desc="The name of the ROI file to be generated")


class BatchNODDIFittingOutputSpec(TraitedSpec):

    out_file = File(exists=True, desc='ROI for NODDI processing')


class BatchNODDIFitting(BaseInterface):

    input_spec = BatchNODDIFittingInputSpec
    output_spec = BatchNODDIFittingOutputSpec

    def _run_interface(self, runtime):  # @UnusedVariable
        script = """
        protocol = FSL2Protocol('{bvals}', '{bvecs}');
        noddi = MakeModel('{model}');
        batch_fitting('{roi}', protocol, noddi, '{out_file}', {nthreads});
        """.format(
            bvecs=self.inputs.bvecs_file, bvals=self.inputs.bvals_file,
            model=self.inputs.model, roi=self.inputs.roi_file,
            out_file=self._gen_outfilename(), nthreads=self.inputs.nthreads)
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
            base, _ = os.path.splitext(os.path.basename(self.inputs.roi_file))
            out_name = os.path.join(
                os.getcwd(), "{}_fitted_params.mat".format(base))
        return out_name


class SaveParamsAsNIfTIInputSpec(BaseInterfaceInputSpec):

    params_file = File(
        exists=True, mandatory=True,
        desc="The parameters fitted by BatchNODDIFitting")

    roi_file = File(
        exists=True, mandatory=True, desc="The ROI file created by CreateROI")

    brain_mask_file = File(
        exists=True, mandatory=True, desc="A whole brain mask")

    output_prefix = traits.Str(  # @UndefinedVariable
        "processed_noddi", mandatory=False,
        desc="Prefix of the generated output files")


class SaveParamsAsNIfTIOutputSpec(TraitedSpec):

    ficvf = File(
        exists=True,
        desc="Neurite density (or intra-cellular volume fraction)")

    odi = File(exists=True, desc="Orientation dispersion index (ODI)")

    fiso = File(exists=True, desc="CSF volume fraction")

    fibredirs_xvec = File(exists=True, desc="X fibre orientation")

    fibredirs_yvec = File(exists=True, desc="Y fibre orientation")

    fibredirs_zvec = File(exists=True, desc="Z fibre orientation")

    fmin = File(exists=True, desc="Fitting objective function values")

    kappa = File(
        exists=True,
        desc=("Concentration parameter of Watson distribution used to "
              "compute ODI"))

    error_code = File(
        exists=True,
        desc="Error code (NEW) Nonzero values indicate fitting errors")


class SaveParamsAsNIfTI(BaseInterface):

    input_spec = SaveParamsAsNIfTIInputSpec
    output_spec = SaveParamsAsNIfTIOutputSpec

    def _run_interface(self, runtime):  # @UnusedVariable
        script = """
        SaveParamsAsNIfTI('{params}', '{roi}', '{brain_mask}', '{prefix}');
        """.format(
            params=self.inputs.params_file, roi=self.inputs.roi_file,
            brain_mask=self.inputs.brain_mask_file,
            prefix=self.inputs.output_prefix)
        mlab = MatlabCommand(script=script, mfile=True)
        result = mlab.run()
        return result.runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        for name in ('ficvf', 'odi', 'fiso', 'fibredirs_xvec',
                     'fibredirs_yvec', 'fibredirs_zvec', 'fmin', 'kappa',
                     'error_code'):
            outputs[name] = os.path.join(
                os.getcwd(), '{}_{}.nii'.format(self.inputs.output_prefix,
                                                name))
        return outputs
