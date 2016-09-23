from nipype.pipeline import engine as pe
from nipype.interfaces.mrtrix3.utils import BrainMask, TensorMetrics
from nipype.interfaces.mrtrix3.reconst import FitTensor, EstimateFOD
from nipype.interfaces.mrtrix3.preprocess import ResponseSD
from nianalysis.interfaces.mrtrix import (
    DWIPreproc, MRCat, ExtractDWIorB0, MRMath, DWIBiasCorrect)
from nipype.workflows.dmri.fsl.tbss import create_tbss_all
from nianalysis.interfaces.noddi import (
    CreateROI, BatchNODDIFitting, SaveParamsAsNIfTI)
from .t2 import T2Dataset
from nianalysis.interfaces.mrtrix import MRConvert, ExtractFSLGradients
from nianalysis.interfaces.utils import MergeTuple
from nianalysis.citations import (
    mrtrix_cite, fsl_cite, eddy_cite, topup_cite, distort_correct_cite,
    noddi_cite, fast_cite, n4_cite, tbss_cite)
from nianalysis.formats import (
    mrtrix_format, nifti_gz_format, fsl_bvecs_format, fsl_bvals_format,
    nifti_format)
from nianalysis.requirements import (
    fsl5_req, mrtrix3_req, Requirement, ants2_req)
from nianalysis.exceptions import NiAnalysisError
from .base import _create_component_dict, Scan


class DiffusionDataset(T2Dataset):

    def preprocess_pipeline(self, phase_dir='LR', **kwargs):  # @UnusedVariable @IgnorePep8
        """
        Performs a series of FSL preprocessing steps, including Eddy and Topup

        Parameters
        ----------
        phase_dir : str{AP|LR|IS}
            The phase encode direction
        """
        pipeline = self._create_pipeline(
            name='preprocess',
            inputs=['dwi_scan', 'forward_rpe', 'reverse_rpe'],
            outputs=['dwi_preproc', 'grad_dirs', 'bvalues'],
            description="Preprocess dMRI datasets using distortion correction",
            options={'phase_dir': phase_dir},
            requirements=[mrtrix3_req, fsl5_req],
            citations=[fsl_cite, eddy_cite, topup_cite, distort_correct_cite],
            approx_runtime=30)
        # Create preprocessing node
        dwipreproc = pe.Node(DWIPreproc(), name='dwipreproc')
        dwipreproc.inputs.pe_dir = phase_dir
        # Create nodes to convert preprocessed scan and gradients to FSL format
        mrconvert = pe.Node(MRConvert(), name='mrconvert')
        mrconvert.inputs.out_ext = 'nii.gz'
        mrconvert.inputs.quiet = True
        extract_grad = pe.Node(ExtractFSLGradients(), name="extract_grad")
        pipeline.connect(dwipreproc, 'out_file', mrconvert, 'in_file')
        pipeline.connect(dwipreproc, 'out_file', extract_grad, 'in_file')
        # Connect inputs
        pipeline.connect_input('dwi_scan', dwipreproc, 'in_file')
        pipeline.connect_input('forward_rpe', dwipreproc, 'forward_rpe')
        pipeline.connect_input('reverse_rpe', dwipreproc, 'reverse_rpe')
        # Connect outputs
        pipeline.connect_output('dwi_preproc', mrconvert, 'out_file')
        pipeline.connect_output('grad_dirs', extract_grad,
                                'bvecs_file')
        pipeline.connect_output('bvalues', extract_grad, 'bvals_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    def brain_mask_pipeline(self, mask_tool='bet', **kwargs):  # @UnusedVariable @IgnorePep8
        """
        Generates a whole brain mask using MRtrix's 'dwi2mask' command

        Parameters
        ----------
        mask_tool: Str
            Can be either 'bet' or 'dwi2mask' depending on which mask tool you
            want to use
        """
        if mask_tool == 'fsl':
            pipeline = super(DiffusionDataset, self).brain_mask_pipeline(
                **kwargs)
        elif mask_tool == 'dwi2mask':
            pipeline = self._create_pipeline(
                name='brain_mask',
                inputs=['dwi_preproc', 'grad_dirs', 'bvalues'],
                outputs=['brain_mask'],
                description="Generate brain mask from b0 images",
                options={'mask_tool': mask_tool},
                requirements=[mrtrix3_req],
                citations=[mrtrix_cite], approx_runtime=1)
            # Create mask node
            dwi2mask = pe.Node(BrainMask(), name='dwi2mask')
            dwi2mask.inputs.out_file = 'brain_mask.nii.gz'
            # Gradient merge node
            fsl_grads = pe.Node(MergeTuple(2), name="fsl_grads")
            # Connect nodes
            pipeline.connect(fsl_grads, 'out', dwi2mask, 'fslgrad')
            # Connect inputs
            pipeline.connect_input('grad_dirs', fsl_grads, 'in1')
            pipeline.connect_input('bvalues', fsl_grads, 'in2')
            pipeline.connect_input('dwi_preproc', dwi2mask, 'in_file')
            # Connect outputs
            pipeline.connect_output('brain_mask', dwi2mask, 'out_file')
            # Check inputs/outputs are connected
            pipeline.assert_connected()
        else:
            raise NiAnalysisError(
                "Unrecognised mask_tool '{}' (valid options 'bet' or "
                "'dwi2mask')")
        return pipeline

    def bias_correct_pipeline(self, bias_method='ants', **kwargs):  # @UnusedVariable @IgnorePep8
        """
        Corrects B1 field inhomogeneities
        """
        if bias_method not in ('ants', 'fsl'):
            raise NiAnalysisError(
                "Unrecognised value for 'bias_method' option '{}'. It can be "
                "one of 'ants' or 'fsl'.".format(bias_method))
        pipeline = self._create_pipeline(
            name='bias_correct',
            inputs=['dwi_preproc', 'brain_mask', 'grad_dirs',
                    'bvalues'],
            outputs=['bias_correct'],
            description="Corrects for B1 field inhomogeneity",
            options={'method': bias_method},
            requirements=[mrtrix3_req,
                          (ants2_req if bias_method == 'ants' else fsl5_req)],
            citations=[fast_cite,
                       (n4_cite if bias_method == 'ants' else fsl_cite)],
            approx_runtime=1)
        # Create bias correct node
        bias_correct = pe.Node(DWIBiasCorrect(), name="bias_correct")
        bias_correct.inputs.method = bias_method
        # Gradient merge node
        fsl_grads = pe.Node(MergeTuple(2), name="fsl_grads")
        # Connect nodes
        pipeline.connect(fsl_grads, 'out', bias_correct, 'fslgrad')
        # Connect to inputs
        pipeline.connect_input('grad_dirs', fsl_grads, 'in1')
        pipeline.connect_input('bvalues', fsl_grads, 'in2')
        pipeline.connect_input('dwi_preproc', bias_correct, 'in_file')
        pipeline.connect_input('brain_mask', bias_correct, 'mask')
        # Connect to outputs
        pipeline.connect_output('bias_correct', bias_correct, 'out_file')
        # Check inputs/output are connected
        pipeline.assert_connected()
        return pipeline

    def tensor_pipeline(self, **kwargs):  # @UnusedVariable
        """
        Fits the apparrent diffusion tensor (DT) to each voxel of the image
        """
        pipeline = self._create_pipeline(
            name='tensor',
            inputs=['bias_correct', 'grad_dirs', 'bvalues', 'brain_mask'],
            outputs=['tensor'],
            description=("Estimates the apparrent diffusion tensor in each "
                         "voxel"),
            options={},
            citations=[],
            requirements=[mrtrix3_req],
            approx_runtime=1)
        # Create tensor fit node
        dwi2tensor = pe.Node(FitTensor(), name='dwi2tensor')
        dwi2tensor.inputs.out_file = 'dti.nii.gz'
        # Gradient merge node
        fsl_grads = pe.Node(MergeTuple(2), name="fsl_grads")
        # Connect nodes
        pipeline.connect(fsl_grads, 'out', dwi2tensor, 'grad_fsl')
        # Connect to inputs
        pipeline.connect_input('grad_dirs', fsl_grads, 'in1')
        pipeline.connect_input('bvalues', fsl_grads, 'in2')
        pipeline.connect_input('bias_correct', dwi2tensor, 'in_file')
        pipeline.connect_input('brain_mask', dwi2tensor, 'in_mask')
        # Connect to outputs
        pipeline.connect_output('tensor', dwi2tensor, 'out_file')
        # Check inputs/output are connected
        pipeline.assert_connected()
        return pipeline

    def fa_pipeline(self, **kwargs):  # @UnusedVariable
        """
        Fits the apparrent diffusion tensor (DT) to each voxel of the image
        """
        pipeline = self._create_pipeline(
            name='fa',
            inputs=['tensor', 'brain_mask'],
            outputs=['fa', 'adc'],
            description=("Calculates the FA and ADC from a tensor image"),
            options={},
            citations=[],
            requirements=[mrtrix3_req],
            approx_runtime=1)
        # Create tensor fit node
        metrics = pe.Node(TensorMetrics(), name='metrics')
        metrics.inputs.out_fa = 'fa.nii.gz'
        metrics.inputs.out_adc = 'adc.nii.gz'
        # Connect to inputs
        pipeline.connect_input('tensor', metrics, 'in_file')
        pipeline.connect_input('brain_mask', metrics, 'in_mask')
        # Connect to outputs
        pipeline.connect_output('fa', metrics, 'out_fa')
        pipeline.connect_output('adc', metrics, 'out_adc')
        # Check inputs/output are connected
        pipeline.assert_connected()
        return pipeline

    def fod_pipeline(self, **kwargs):  # @UnusedVariable
        """
        Estimates the fibre orientation distribution (FOD) using constrained
        spherical deconvolution

        Parameters
        ----------
        """
        pipeline = self._create_pipeline(
            name='fod',
            inputs=['bias_correct', 'grad_dirs', 'bvalues', 'brain_mask'],
            outputs=['fod'],
            description=("Estimates the fibre orientation distribution in each"
                         " voxel"),
            options={},
            citations=[mrtrix_cite],
            requirements=[mrtrix3_req],
            approx_runtime=1)
        # Create fod fit node
        dwi2fod = pe.Node(EstimateFOD(), name='dwi2fod')
        response = pe.Node(ResponseSD(), name='response')
        # Gradient merge node
        fsl_grads = pe.Node(MergeTuple(2), name="fsl_grads")
        # Connect nodes
        pipeline.connect(fsl_grads, 'out', response, 'grad_fsl')
        pipeline.connect(fsl_grads, 'out', dwi2fod, 'grad_fsl')
        pipeline.connect(response, 'out_file', dwi2fod, 'response')
        # Connect to inputs
        pipeline.connect_input('grad_dirs', fsl_grads, 'in1')
        pipeline.connect_input('bvalues', fsl_grads, 'in2')
        pipeline.connect_input('bias_correct', dwi2fod, 'in_file')
        pipeline.connect_input('bias_correct', response, 'in_file')
        pipeline.connect_input('brain_mask', response, 'in_mask')
        # Connect to outputs
        pipeline.connect_output('fod', dwi2fod, 'out_file')
        # Check inputs/output are connected
        pipeline.assert_connected()
        return pipeline

    def tbss_pipeline(self, tbss_skel_thresh=0.2, **kwargs):  # @UnusedVariable
        pipeline = self._create_pipeline(
            'tbss',
            inputs=['fa'],
            outputs=['tbss_mean_fa', 'tbss_proj_fa', 'tbss_skeleton',
                     'tbss_skeleton_mask'],
            options={'tbss_skel_thresh': tbss_skel_thresh},
            citations=[tbss_cite, fsl_cite],
            requirements=[fsl5_req],
            approx_runtime=1)
        # Create TBSS workflow
        tbss = create_tbss_all(name='tbss')
        # Connect inputs
        pipeline.connect_input('fa', tbss, 'inputnode.fa_list')
        # Connect outputs
        pipeline.connect_output('tbss_mean_fa', tbss,
                                'outputnode.meanfa_file')
        pipeline.connect_output('tbss_proj_fa', tbss,
                                'outputnode.projectedfa_file')
        pipeline.connect_output('tbss_skeleton', tbss,
                                'outputnode.skeleton_file')
        pipeline.connect_output('tbss_skeleton_mask', tbss,
                                'outputnode.skeleton_mask')
        # Check inputs/output are connected
        pipeline.assert_connected()
        return pipeline

    def extract_b0_pipeline(self, **kwargs):  # @UnusedVariable
        """
        Extracts the b0 images from a DWI dataset and takes their mean
        """
        pipeline = self._create_pipeline(
            name='extract_b0',
            inputs=['dwi_preproc', 'grad_dirs', 'bvalues'],
            outputs=['mri_scan'],
            description="Extract b0 image from a DWI dataset",
            options={}, requirements=[mrtrix3_req], citations=[mrtrix_cite],
            approx_runtime=0.5)
        # Gradient merge node
        fsl_grads = pe.Node(MergeTuple(2), name="fsl_grads")
        # Extraction node
        extract_b0s = pe.Node(ExtractDWIorB0(), name='extract_b0s')
        extract_b0s.inputs.bzero = True
        extract_b0s.inputs.quiet = True
        # Mean calculation node
        mean = pe.Node(MRMath(), name="mean")
        mean.inputs.axis = 3
        mean.inputs.operation = 'mean'
        mean.inputs.quiet = True
        # Convert to Nifti
        mrconvert = pe.Node(MRConvert(), name="output_conversion")
        mrconvert.inputs.out_ext = 'nii.gz'
        mrconvert.inputs.quiet = True
        # Connect inputs
        pipeline.connect_input('dwi_preproc', extract_b0s, 'in_file')
        pipeline.connect_input('grad_dirs', fsl_grads, 'in1')
        pipeline.connect_input('bvalues', fsl_grads, 'in2')
        # Connect between nodes
        pipeline.connect(extract_b0s, 'out_file', mean, 'in_files')
        pipeline.connect(fsl_grads, 'out', extract_b0s, 'fslgrad')
        pipeline.connect(mean, 'out_file', mrconvert, 'in_file')
        # Connect outputs
        pipeline.connect_output('mri_scan', mrconvert, 'out_file')
        pipeline.assert_connected()
        # Check inputs/outputs are connected
        return pipeline

    # The list of dataset components that are either acquired from the scanner
    # (i.e. without a specified pipeline) or generated by processing pipelines
    _components = _create_component_dict(
        Scan('dwi_scan', mrtrix_format),
        Scan('forward_rpe', mrtrix_format),
        Scan('reverse_rpe', mrtrix_format),
        Scan('mri_scan', nifti_gz_format, extract_b0_pipeline),
        Scan('tensor', nifti_gz_format, tensor_pipeline),
        Scan('fa', nifti_gz_format, tensor_pipeline),
        Scan('adc', nifti_gz_format, tensor_pipeline),
        Scan('fod', mrtrix_format, tensor_pipeline),
        Scan('dwi_preproc', nifti_gz_format, preprocess_pipeline),
        Scan('bias_correct', nifti_gz_format, bias_correct_pipeline),
        Scan('grad_dirs', fsl_bvecs_format, preprocess_pipeline),
        Scan('bvalues', fsl_bvals_format, preprocess_pipeline),
        Scan('tbss_mean_fa', nifti_gz_format, tbss_pipeline,
             multiplicity='per_project'),
        Scan('tbss_proj_fa', nifti_gz_format, tbss_pipeline,
             multiplicity='per_project'),
        Scan('tbss_skeleton', nifti_gz_format, tbss_pipeline,
             multiplicity='per_project'),
        Scan('tbss_skeleton_mask', nifti_gz_format, tbss_pipeline,
             multiplicity='per_project'),
        inherit_from=T2Dataset.generated_components())


class NODDIDataset(DiffusionDataset):

    def concatenate_pipeline(self, **kwargs):  # @UnusedVariable
        """
        Concatenates two dMRI scans (with different b-values) along the
        DW encoding (4th) axis
        """
        pipeline = self._create_pipeline(
            name='concatenation',
            inputs=['low_b_dw_scan', 'high_b_dw_scan'],
            outputs=['dwi_scan'],
            description=(
                "Concatenate low and high b-value dMRI scans for NODDI "
                "processing"),
            options={},
            requirements=[mrtrix3_req],
            citations=[mrtrix_cite], approx_runtime=1)
        # Create concatenation node
        mrcat = pe.Node(MRCat(), name='mrcat')
        mrcat.inputs.quiet = True
        # Connect inputs/outputs
        pipeline.connect_input('low_b_dw_scan', mrcat, 'first_scan')
        pipeline.connect_input('high_b_dw_scan', mrcat, 'second_scan')
        pipeline.connect_output('dwi_scan', mrcat, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    def noddi_fitting_pipeline(
            self, noddi_model='WatsonSHStickTortIsoV_B0', single_slice=None,
            nthreads=4, **kwargs):  # @UnusedVariable
        """
        Creates a ROI in which the NODDI processing will be performed

        Parameters
        ----------
        single_slice: Int
            If provided the processing is only performed on a single slice
            (for testing)
        noddi_model: Str
            Name of the NODDI model to use for the fitting
        nthreads: Int
            Number of processes to use
        """
        inputs = ['dwi_preproc', 'grad_dirs', 'bvalues']
        if single_slice is None:
            inputs.append('brain_mask')
        else:
            inputs.append('eroded_mask')
        pipeline = self._create_pipeline(
            name='noddi_fitting',
            inputs=inputs,
            outputs=['ficvf', 'odi', 'fiso', 'fibredirs_xvec',
                     'fibredirs_yvec', 'fibredirs_zvec', 'fmin', 'kappa',
                     'error_code'],
            description=(
                "Creates a ROI in which the NODDI processing will be "
                "performed"),
            options={'noddi_model': noddi_model},
            requirements=[Requirement('matlab', min_version=(2016, 'a')),
                          Requirement('noddi', min_version=(0, 9)),
                          Requirement('niftimatlib', (1, 2))],
            citations=[noddi_cite], approx_runtime=60)
        # Create node to unzip the nifti files
        unzip_preproc = pe.Node(MRConvert(), name="unzip_preproc")
        unzip_preproc.inputs.out_ext = 'nii'
        unzip_preproc.inputs.quiet = True
        unzip_mask = pe.Node(MRConvert(), name="unzip_mask")
        unzip_mask.inputs.out_ext = 'nii'
        unzip_mask.inputs.quiet = True
        # Create create-roi node
        create_roi = pe.Node(CreateROI(), name='create_roi')
        pipeline.connect(unzip_preproc, 'out_file', create_roi, 'in_file')
        pipeline.connect(unzip_mask, 'out_file', create_roi, 'brain_mask')
        # Create batch-fitting node
        batch_fit = pe.Node(BatchNODDIFitting(), name="batch_fit")
        batch_fit.inputs.model = noddi_model
        batch_fit.inputs.nthreads = nthreads
        pipeline.connect(create_roi, 'out_file', batch_fit, 'roi_file')
        # Create output node
        save_params = pe.Node(SaveParamsAsNIfTI(), name="save_params")
        save_params.inputs.output_prefix = 'params'
        pipeline.connect(batch_fit, 'out_file', save_params, 'params_file')
        pipeline.connect(create_roi, 'out_file', save_params, 'roi_file')
        pipeline.connect(unzip_mask, 'out_file', save_params,
                         'brain_mask_file')
        # Connect inputs
        pipeline.connect_input('dwi_preproc', unzip_preproc, 'in_file')
        if single_slice is None:
            pipeline.connect_input('brain_mask', unzip_mask, 'in_file')
        else:
            pipeline.connect_input('eroded_mask', unzip_mask, 'in_file')
        pipeline.connect_input('grad_dirs', batch_fit, 'bvecs_file')
        pipeline.connect_input('bvalues', batch_fit, 'bvals_file')
        # Connect outputs
        pipeline.connect_output('ficvf', save_params, 'ficvf')
        pipeline.connect_output('odi', save_params, 'odi')
        pipeline.connect_output('fiso', save_params, 'fiso')
        pipeline.connect_output('fibredirs_xvec', save_params,
                                'fibredirs_xvec')
        pipeline.connect_output('fibredirs_yvec', save_params,
                                'fibredirs_yvec')
        pipeline.connect_output('fibredirs_zvec', save_params,
                                'fibredirs_zvec')
        pipeline.connect_output('fmin', save_params, 'fmin')
        pipeline.connect_output('kappa', save_params, 'kappa')
        pipeline.connect_output('error_code', save_params, 'error_code')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    _components = _create_component_dict(
        Scan('low_b_dw_scan', mrtrix_format),
        Scan('high_b_dw_scan', mrtrix_format),
        Scan('forward_rpe', mrtrix_format),
        Scan('reverse_rpe', mrtrix_format),
        Scan('dwi_scan', mrtrix_format, concatenate_pipeline),
        Scan('ficvf', nifti_format, noddi_fitting_pipeline),
        Scan('odi', nifti_format, noddi_fitting_pipeline),
        Scan('fiso', nifti_format, noddi_fitting_pipeline),
        Scan('fibredirs_xvec', nifti_format, noddi_fitting_pipeline),
        Scan('fibredirs_yvec', nifti_format, noddi_fitting_pipeline),
        Scan('fibredirs_zvec', nifti_format, noddi_fitting_pipeline),
        Scan('fmin', nifti_format, noddi_fitting_pipeline),
        Scan('kappa', nifti_format, noddi_fitting_pipeline),
        Scan('error_code', nifti_format, noddi_fitting_pipeline),
        inherit_from=DiffusionDataset.generated_components())
