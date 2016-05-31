from nipype.pipeline import engine as pe
from nipype.interfaces.mrtrix3.utils import BrainMask
from ..interfaces.mrtrix import DWIPreproc, MRCat
from ..interfaces.noddi import (
    CreateROI, BatchNODDIFitting, SaveParamsAsNIfTI)
from .t2 import T2Dataset
from ..interfaces.mrtrix import MRConvert, ExtractFSLGradients
from neuroanalysis.citations import (
    mrtrix_cite, fsl_cite, eddy_cite, topup_cite, distort_correct_cite,
    noddi_cite)
from neuroanalysis.file_formats import (
    mrtrix_format, nifti_gz_format, fsl_bvecs_format, fsl_bvals_format,
    nifti_format)
from neuroanalysis.requirements import Requirement
from neuroanalysis.exception import NeuroAnalysisError


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
            outputs=['dwi_preproc', 'gradient_directions', 'bvalues'],
            description="Preprocess dMRI datasets using distortion correction",
            options={'phase_dir': phase_dir},
            requirements=[Requirement('mrtrix3', min_version=(0, 3, 12)),
                          Requirement('fsl', min_version=(5, 0))],
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
        pipeline.connect_output('gradient_directions', extract_grad,
                                'bvecs_file')
        pipeline.connect_output('bvalues', extract_grad, 'bvals_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    def brain_mask_pipeline(self, mask_tool='fsl', **kwargs):  # @UnusedVariable @IgnorePep8
        """
        Generates a whole brain mask using MRtrix's 'dwi2mask' command
        """
        if mask_tool == 'fsl':
            pipeline = super(DiffusionDataset, self).brain_mask_pipeline(
                **kwargs)
        elif mask_tool == 'mrtrix3':
            pipeline = self._create_pipeline(
                name='brain_mask',
                inputs=['dwi_preproc'],
                outputs=['brain_mask'],
                description="Generate brain mask from b0 images",
                options={},
                requirements=[Requirement('mrtrix3', min_version=(0, 3, 12))],
                citations=[mrtrix_cite], approx_runtime=1)
            # Create mask node
            dwi2mask = pe.Node(BrainMask(), name='dwi2mask')
            dwi2mask.inputs.out_file = 'brain_mask.nii.gz'
            # Connect inputs/outputs
            pipeline.connect_input('dwi_preproc', dwi2mask, 'in_file')
            pipeline.connect_output('brain_mask', dwi2mask, 'out_file')
            # Check inputs/outputs are connected
            pipeline.assert_connected()
        else:
            raise NeuroAnalysisError(
                "Unrecognised mask_tool '{}' (valid options 'fsl' or "
                "'mrtrix3')")
        return pipeline

    def fod_pipeline(self):
        raise NotImplementedError

    # The list of dataset components that are acquired by the scanner
    acquired_components = dict(
        T2Dataset.acquired_components.items() +
        [('forward_rpe', mrtrix_format),
         ('reverse_rpe', mrtrix_format)])

    generated_components = dict(
        T2Dataset.generated_components.items() +
        [('fod', (fod_pipeline, mrtrix_format)),
         ('dwi_preproc', (preprocess_pipeline, nifti_gz_format)),
         ('gradient_directions', (preprocess_pipeline, fsl_bvecs_format)),
         ('bvalues', (preprocess_pipeline, fsl_bvals_format))])


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
            requirements=[Requirement('mrtrix3', min_version=(0, 3, 12))],
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

    def noddi_fitting_pipeline(self, noddi_model='WatsonSHStickTortIsoV_B0',
                               nthreads=4, **kwargs):  # @UnusedVariable
        """
        Creates a ROI in which the NODDI processing will be performed
        """
        pipeline = self._create_pipeline(
            name='noddi_fitting',
            inputs=['dwi_preproc', 'brain_mask', 'gradient_directions',
                    'bvalues'],
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
        pipeline.connect_input('brain_mask', unzip_mask, 'in_file')
        pipeline.connect_input('gradient_directions', batch_fit, 'bvecs_file')
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

    acquired_components = {
        'low_b_dw_scan': mrtrix_format, 'high_b_dw_scan': mrtrix_format,
        'forward_rpe': mrtrix_format, 'reverse_rpe': mrtrix_format}

    generated_components = dict(
        DiffusionDataset.generated_components.items() +
        [('dwi_scan', (concatenate_pipeline, mrtrix_format)),
         ('ficvf', (noddi_fitting_pipeline, nifti_format)),
         ('odi', (noddi_fitting_pipeline, nifti_format)),
         ('fiso', (noddi_fitting_pipeline, nifti_format)),
         ('fibredirs_xvec', (noddi_fitting_pipeline, nifti_format)),
         ('fibredirs_yvec', (noddi_fitting_pipeline, nifti_format)),
         ('fibredirs_zvec', (noddi_fitting_pipeline, nifti_format)),
         ('fmin', (noddi_fitting_pipeline, nifti_format)),
         ('kappa', (noddi_fitting_pipeline, nifti_format)),
         ('error_code', (noddi_fitting_pipeline, nifti_format))])
