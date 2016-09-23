from .base import MRDataset
import nipype.pipeline.engine as pe
from nipype.interfaces.spm.preprocess import Segment
from nianalysis.base import Scan
from nianalysis.citations import spm_cite
from nianalysis.requirements import spm12_req
from nianalysis.formats import nifti_format
from nianalysis.dataset.base import _create_component_dict


class T1Dataset(MRDataset):

    def segmentation_pipeline(self):
        pipeline = self._create_pipeline(
            name='segmentation',
            inputs=['t1'],
            outputs=[
                'native_gm', 'normalized_gm', 'modulated_gm',
                'native_wm', 'normalized_wm', 'modulated_wm',
                'native_csf', 'normalized_csf',
                'modulated_csf', 'modulated_input',
                'bias_corrected', 'seg_transform',
                'seg_inv_transform'],
            description="Preprocess dMRI datasets using distortion correction",
            options={},
            requirements=[spm12_req],
            citations=[spm_cite],
            approx_runtime=5)
        segment = pe.Node(Segment(), name='segment')
        segment.inputs.gm_output_type = [True, True, True]
        segment.inputs.wm_output_type = [True, True, True]
        segment.inputs.csf_output_type = [True, True, True]
        segment.inputs.bias_regularization = 0.0
        segment.inputs.bias_fwhm = 120
        # Connect inputs
        pipeline.connect_input('t1', segment, 'data')
        # Connect outputs
        pipeline.connect_output('native_gm', segment,
                                'native_gm_image')
        pipeline.connect_output('normalized_gm', segment,
                                'normalized_gm_image')
        pipeline.connect_output('modulated_gm', segment,
                                'modulated_gm_image')
        pipeline.connect_output('native_wm', segment,
                                'native_wm_image')
        pipeline.connect_output('normalized_wm', segment,
                                'normalized_wm_image')
        pipeline.connect_output('modulated_wm', segment,
                                'modulated_wm_image')
        pipeline.connect_output('native_csf', segment,
                                'native_csf_image')
        pipeline.connect_output('normalized_csf', segment,
                                'normalized_csf_image')
        pipeline.connect_output('modulated_csf', segment,
                                'modulated_csf_image')
        pipeline.connect_output('modulated_input', segment,
                                'modulated_input_image')
        pipeline.connect_output('bias_corrected', segment,
                                'bias_corrected_image')
        pipeline.connect_output('transform', segment,
                                'transformation_mat')
        pipeline.connect_output('inv_transform', segment,
                                'inverse_transformation_mat')
        pipeline.assert_connected()
        return pipeline

    _components = _create_component_dict(
        Scan('t1', nifti_format),
        Scan('native_gm', nifti_format, pipeline=segmentation_pipeline),
        Scan('normalized_gm', nifti_format, pipeline=segmentation_pipeline),
        Scan('modulated_gm', nifti_format, pipeline=segmentation_pipeline),
        Scan('native_wm', nifti_format, pipeline=segmentation_pipeline),
        Scan('normalized_wm', nifti_format, pipeline=segmentation_pipeline),
        Scan('modulated_wm', nifti_format, pipeline=segmentation_pipeline),
        Scan('native_csf', nifti_format, pipeline=segmentation_pipeline),
        Scan('normalized_csf', nifti_format, pipeline=segmentation_pipeline),
        Scan('modulated_csf', nifti_format, pipeline=segmentation_pipeline),
        Scan('modulated_input', nifti_format, pipeline=segmentation_pipeline),
        Scan('bias_corrected', nifti_format, pipeline=segmentation_pipeline),
        Scan('transform', nifti_format, pipeline=segmentation_pipeline),
        Scan('inv_transform', nifti_format, pipeline=segmentation_pipeline))
