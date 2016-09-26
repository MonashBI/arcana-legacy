import os
from itertools import chain
from nipype.pipeline import engine as pe
from nipype.interfaces.spm.preprocess import Coregister
from nianalysis.dataset.base import _create_component_dict
from nipype.interfaces.utility import Merge, Split
from nipype.interfaces.spm import NewSegment, Info
from nianalysis.base import Scan
from nianalysis.formats import nifti_format
from nianalysis.requirements import spm12_req
from nianalysis.citations import spm_cite
from nianalysis.exceptions import NiAnalysisError
from .t1 import T1Dataset
from .t2 import T2Dataset


try:
    # See if SPMDIR is set to avoid MATLAB call
    spm_path = os.environ['SPMDIR']
except KeyError:
    try:
        spm_path = Info.version()['path']
    except AttributeError:
        raise NiAnalysisError(
            "Cannot find MATLAB on system path ({})"
            .format(os.environ['PATH']))


class T1AndT2Dataset(T1Dataset, T2Dataset):

    def __init__(self, *args, **kwargs):
        T1Dataset.__init__(self, *args, **kwargs)
        T2Dataset.__init__(self, *args, **kwargs)

    def coregistration_pipeline(self):
        pipeline = self._create_pipeline(
            name='coregistration',
            inputs=['t1', 't2'],
            outputs=['t2_coreg_t1'],
            description="Coregister T2-weighted images to T1",
            options={},
            requirements=[spm12_req],
            citations=[spm_cite],
            approx_runtime=30)
        coreg = pe.Node(Coregister(), name='coreg')
        coreg.inputs.jobtype = 'estwrite'
        coreg.inputs.cost_function = 'nmi'
        coreg.inputs.separation = [4, 2]
        coreg.inputs.tolerance = [
            0.02, 0.02, 0.02, 0.001, 0.001, 0.001, 0.01, 0.01, 0.01, 0.001,
            0.001, 0.001]
        coreg.inputs.fwhm = [7, 7]
        coreg.inputs.write_interp = 4
        coreg.inputs.write_wrap = [0, 0, 0]
        coreg.inputs.write_mask = False
        coreg.inputs.out_prefix = 'r'
        # Connect inputs
        pipeline.connect_input('t1', coreg, 'target')
        pipeline.connect_input('t2', coreg, 'source')
        # Connect outputs
        pipeline.connect_output('t2_coreg_t1', coreg, 'coregistered_source')
        pipeline.assert_connected()
        return pipeline

    def joint_segmentation_pipeline(self):
        pipeline = self._create_pipeline(
            name='segmentation',
            inputs=['t1'],
            outputs=['t1_white_matter', 't1_grey_matter', 't1_csf'],
            description="Segment white/grey matter and csf",
            options={},
            requirements=[spm12_req],
            citations=[spm_cite],
            approx_runtime=5)
        seg = pe.Node(NewSegment(), name='seg')
        tmp_path = os.path.join(spm_path, 'tpm', 'TPM.nii')
        seg.inputs.tissues = [
            ((tmp_path, 1), 5, (True, False), (False, False)),
            ((tmp_path, 2), 5, (True, False), (False, False)),
            ((tmp_path, 3), 5, (True, False), (False, False)),
            ((tmp_path, 4), 3, (False, False), (False, False)),
            ((tmp_path, 5), 4, (False, False), (False, False)),
            ((tmp_path, 6), 2, (False, False), (False, False))]
        seg.inputs.affine_regularization = 'mni'
        seg.inputs.warping_regularization = [0.0, 0.001, 0.5, 0.025, 0.1]
        seg.inputs.sampling_distance = 3.0
        seg.write_deformation_fields = False
        # Not sure what inputs these should correspond to
#         seg.inputs.mrf = 2.0
#         seg.inputs.fwhm = 0.0
        num_channels = 1
#         merge = pe.Node(Merge(2), name='input_merge')
#         pipeline.connect(merge, 'out', seg, 'channel_files')
#         pipeline.connect_input('t1', merge, 'in1')
#         pipeline.connect_input('t2_coreg_t1', merge, 'in2')
#         num_channels = 2
        tissue_split = pe.Node(Split(), name='tissue_split')
        tissue_split.inputs.splits = [1] * len(seg.inputs.tissues)
        tissue_split.inputs.squeeze = True
        pipeline.connect(seg, 'native_class_images', tissue_split, 'inlist')
        channel_splits = []
        for i, tissue in enumerate(seg.inputs.tissues):
            if tissue[2][0]:
                split = pe.Node(Split(), name='tissue{}_split'.format(i))
                split.inputs.splits = [1] * num_channels
                split.inputs.squeeze = True
                pipeline.connect(tissue_split, 'out' + str(i + 1), split,
                                 'inlist')
                channel_splits.append(split)
        # Connect inputs
        pipeline.connect_input('t1', seg, 'channel_files')
        # Connect outputs
        pipeline.connect_output('t1_grey_matter', channel_splits[0], 'out1')
        pipeline.connect_output('t1_white_matter', channel_splits[1], 'out1')
        pipeline.connect_output('t1_csf', channel_splits[2], 'out1')
#         pipeline.connect_output('t2_grey_matter', channel_splits[1], 'out2')
#         pipeline.connect_output('t2_white_matter', channel_splits[2], 'out2')
#         pipeline.connect_output('t2_csf', channel_splits[3], 'out2')
        return pipeline

    _components = _create_component_dict(
        Scan('t1', nifti_format),
        Scan('t2', nifti_format),
        Scan('t2_coreg_t1', nifti_format, coregistration_pipeline),
        Scan('t1_white_matter', nifti_format, joint_segmentation_pipeline),
        Scan('t1_grey_matter', nifti_format, joint_segmentation_pipeline),
        Scan('t1_csf', nifti_format, joint_segmentation_pipeline),
        inherit_from=chain(T1Dataset.generated_components(),
                           T2Dataset.generated_components()))
