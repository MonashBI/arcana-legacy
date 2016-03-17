import os.path
from nipype.interfaces.io import DataSink
from nipype.interfaces import utility as util
from nipype.pipeline import engine as pe
from nipype.interfaces import fsl as fsl
from nipype.interfaces import mrtrix as mrtrix
from .interfaces.mrtrix import ExtractMRtrixGradients


class DiffusionProcessor(object):

    def __init__(self, data_accessor=None):
        self._data_accessor = data_accessor

    def process_mrtrix(self, subject_id, time_point):
        diffusion_image = self._data_accessor.download_scan(
            subject_id, 'Diffusion', time_point=time_point)
        pipeline = self._create_mrtrix_workflow()
        pipeline.inputnode.dwi = diffusion_image

    def _create_mrtrix_workflow(self, name="mrtrix_processing",
                                tractography_type='probabilistic',
                                output_dir=None, working_dir=None):
        """Creates a pipeline that does the standard MRtrix (2.12) diffusion
        processing. The workflow will return the tractography computed from
        spherical deconvolution and probabilistic streamline tractography

        Example
        -------

        >>> workflow, inputs, outputs = processor._create_mrtrix_workflow()
        >>> inputs.dwi = 'data.nii'
        >>> workflow.run()

        Inputs::

            dwi

        Outputs::

            fa
            tdi
            tracts_tck
            csdeconv

        """
        # =====================================================================
        # Create workflow
        # =====================================================================
        workflow = pe.Workflow(name=name, base_dir=working_dir)
        workflow.base_output_dir = (output_dir
                                    if output_dir is not None else name)
        # =====================================================================
        # Create workflow nodes
        # =====================================================================
        # Input Node
        inputnode = pe.Node(interface=util.IdentityInterface(fields=["dwi"]),
                            name="inputnode")
        # Convert input file format (including graidents to NIFTI)
        mrtrix2fsl = pe.Node(interface=mrtrix.MRConvert(), name='MRtrix2FSL')
        mrtrix2fsl.inputs.out_filename = 'dwi.nii.gz'
        # Extracts gradients from input image to be used with other interfaces
        extract_gradients = pe.Node(interface=ExtractMRtrixGradients(),
                                    name="extract_graidents")
        # FSL's brain extraction tool
        bet = pe.Node(interface=fsl.BET(), name="bet")
        bet.inputs.mask = True
        # MRtrix's tensor, ADC, FA caclulator
        dwi2tensor = pe.Node(interface=mrtrix.DWI2Tensor(), name='dwi2tensor')
        tensor2vector = pe.Node(interface=mrtrix.Tensor2Vector(),
                                name='tensor2vector')
        tensor2adc = pe.Node(interface=mrtrix.Tensor2ApparentDiffusion(),
                             name='tensor2adc')
        tensor2fa = pe.Node(interface=mrtrix.Tensor2FractionalAnisotropy(),
                            name='tensor2fa')
        # Threshold and mask erosion tools
        erode_mask_firstpass = pe.Node(interface=mrtrix.Erode(),
                                       name='erode_mask_firstpass')
        erode_mask_secondpass = pe.Node(interface=mrtrix.Erode(),
                                        name='erode_mask_secondpass')
        threshold_b0 = pe.Node(
            interface=mrtrix.Threshold(),
            name='threshold_b0')
        threshold_FA = pe.Node(
            interface=mrtrix.Threshold(),
            name='threshold_FA')
        threshold_FA.inputs.absolute_threshold_value = 0.7
        threshold_wmmask = pe.Node(interface=mrtrix.Threshold(),
                                   name='threshold_wmmask')
        threshold_wmmask.inputs.absolute_threshold_value = 0.4
        gen_WM_mask = pe.Node(interface=mrtrix.GenerateWhiteMatterMask(),
                              name='gen_WM_mask')
        MRmultiply = pe.Node(interface=mrtrix.MRMultiply(), name='MRmultiply')
        MRmult_merge = pe.Node(
            interface=util.Merge(2),
            name='MRmultiply_merge')
        median3d = pe.Node(interface=mrtrix.MedianFilter3D(), name='median3D')
        MRconvert = pe.Node(interface=mrtrix.MRConvert(), name='MRconvert')
        MRconvert.inputs.extract_at_axis = 3
        MRconvert.inputs.extract_at_coordinate = [0]
        # Calculate Constrained Spherical deconvolution
        csdeconv = pe.Node(
            interface=mrtrix.ConstrainedSphericalDeconvolution(),
            name='csdeconv')
        # Estimate diffusion response function
        estimateresponse = pe.Node(interface=mrtrix.EstimateResponseForSH(),
                                   name='estimateresponse')
        # Perform tracking
        if tractography_type == 'probabilistic':
            CSDstreamtrack = pe.Node(
                interface=mrtrix.ProbabilisticSphericallyDeconvolutedStreamlineTrack(),  # @IgnorePep8
                name='CSDstreamtrack')
        else:
            CSDstreamtrack = pe.Node(
                interface=mrtrix.SphericallyDeconvolutedStreamlineTrack(),
                name='CSDstreamtrack')
        CSDstreamtrack.inputs.desired_number_of_tracks = 15000
        # Calculate TDI
        tracks2prob = pe.Node(
            interface=mrtrix.Tracks2Prob(),
            name='tracks2prob')
        tracks2prob.inputs.colour = True
        # =====================================================================
        # Connect workflow nodes
        # =====================================================================
        # Tensor connections
        workflow.connect([(inputnode, dwi2tensor, [("dwi", "in_file")])])
        workflow.connect([(dwi2tensor, tensor2vector, [['tensor', 'in_file']]),
                          (dwi2tensor, tensor2adc, [['tensor', 'in_file']]),
                          (dwi2tensor, tensor2fa, [['tensor', 'in_file']]),
                          ])
        # Conversion connections
        workflow.connect([(inputnode, mrtrix2fsl, [("dwi", "in_file")])])
        workflow.connect([(inputnode, MRconvert, [("dwi", "in_file")])])
        workflow.connect(
            [(MRconvert, threshold_b0, [("converted", "in_file")])])
        workflow.connect([(threshold_b0, median3d, [("out_file", "in_file")])])
        workflow.connect(
            [(median3d, erode_mask_firstpass, [("out_file", "in_file")])])
        workflow.connect(
            [(erode_mask_firstpass, erode_mask_secondpass,
              [("out_file", "in_file")])])

        workflow.connect([(tensor2fa, MRmult_merge, [("FA", "in1")])])
        workflow.connect(
            [(erode_mask_secondpass, MRmult_merge, [("out_file", "in2")])])
        workflow.connect([(MRmult_merge, MRmultiply, [("out", "in_files")])])
        workflow.connect(
            [(MRmultiply, threshold_FA, [("out_file", "in_file")])])
        workflow.connect(
            [(inputnode, extract_gradients, [('dwi', 'in_file')])])
        workflow.connect(
            [(threshold_FA, estimateresponse, [("out_file", "mask_image")])])
        workflow.connect(
            [(extract_gradients, estimateresponse,
              [('out_file', 'encoding_file')])])
        workflow.connect([(mrtrix2fsl, bet, [("converted", "in_file")])])
        workflow.connect([(inputnode, gen_WM_mask, [("dwi", "in_file")])])
        workflow.connect([(extract_gradients, gen_WM_mask,
                           [("out_file", "encoding_file")])])
        workflow.connect([(bet, gen_WM_mask, [("mask_file", "binary_mask")])])
        workflow.connect([(inputnode, estimateresponse, [("dwi", "in_file")])])
        workflow.connect([(inputnode, csdeconv, [("dwi", "in_file")])])
        workflow.connect(
            [(gen_WM_mask, csdeconv, [("WMprobabilitymap", "mask_image")])])
        workflow.connect(
            [(estimateresponse, csdeconv, [("response", "response_file")])])
        workflow.connect(
            [(gen_WM_mask, threshold_wmmask,
              [("WMprobabilitymap", "in_file")])])
        workflow.connect(
            [(threshold_wmmask, CSDstreamtrack, [("out_file", "seed_file")])])
        workflow.connect(
            [(csdeconv, CSDstreamtrack,
              [("spherical_harmonics_image", "in_file")])])
        # Include probabilistic tractography
        if tractography_type == 'probabilistic':
            workflow.connect(
                [(CSDstreamtrack, tracks2prob, [("tracked", "in_file")])])
            workflow.connect(
                [(inputnode, tracks2prob, [("dwi", "template_file")])])
        output_fields = ["fa", "tracts_trk", "csdeconv", "tracts_tck"]
        if tractography_type == 'probabilistic':
            output_fields.append("tdi")
        outputnode = pe.Node(
            interface=util.IdentityInterface(fields=output_fields),
            name="outputnode")
        workflow.connect(
            [(CSDstreamtrack, outputnode, [("tracked", "tracts_tck")]),
             (csdeconv, outputnode,
              [("spherical_harmonics_image", "csdeconv")]),
             (tensor2fa, outputnode, [("FA", "fa")])])
        if tractography_type == 'probabilistic':
            workflow.connect(
                [(tracks2prob, outputnode, [("tract_image", "tdi")])])
        # Return workflow, input and output nodes
        return workflow, inputnode, outputnode

    def process(self, input_image, output_dir, **kwargs):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        (mrtrix_workflow, mrtrix_input,
         mrtrix_output) = self._create_mrtrix_workflow(**kwargs)
        mrtrix_input.dwi = input_image
        overall_workflow = pe.Workflow(name='overall')
        datasink = pe.Node(DataSink(), name='datasink')
        datasink.inputs.base_directory = output_dir
        overall_workflow.add_nodes((mrtrix_workflow, datasink))
        overall_workflow.connect(
            mrtrix_output, 'fa', datasink, 'output')
        overall_workflow.connect(
            mrtrix_output, 'tdi', datasink, 'output.@1')
        overall_workflow.connect(
            mrtrix_output, 'tracts_tck', datasink, 'output.@2')
        overall_workflow.connect(
            mrtrix_output, 'csdeconv', datasink, 'output.@3')
        overall_workflow.run()

    def plot_graph(self):
        workflow = self._create_mrtrix_workflow()[0]
        workflow.write_graph(graph2use='flat')

