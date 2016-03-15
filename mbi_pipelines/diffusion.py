from informatics.daris.system import (
    DarisInformaticsSystem, DarisProject, DarisExperiment,
    DarisScan)
import os.path
from nipype.interfaces.io import DataSink
from nipype.interfaces import utility as util
from nipype.pipeline import engine as pe
from nipype.interfaces import fsl as fsl
from nipype.interfaces import mrtrix as mrtrix


class DiffusionProcessor(object):

    MONASH_DARIS_HOSTNAME = 'mf-erc.its.monash.edu.au'
    MONASH_LDAP_DOMAIN = 'monash-ldap'
    ASPREE_DOWNLOAD_DIR_DEFAULT = '/data/reference/aspree'
    ASPREE_PROJECT_NUMBER = '1008.2.88'
    file_exts = {'nii', 'nii.gz'}

    def __init__(self, user, password,
                 aspree_download_dir=ASPREE_DOWNLOAD_DIR_DEFAULT):
        if not os.path.exists(aspree_download_dir):
            raise Exception("Provided aspree download directory '{}' does not "
                            "exist".format(aspree_download_dir))
        self._download_dir = aspree_download_dir
        self._user = user
        self._password = password
        self._daris = None

    def _connect_to_daris(self):
        if self._daris is None:
            self._daris = DarisInformaticsSystem()
            self._daris.initialise_creds(
                self.MONASH_DARIS_HOSTNAME,
                self.MONASH_LDAP_DOMAIN,
                self._user,
                self._password)
            self._daris.connect()
            self._aspree = DarisProject(
                self._daris.getAllProjects()[self.ASPREE_PROJECT_NUMBER].dPObj)
            # Load all subjects into self._aspree.subjects
            self._aspree.getAllSubjects()

    def _download_scan_from_daris(self, subject_id, scan_name, time_point=1,
                                  processed=False, file_type='nii'):
        self._connect_to_daris()
        subject = self._aspree.subjects[
            '{}.{}'.format(self.ASPREE_PROJECT_NUMBER, subject_id)]
        # Construct experiment id
        experiment_id = ('{aspree}.{subject}.{raw_or_processed}.{time_point}'
                         .format(aspree=self.ASPREE_PROJECT_NUMBER,
                                 subject=subject_id,
                                 raw_or_processed=(2 if processed else 1),
                                 time_point=time_point))
        experiment = DarisExperiment(
            subject.getAllExperiments()[experiment_id].dEObj)
        scans = experiment.getAllScans()
        scan = DarisScan(next(s.dScObj for s in scans.itervalues()
                              if s.dScObj['name'] == scan_name))
        # Download scan
        subject_dir = os.path.join(self._download_dir, subject_id)
        if not os.path.exists(subject_dir):
            os.mkdir(subject_dir)
        scan_path = os.path.join(
            subject_dir, scan_name + self.file_exts[file_type])
        if not os.path.exists(scan_path):
            scan.downloadFile(file_type, subject_dir)
        return scan_path

    def process_mrtrix(self, subject_id):
        diffusion_image = self._download_scan(subject_id, 'Diffusion', 1)
        pipeline = self._create_mrtrix_workflow()
        pipeline.inputnode.dwi = diffusion_image

    def _create_mrtrix_workflow(self, name="mrtrix_processing",
                                tractography_type='probabilistic',
                                working_dir=None):
        """Creates a pipeline that does the same diffusion processing as in the
        :doc:`../../users/examples/dmri_mrtrix_dti` example script. Given a
        diffusion-weighted image, b-values, and b-vectors, the workflow will
        return the tractography computed from spherical deconvolution and
        probabilistic streamline tractography

        Example
        -------

        >>> dti = create_mrtrix_dti_pipeline("mrtrix_dti")
        >>> dti.inputs.inputnode.dwi = 'data.nii'
        >>> dti.run()                  # doctest: +SKIP

        Inputs::

            inputnode.dwi

        Outputs::

            outputnode.fa
            outputnode.tdi
            outputnode.tracts_tck
            outputnode.csdeconv

        """
        # Create workflow nodes
        inputnode = pe.Node(interface=util.IdentityInterface(fields=["dwi"]),
                            name="inputnode")
        mrtrix2fsl = pe.Node(interface=mrtrix.MRConvert(), name='MRtrix2FSL')
        mrtrix2fsl.inputs.out_filename = 'dwi.nii.gz'
        bet = pe.Node(interface=fsl.BET(), name="bet")
        bet.inputs.mask = True
        dwi2tensor = pe.Node(interface=mrtrix.DWI2Tensor(), name='dwi2tensor')
        tensor2vector = pe.Node(interface=mrtrix.Tensor2Vector(),
                                name='tensor2vector')
        tensor2adc = pe.Node(interface=mrtrix.Tensor2ApparentDiffusion(),
                             name='tensor2adc')
        tensor2fa = pe.Node(interface=mrtrix.Tensor2FractionalAnisotropy(),
                            name='tensor2fa')
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

        MRmultiply = pe.Node(interface=mrtrix.MRMultiply(), name='MRmultiply')
        MRmult_merge = pe.Node(
            interface=util.Merge(2),
            name='MRmultiply_merge')

        median3d = pe.Node(interface=mrtrix.MedianFilter3D(), name='median3D')

        MRconvert = pe.Node(interface=mrtrix.MRConvert(), name='MRconvert')
        MRconvert.inputs.extract_at_axis = 3
        MRconvert.inputs.extract_at_coordinate = [0]

        csdeconv = pe.Node(
            interface=mrtrix.ConstrainedSphericalDeconvolution(),
            name='csdeconv')

        gen_WM_mask = pe.Node(interface=mrtrix.GenerateWhiteMatterMask(),
                              name='gen_WM_mask')

        extract_gradients = pe.Node(interface=ExtractMRtrixGradients(),
                                    name="extract_graidents")
        estimateresponse = pe.Node(interface=mrtrix.EstimateResponseForSH(),
                                   name='estimateresponse')

        if tractography_type == 'probabilistic':
            CSDstreamtrack = pe.Node(
                interface=mrtrix.ProbabilisticSphericallyDeconvolutedStreamlineTrack(),  # @IgnorePep8
                name='CSDstreamtrack')
        else:
            CSDstreamtrack = pe.Node(
                interface=mrtrix.SphericallyDeconvolutedStreamlineTrack(),
                name='CSDstreamtrack')
        CSDstreamtrack.inputs.desired_number_of_tracks = 15000

        tracks2prob = pe.Node(
            interface=mrtrix.Tracks2Prob(),
            name='tracks2prob')
        tracks2prob.inputs.colour = True

        workflow = pe.Workflow(name=name, base_dir=working_dir)
        workflow.base_output_dir = name
        workflow.connect([(inputnode, dwi2tensor, [("dwi", "in_file")])])
        workflow.connect([(dwi2tensor, tensor2vector, [['tensor', 'in_file']]),
                          (dwi2tensor, tensor2adc, [['tensor', 'in_file']]),
                          (dwi2tensor, tensor2fa, [['tensor', 'in_file']]),
                          ])
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
        return workflow, outputnode

    def process(self, input_image, output_dir, **kwargs):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        mrtrix_workflow, mrtrix_output = self._create_mrtrix_workflow(**kwargs)
        mrtrix_workflow.inputs.inputnode.dwi = input_image
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


if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('input_image', type=str,
                        help="Input dwi image in mif format (with gradients)")
    parser.add_argument('output_dir', type=str,
                        help=("Output directory where FA, CSD and tracks files"
                              " are stored."))
    parser.add_argument('--working_dir', type=str, default=None,
                        help=("The directory where the intermediate files are "
                              "stored"))
    args = parser.parse_args()
    processor = DiffusionProcessor('tclose', os.environ['DARIS_PASSWORD'])
    processor.process(args.input_image, args.output_dir,
                      working_dir=args.working_dir)
