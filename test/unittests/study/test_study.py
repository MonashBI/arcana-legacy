import os.path
from nipype import config
config.enable_debug_mode()
import subprocess as sp  # @IgnorePep8
from nianalysis.dataset import Dataset, DatasetSpec  # @IgnorePep8
from nianalysis.data_formats import nifti_gz_format, mrtrix_format, text_format  # @IgnorePep8
from nianalysis.requirements import mrtrix3_req  # @IgnorePep8
from nianalysis.study.base import Study, set_dataset_specs  # @IgnorePep8
from nianalysis.interfaces.mrtrix import MRConvert, MRCat, MRMath  # @IgnorePep8
from nianalysis.testing import BaseTestCase  # @IgnorePep8
from nianalysis.nodes import NiAnalysisNodeMixin  # @IgnorePep8
from nianalysis.exceptions import NiAnalysisModulesNotInstalledException  # @IgnorePep8
import logging  # @IgnorePep8
from nipype.interfaces.base import (  # @IgnorePep8
    BaseInterface, File, TraitedSpec, traits, isdefined)

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("workflow").setLevel(logging.INFO)

logger = logging.getLogger('NiAnalysis')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


class DummyStudy(Study):

    def pipeline1(self):
        pipeline = self.create_pipeline(
            name='pipeline1',
            inputs=[DatasetSpec('start', nifti_gz_format)],
            outputs=[DatasetSpec('pipeline1_1', nifti_gz_format),
                     DatasetSpec('pipeline1_2', nifti_gz_format)],
            description="A dummy pipeline used to test 'run_pipeline' method",
            default_options={},
            version=1,
            citations=[],)
        mrconvert = pipeline.create_node(MRConvert(), name="convert1",
                                         requirements=[mrtrix3_req])
        mrconvert2 = pipeline.create_node(MRConvert(), name="convert2",
                                          requirements=[mrtrix3_req])
        # Connect inputs
        pipeline.connect_input('start', mrconvert, 'in_file')
        pipeline.connect_input('start', mrconvert2, 'in_file')
        # Connect outputs
        pipeline.connect_output('pipeline1_1', mrconvert, 'out_file')
        pipeline.connect_output('pipeline1_2', mrconvert2, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    def pipeline2(self):
        pipeline = self.create_pipeline(
            name='pipeline2',
            inputs=[DatasetSpec('start', nifti_gz_format),
                    DatasetSpec('pipeline1_1', nifti_gz_format)],
            outputs=[DatasetSpec('pipeline2', nifti_gz_format)],
            description="A dummy pipeline used to test 'run_pipeline' method",
            default_options={},
            version=1,
            citations=[],)
        mrmath = pipeline.create_node(MRCat(), name="mrcat",
                                      requirements=[mrtrix3_req])
        mrmath.inputs.axis = 0
        # Connect inputs
        pipeline.connect_input('start', mrmath, 'first_scan')
        pipeline.connect_input('pipeline1_1', mrmath, 'second_scan')
        # Connect outputs
        pipeline.connect_output('pipeline2', mrmath, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    def pipeline3(self):
        pipeline = self.create_pipeline(
            name='pipeline3',
            inputs=[DatasetSpec('pipeline2', nifti_gz_format)],
            outputs=[DatasetSpec('pipeline3', nifti_gz_format)],
            description="A dummy pipeline used to test 'run_pipeline' method",
            default_options={},
            version=1,
            citations=[])
        mrconvert = pipeline.create_node(MRConvert(), name="convert")
        # Connect inputs
        pipeline.connect_input('pipeline2', mrconvert, 'in_file')
        # Connect outputs
        pipeline.connect_output('pipeline3', mrconvert, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    def pipeline4(self):
        pipeline = self.create_pipeline(
            name='pipeline4',
            inputs=[DatasetSpec('pipeline1_2', nifti_gz_format),
                    DatasetSpec('pipeline3', nifti_gz_format)],
            outputs=[DatasetSpec('pipeline4', nifti_gz_format)],
            description="A dummy pipeline used to test 'run_pipeline' method",
            default_options={},
            version=1,
            citations=[])
        mrmath = pipeline.create_node(MRCat(), name="mrcat",
                                      requirements=[mrtrix3_req])
        mrmath.inputs.axis = 0
        # Connect inputs
        pipeline.connect_input('pipeline1_2', mrmath, 'first_scan')
        pipeline.connect_input('pipeline3', mrmath, 'second_scan')
        # Connect outputs
        pipeline.connect_output('pipeline4', mrmath, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    def session_ids_access_pipeline(self):
        pipeline = self.create_pipeline(
            name='session_ids_access',
            inputs=[],
            outputs=[DatasetSpec('session_ids', text_format)],
            description=(
                "A dummy pipeline used to test access to 'session' IDs"),
            default_options={},
            version=1,
            citations=[])
        sessions_to_file = pipeline.create_join_sessions_node(
            IteratorToFile(), name='sessions_to_file', joinfield='ids')
        pipeline.connect_session_id(sessions_to_file, 'ids')
        pipeline.connect_output('session_ids', sessions_to_file, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    def subject_ids_access_pipeline(self):
        pipeline = self.create_pipeline(
            name='subject_ids_access',
            inputs=[],
            outputs=[DatasetSpec('subject_ids', text_format)],
            description=(
                "A dummy pipeline used to test access to 'subject' IDs"),
            default_options={},
            version=1,
            citations=[])
        subjects_to_file = pipeline.create_join_subjects_node(
            IteratorToFile(), name='subjects_to_file', joinfield='ids')
        pipeline.connect_subject_id(subjects_to_file, 'ids')
        pipeline.connect_output('subject_ids', subjects_to_file, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    def subject_summary_pipeline(self):
        pipeline = self.create_pipeline(
            name="subject_summary",
            inputs=[DatasetSpec('ones_slice', mrtrix_format)],
            outputs=[DatasetSpec('subject_summary', mrtrix_format)],
            description=("Test of project summary variables"),
            default_options={},
            version=1,
            citations=[],)
        mrmath = pipeline.create_join_sessions_node(
            MRMath(), 'in_files', 'mrmath', requirements=[mrtrix3_req])
        mrmath.inputs.operation = 'sum'
        # Connect inputs
        pipeline.connect_input('ones_slice', mrmath, 'in_files')
        # Connect outputs
        pipeline.connect_output('subject_summary', mrmath, 'out_file')
        pipeline.assert_connected()
        return pipeline

    def timepoint_summary_pipeline(self):
        pipeline = self.create_pipeline(
            name="timepoint_summary",
            inputs=[DatasetSpec('ones_slice', mrtrix_format)],
            outputs=[DatasetSpec('timepoint_summary', mrtrix_format)],
            description=("Test of project summary variables"),
            default_options={},
            version=1,
            citations=[],)
        mrmath = pipeline.create_join_sessions_node(
            MRMath(), 'in_files', 'mrmath', requirements=[mrtrix3_req])
        mrmath.inputs.operation = 'sum'
        # Connect inputs
        pipeline.connect_input('ones_slice', mrmath, 'in_files')
        # Connect outputs
        pipeline.connect_output('timepoint_summary', mrmath, 'out_file')
        pipeline.assert_connected()
        return pipeline

    def project_summary_pipeline(self):
        pipeline = self.create_pipeline(
            name="project_summary",
            inputs=[DatasetSpec('ones_slice', mrtrix_format)],
            outputs=[DatasetSpec('project_summary', mrtrix_format)],
            description=("Test of project summary variables"),
            default_options={},
            version=1,
            citations=[],)
        mrmath1 = pipeline.create_join_sessions_node(
            MRMath(), 'in_files', 'mrmath1', requirements=[mrtrix3_req])
        mrmath2 = pipeline.create_join_subjects_node(
            MRMath(), 'in_files', 'mrmath2', requirements=[mrtrix3_req])
        mrmath1.inputs.operation = 'sum'
        mrmath2.inputs.operation = 'sum'
        # Connect inputs
        pipeline.connect_input('ones_slice', mrmath1, 'in_files')
        pipeline.connect(mrmath1, 'out_file', mrmath2, 'in_files')
        # Connect outputs
        pipeline.connect_output('project_summary', mrmath2, 'out_file')
        pipeline.assert_connected()
        return pipeline

    _dataset_specs = set_dataset_specs(
        DatasetSpec('start', nifti_gz_format),
        DatasetSpec('ones_slice', mrtrix_format),
        DatasetSpec('pipeline1_1', nifti_gz_format, pipeline1),
        DatasetSpec('pipeline1_2', nifti_gz_format, pipeline1),
        DatasetSpec('pipeline2', nifti_gz_format, pipeline2),
        DatasetSpec('pipeline3', nifti_gz_format, pipeline3),
        DatasetSpec('pipeline4', nifti_gz_format, pipeline4),
        DatasetSpec('subject_summary', mrtrix_format, subject_summary_pipeline,
                    multiplicity='per_subject'),
        DatasetSpec('timepoint_summary', mrtrix_format,
                    timepoint_summary_pipeline,
                    multiplicity='per_timepoint'),
        DatasetSpec('project_summary', mrtrix_format, project_summary_pipeline,
                    multiplicity='per_project'),
        DatasetSpec('subject_ids', text_format, subject_ids_access_pipeline,
                    multiplicity='per_timepoint'),
        DatasetSpec('session_ids', text_format, session_ids_access_pipeline,
                    multiplicity='per_subject'))


class IteratorToFileInputSpec(TraitedSpec):
    ids = traits.List(traits.Str(), desc="ID of the iterable")
    out_file = File(genfile=True, desc="The name of the generated file")


class IteratorToFileOutputSpec(TraitedSpec):
    out_file = File(exists=True, desc="Output file containing iterables")


class IteratorToFile(BaseInterface):

    input_spec = IteratorToFileInputSpec
    output_spec = IteratorToFileOutputSpec

    def _run_interface(self, runtime):
        with open(self._gen_filename('out_file'), 'w') as f:
            f.write('\n'.join(str(i) for i in self.inputs.ids))
        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['out_file'] = self._gen_filename('out_file')
        return outputs

    def _gen_filename(self, name):
        if name == 'out_file':
            if isdefined(self.inputs.out_file):
                fname = self.inputs.out_file
            else:
                fname = os.path.join(os.getcwd(), 'out.txt')
        else:
            assert False
        return fname


class TestRunPipeline(BaseTestCase):

    SUBJECT_IDS = ['SUBJECTID1', 'SUBJECTID2', 'SUBJECTID3']
    SESSION_IDS = ['SESSIONID1', 'SESSIONID2']

    def setUp(self):
        try:
            NiAnalysisNodeMixin.load_module('mrtrix')
        except NiAnalysisModulesNotInstalledException:
            pass
        self.reset_dirs()
        for subject_id in self.SUBJECT_IDS:
            for session_id in self.SESSION_IDS:
                self.add_session(self.project_dir, subject_id, session_id)
        self.study = self.create_study(
            DummyStudy, 'dummy', input_datasets={
                'start': Dataset('start', nifti_gz_format),
                'ones_slice': Dataset('ones_slice', mrtrix_format)})

    def tearDown(self):
        try:
            NiAnalysisNodeMixin.unload_module('mrtrix')
        except NiAnalysisModulesNotInstalledException:
            pass

    def test_pipeline_prerequisites(self):
        pipeline = self.study.pipeline4()
        pipeline.run(work_dir=self.work_dir)
        for dataset in DummyStudy.dataset_specs():
            if dataset.multiplicity == 'per_session' and dataset.processed:
                for subject_id in self.SUBJECT_IDS:
                    for session_id in self.SESSION_IDS:
                        self.assertDatasetCreated(
                            dataset.name + dataset.format.extension,
                            self.study.name, subject=subject_id,
                            session=session_id)

    def test_subject_summary(self):
        self.study.subject_summary_pipeline().run(work_dir=self.work_dir)
        for subject_id in self.SUBJECT_IDS:
            # Get mean value from resultant image (should be the same as the
            # number of sessions as the original image is full of ones and
            # all sessions have been summed together
            mean_val = float(sp.check_output(
                'mrstats {} -output mean'.format(
                    self.output_file_path(
                        'subject_summary.mif', self.study.name,
                        subject=subject_id, multiplicity='per_subject')),
                shell=True))
            self.assertEqual(mean_val, len(self.SESSION_IDS))

    def test_timepoint_summary(self):
        self.study.timepoint_summary_pipeline().run(work_dir=self.work_dir)
        for timepoint_id in self.SESSION_IDS:
            # Get mean value from resultant image (should be the same as the
            # number of sessions as the original image is full of ones and
            # all sessions have been summed together
            mean_val = float(sp.check_output(
                'mrstats {} -output mean'.format(
                    self.output_file_path(
                        'timepoint_summary.mif', self.study.name,
                        session=timepoint_id, multiplicity='per_timepoint')),
                shell=True))
            self.assertEqual(mean_val, len(self.SESSION_IDS))

    def test_project_summary(self):
        self.study.project_summary_pipeline().run(work_dir=self.work_dir)
        # Get mean value from resultant image (should be the same as the
        # number of sessions as the original image is full of ones and
        # all sessions have been summed together
        mean_val = float(sp.check_output(
            'mrstats {} -output mean'.format(self.output_file_path(
                'project_summary.mif', self.study.name,
                multiplicity='per_project')),
            shell=True))
        self.assertEqual(mean_val,
                         len(self.SUBJECT_IDS) * len(self.SESSION_IDS))

    def test_subject_ids_access(self):
        self.study.subject_ids_access_pipeline().run(work_dir=self.work_dir)
        for session_id in self.SESSION_IDS:
            subject_ids_path = self.output_file_path(
                'subject_ids.txt', self.study.name,
                session=session_id, multiplicity='per_timepoint')
            with open(subject_ids_path) as f:
                ids = f.read().split('\n')
            self.assertEqual(sorted(ids), sorted(self.SUBJECT_IDS))

    def test_session_ids_access(self):
        self.study.session_ids_access_pipeline().run(work_dir=self.work_dir)
        for subject_id in self.SUBJECT_IDS:
            session_ids_path = self.output_file_path(
                'session_ids.txt', self.study.name,
                subject=subject_id, multiplicity='per_subject')
            with open(session_ids_path) as f:
                ids = f.read().split('\n')
            self.assertEqual(sorted(ids), sorted(self.SESSION_IDS))
