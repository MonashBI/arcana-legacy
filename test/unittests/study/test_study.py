import subprocess as sp
from nipype.pipeline import engine as pe
from nianalysis.dataset import Dataset, DatasetSpec
from nianalysis.data_formats import nifti_gz_format, mrtrix_format
from nianalysis.requirements import mrtrix3_req
from nianalysis.study.base import Study, set_dataset_specs
from nianalysis.interfaces.mrtrix import MRConvert, MRCat, MRMath
from nianalysis.testing import PipelineTestCase
import logging

logger = logging.getLogger('NiAnalysis')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


class DummyStudy(Study):

    def pipeline1(self):
        pipeline = self._create_pipeline(
            name='pipeline1',
            inputs=['start'],
            outputs=['pipeline1_1', 'pipeline1_2'],
            description="A dummy pipeline used to test 'run_pipeline' method",
            default_options={},
            version=1,
            requirements=[mrtrix3_req],
            citations=[],
            approx_runtime=1)
        mrconvert = pe.Node(MRConvert(), name="convert1")
        mrconvert2 = pe.Node(MRConvert(), name="convert2")
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
        pipeline = self._create_pipeline(
            name='pipeline2',
            inputs=['start', 'pipeline1_1'],
            outputs=['pipeline2'],
            description="A dummy pipeline used to test 'run_pipeline' method",
            default_options={},
            version=1,
            requirements=[mrtrix3_req],
            citations=[],
            approx_runtime=1)
        mrmath = pe.Node(MRCat(), name="mrcat")
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
        pipeline = self._create_pipeline(
            name='pipeline3',
            inputs=['pipeline2'],
            outputs=['pipeline3'],
            description="A dummy pipeline used to test 'run_pipeline' method",
            default_options={},
            version=1,
            requirements=[mrtrix3_req],
            citations=[],
            approx_runtime=1)
        mrconvert = pe.Node(MRConvert(), name="convert")
        # Connect inputs
        pipeline.connect_input('pipeline2', mrconvert, 'in_file')
        # Connect outputs
        pipeline.connect_output('pipeline3', mrconvert, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    def pipeline4(self):
        pipeline = self._create_pipeline(
            name='pipeline4',
            inputs=['pipeline3'],
            outputs=['pipeline4'],
            description="A dummy pipeline used to test 'run_pipeline' method",
            default_options={},
            version=1,
            requirements=[mrtrix3_req],
            citations=[],
            approx_runtime=1)
        mrconvert = pe.Node(MRConvert(), name="convert")
        # Connect inputs
        pipeline.connect_input('pipeline3', mrconvert, 'in_file')
        # Connect outputs
        pipeline.connect_output('pipeline4', mrconvert, 'out_file')
        # Check inputs/outputs are connected
        pipeline.assert_connected()
        return pipeline

    def subject_summary_pipeline(self):
        pipeline = self._create_pipeline(
            name="subject_summary",
            inputs=['ones_slice'],
            outputs=["subject_summary"],
            description=("Test of project summary variables"),
            default_options={},
            version=1,
            requirements=[mrtrix3_req],
            citations=[],
            approx_runtime=1)
        mrmath = pe.JoinNode(MRMath(), joinsource='sessions',
                             joinfield=['in_files'], name='mrmath')
        mrmath.inputs.operation = 'sum'
        # Connect inputs
        pipeline.connect_input('ones_slice', mrmath, 'in_files')
        # Connect outputs
        pipeline.connect_output('subject_summary', mrmath, 'out_file')
        pipeline.assert_connected()
        return pipeline

    def project_summary_pipeline(self):
        pipeline = self._create_pipeline(
            name="project_summary",
            inputs=['ones_slice'],
            outputs=["project_summary"],
            description=("Test of project summary variables"),
            default_options={},
            version=1,
            requirements=[mrtrix3_req],
            citations=[],
            approx_runtime=1)
        mrmath1 = pe.JoinNode(MRMath(), joinsource='sessions',
                              joinfield=['in_files'], name='mrmath1')
        mrmath2 = pe.JoinNode(MRMath(), joinsource='subjects',
                              joinfield=['in_files'], name='mrmath2')
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
        DatasetSpec('project_summary', mrtrix_format, project_summary_pipeline,
                    multiplicity='per_project'))


class TestRunPipeline(PipelineTestCase):

    SUBJECT_IDS = ['SUBJECTID1', 'SUBJECTID2', 'SUBJECTID3']
    SESSION_IDS = ['SESSIONID1', 'SESSIONID2']

    def setUp(self):
        self.delete_project(self.project_dir)
        for subject_id in self.SUBJECT_IDS:
            for session_id in self.SESSION_IDS:
                self.add_session(self.project_dir, subject_id, session_id)
        self.study = self.create_study(
            DummyStudy, 'dummy', input_datasets={
                'start': Dataset('start', nifti_gz_format),
                'ones_slice': Dataset('ones_slice', mrtrix_format)})

    def test_pipeline_prerequisites(self):
        self.study.pipeline4().run(work_dir=self.work_dir)
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
