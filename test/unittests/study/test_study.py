import os.path
# from nipype import config
# config.enable_debug_mode()
import subprocess as sp  # @IgnorePep8
from nianalysis.requirement import Requirement, mrtrix3_req
from arcana.dataset import DatasetMatch, DatasetSpec  # @IgnorePep8
from nianalysis.data_format import (
    nifti_gz_format, mrtrix_format, text_format)  # @IgnorePep8
from nipype.interfaces.utility import Merge  # @IgnorePep8
from arcana.study.base import Study, StudyMetaClass  # @IgnorePep8
from arcana.interfaces.mrtrix import MRConvert, MRCat, MRMath, MRCalc  # @IgnorePep8
from arcana.testing import BaseTestCase, BaseMultiSubjectTestCase  # @IgnorePep8
from arcana.node import ArcanaNodeMixin  # @IgnorePep8
from arcana.exception import ArcanaModulesNotInstalledException  # @IgnorePep8
from nipype.interfaces.base import (  # @IgnorePep8
    BaseInterface, File, TraitedSpec, traits, isdefined)
from arcana.option import OptionSpec


class TestStudy(Study):

    __metaclass__ = StudyMetaClass

    add_data_specs = [
        DatasetSpec('start', nifti_gz_format),
        DatasetSpec('ones_slice', mrtrix_format),
        DatasetSpec('derived1_1', nifti_gz_format, 'pipeline1'),
        DatasetSpec('derived1_2', nifti_gz_format, 'pipeline1'),
        DatasetSpec('derived2', nifti_gz_format, 'pipeline2'),
        DatasetSpec('derived3', nifti_gz_format, 'pipeline3'),
        DatasetSpec('derived4', nifti_gz_format, 'pipeline4'),
        DatasetSpec('subject_summary', mrtrix_format,
                    'subject_summary_pipeline',
                    frequency='per_subject'),
        DatasetSpec('visit_summary', mrtrix_format,
                    'visit_summary_pipeline',
                    frequency='per_visit'),
        DatasetSpec('project_summary', mrtrix_format,
                    'project_summary_pipeline',
                    frequency='per_project'),
        DatasetSpec('subject_ids', text_format,
                    'subject_ids_access_pipeline',
                    frequency='per_visit'),
        DatasetSpec('visit_ids', text_format,
                    'visit_ids_access_pipeline',
                    frequency='per_subject')]

    add_option_specs = [
        OptionSpec('pipeline_option', False)]

    def pipeline1(self, **kwargs):
        pipeline = self.create_pipeline(
            name='pipeline1',
            inputs=[DatasetSpec('start', nifti_gz_format)],
            outputs=[DatasetSpec('derived1_1', nifti_gz_format),
                     DatasetSpec('derived1_2', nifti_gz_format)],
            desc="A dummy pipeline used to test 'run_pipeline' method",
            version=1,
            citations=[],
            **kwargs)
        if not pipeline.option('pipeline_option'):
            raise Exception("Pipeline option was not cascaded down to "
                            "pipeline1")
        mrconvert = pipeline.create_node(MRConvert(), name="convert1",
                                         requirements=[mrtrix3_req])
        mrconvert2 = pipeline.create_node(MRConvert(), name="convert2",
                                          requirements=[mrtrix3_req])
        # Connect inputs
        pipeline.connect_input('start', mrconvert, 'in_file')
        pipeline.connect_input('start', mrconvert2, 'in_file')
        # Connect outputs
        pipeline.connect_output('derived1_1', mrconvert, 'out_file')
        pipeline.connect_output('derived1_2', mrconvert2, 'out_file')
        return pipeline

    def pipeline2(self, **kwargs):
        pipeline = self.create_pipeline(
            name='pipeline2',
            inputs=[DatasetSpec('start', nifti_gz_format),
                    DatasetSpec('derived1_1', nifti_gz_format)],
            outputs=[DatasetSpec('derived2', nifti_gz_format)],
            desc="A dummy pipeline used to test 'run_pipeline' method",
            version=1,
            citations=[],
            **kwargs)
        if not pipeline.option('pipeline_option'):
            raise Exception("Pipeline option was not cascaded down to "
                            "pipeline2")
        mrmath = pipeline.create_node(MRCat(), name="mrcat",
                                      requirements=[mrtrix3_req])
        mrmath.inputs.axis = 0
        # Connect inputs
        pipeline.connect_input('start', mrmath, 'first_scan')
        pipeline.connect_input('derived1_1', mrmath, 'second_scan')
        # Connect outputs
        pipeline.connect_output('derived2', mrmath, 'out_file')
        return pipeline

    def pipeline3(self, **kwargs):
        pipeline = self.create_pipeline(
            name='pipeline3',
            inputs=[DatasetSpec('derived2', nifti_gz_format)],
            outputs=[DatasetSpec('derived3', nifti_gz_format)],
            desc="A dummy pipeline used to test 'run_pipeline' method",
            version=1,
            citations=[],
            **kwargs)
        mrconvert = pipeline.create_node(MRConvert(), name="convert",
                                         requirements=[mrtrix3_req])
        # Connect inputs
        pipeline.connect_input('derived2', mrconvert, 'in_file')
        # Connect outputs
        pipeline.connect_output('derived3', mrconvert, 'out_file')
        return pipeline

    def pipeline4(self, **kwargs):
        pipeline = self.create_pipeline(
            name='pipeline4',
            inputs=[DatasetSpec('derived1_2', nifti_gz_format),
                    DatasetSpec('derived3', nifti_gz_format)],
            outputs=[DatasetSpec('derived4', nifti_gz_format)],
            desc="A dummy pipeline used to test 'run_pipeline' method",
            version=1,
            citations=[],
            **kwargs)
        mrmath = pipeline.create_node(MRCat(), name="mrcat",
                                      requirements=[mrtrix3_req])
        mrmath.inputs.axis = 0
        # Connect inputs
        pipeline.connect_input('derived1_2', mrmath, 'first_scan')
        pipeline.connect_input('derived3', mrmath, 'second_scan')
        # Connect outputs
        pipeline.connect_output('derived4', mrmath, 'out_file')
        return pipeline

    def visit_ids_access_pipeline(self, **kwargs):
        pipeline = self.create_pipeline(
            name='visit_ids_access',
            inputs=[],
            outputs=[DatasetSpec('visit_ids', text_format)],
            desc=(
                "A dummy pipeline used to test access to 'session' IDs"),
            version=1,
            citations=[],
            **kwargs)
        sessions_to_file = pipeline.create_join_visits_node(
            IteratorToFile(), name='sess_to_file', joinfield='ids')
        pipeline.connect_visit_id(sessions_to_file, 'ids')
        pipeline.connect_output('visit_ids', sessions_to_file,
                                'out_file')
        return pipeline

    def subject_ids_access_pipeline(self, **kwargs):
        pipeline = self.create_pipeline(
            name='subject_ids_access',
            inputs=[],
            outputs=[DatasetSpec('subject_ids', text_format)],
            desc=(
                "A dummy pipeline used to test access to 'subject' IDs"),
            version=1,
            citations=[],
            **kwargs)
        subjects_to_file = pipeline.create_join_subjects_node(
            IteratorToFile(), name='subjects_to_file', joinfield='ids')
        pipeline.connect_subject_id(subjects_to_file, 'ids')
        pipeline.connect_output('subject_ids', subjects_to_file,
                                'out_file')
        return pipeline

    def subject_summary_pipeline(self, **kwargs):
        pipeline = self.create_pipeline(
            name="subject_summary",
            inputs=[DatasetSpec('ones_slice', mrtrix_format)],
            outputs=[DatasetSpec('subject_summary', mrtrix_format)],
            desc=("Test of project summary variables"),
            version=1,
            citations=[],
            **kwargs)
        mrmath = pipeline.create_join_visits_node(
            MRMath(), 'in_files', 'mrmath', requirements=[mrtrix3_req])
        mrmath.inputs.operation = 'sum'
        # Connect inputs
        pipeline.connect_input('ones_slice', mrmath, 'in_files')
        # Connect outputs
        pipeline.connect_output('subject_summary', mrmath, 'out_file')
        pipeline.assert_connected()
        return pipeline

    def visit_summary_pipeline(self, **kwargs):
        pipeline = self.create_pipeline(
            name="visit_summary",
            inputs=[DatasetSpec('ones_slice', mrtrix_format)],
            outputs=[DatasetSpec('visit_summary', mrtrix_format)],
            desc=("Test of project summary variables"),
            version=1,
            citations=[],
            **kwargs)
        mrmath = pipeline.create_join_visits_node(
            MRMath(), 'in_files', 'mrmath', requirements=[mrtrix3_req])
        mrmath.inputs.operation = 'sum'
        # Connect inputs
        pipeline.connect_input('ones_slice', mrmath, 'in_files')
        # Connect outputs
        pipeline.connect_output('visit_summary', mrmath, 'out_file')
        pipeline.assert_connected()
        return pipeline

    def project_summary_pipeline(self, **kwargs):
        pipeline = self.create_pipeline(
            name="project_summary",
            inputs=[DatasetSpec('ones_slice', mrtrix_format)],
            outputs=[DatasetSpec('project_summary', mrtrix_format)],
            desc=("Test of project summary variables"),
            version=1,
            citations=[],
            **kwargs)
        mrmath1 = pipeline.create_join_visits_node(
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
        self.reset_dirs()
        for subject_id in self.SUBJECT_IDS:
            for visit_id in self.SESSION_IDS:
                self.add_session(self.project_dir, subject_id, visit_id)
        self.study = self.create_study(
            TestStudy, 'dummy', inputs=[
                DatasetMatch('start', nifti_gz_format, 'start'),
                DatasetMatch('ones_slice', mrtrix_format, 'ones_slice')],
            options={'pipeline_option': True})
        # Calculate MRtrix module required for 'mrstats' commands
        try:
            self.mrtrix_req = Requirement.best_requirement(
                [mrtrix3_req], ArcanaNodeMixin.available_modules(),
                ArcanaNodeMixin.preloaded_modules())
        except ArcanaModulesNotInstalledException:
            self.mrtrix_req = None

    def tearDown(self):
        try:
            ArcanaNodeMixin.unload_module('mrtrix')
        except ArcanaModulesNotInstalledException:
            pass

    def test_pipeline_prerequisites(self):
        self.study.data('derived4')[0]
        for dataset in TestStudy.data_specs():
            if dataset.frequency == 'per_session' and dataset.derived:
                for subject_id in self.SUBJECT_IDS:
                    for visit_id in self.SESSION_IDS:
                        self.assertDatasetCreated(
                            dataset.name + dataset.format.extension,
                            self.study.name, subject=subject_id,
                            visit=visit_id)

    def test_subject_summary(self):
        self.study.data('subject_summary')
        for subject_id in self.SUBJECT_IDS:
            # Get mean value from resultant image (should be the same as the
            # number of sessions as the original image is full of ones and
            # all sessions have been summed together
            if self.mrtrix_req is not None:
                ArcanaNodeMixin.load_module(*self.mrtrix_req)
            try:
                mean_val = float(sp.check_output(
                    'mrstats {} -output mean'.format(
                        self.output_file_path(
                            'subject_summary.mif', self.study.name,
                            subject=subject_id,
                            frequency='per_subject')),
                    shell=True))
                self.assertEqual(mean_val, len(self.SESSION_IDS))
            finally:
                if self.mrtrix_req is not None:
                    ArcanaNodeMixin.unload_module(*self.mrtrix_req)

    def test_visit_summary(self):
        self.study.data('visit_summary')
        for visit_id in self.SESSION_IDS:
            # Get mean value from resultant image (should be the same as the
            # number of sessions as the original image is full of ones and
            # all sessions have been summed together
            if self.mrtrix_req is not None:
                ArcanaNodeMixin.load_module(*self.mrtrix_req)
            try:
                mean_val = float(sp.check_output(
                    'mrstats {} -output mean'.format(
                        self.output_file_path(
                            'visit_summary.mif', self.study.name,
                            visit=visit_id, frequency='per_visit')),
                    shell=True))
                self.assertEqual(mean_val, len(self.SESSION_IDS))
            finally:
                if self.mrtrix_req is not None:
                    ArcanaNodeMixin.unload_module(*self.mrtrix_req)

    def test_project_summary(self):
        self.study.data('project_summary')
        # Get mean value from resultant image (should be the same as the
        # number of sessions as the original image is full of ones and
        # all sessions have been summed together
        if self.mrtrix_req is not None:
            ArcanaNodeMixin.load_module(*self.mrtrix_req)
        try:
            mean_val = float(sp.check_output(
                'mrstats {} -output mean'.format(self.output_file_path(
                    'project_summary.mif', self.study.name,
                    frequency='per_project')),
                shell=True))
            self.assertEqual(mean_val,
                             len(self.SUBJECT_IDS) * len(self.SESSION_IDS))
        finally:
            if self.mrtrix_req is not None:
                ArcanaNodeMixin.unload_module(*self.mrtrix_req)

    def test_subject_ids_access(self):
        self.study.data('subject_ids')
        for visit_id in self.SESSION_IDS:
            subject_ids_path = self.output_file_path(
                'subject_ids.txt', self.study.name,
                visit=visit_id, frequency='per_visit')
            with open(subject_ids_path) as f:
                ids = f.read().split('\n')
            self.assertEqual(sorted(ids), sorted(self.SUBJECT_IDS))

    def test_visit_ids_access(self):
        self.study.data('visit_ids')
        for subject_id in self.SUBJECT_IDS:
            visit_ids_path = self.output_file_path(
                'visit_ids.txt', self.study.name,
                subject=subject_id, frequency='per_subject')
            with open(visit_ids_path) as f:
                ids = f.read().split('\n')
            self.assertEqual(sorted(ids), sorted(self.SESSION_IDS))


class ExistingPrereqStudy(Study):

    __metaclass__ = StudyMetaClass

    add_data_specs = [
        DatasetSpec('start', mrtrix_format),
        DatasetSpec('tens', mrtrix_format, 'tens_pipeline'),
        DatasetSpec('hundreds', mrtrix_format, 'hundreds_pipeline'),
        DatasetSpec('thousands', mrtrix_format, 'thousands_pipeline')]

    def pipeline_factory(self, incr, input, output):  # @ReservedAssignment
        pipeline = self.create_pipeline(
            name=output,
            inputs=[DatasetSpec(input, mrtrix_format)],
            outputs=[DatasetSpec(output, mrtrix_format)],
            desc=(
                "A dummy pipeline used to test 'partial-complete' method"),
            version=1,
            citations=[])
        # Nodes
        operands = pipeline.create_node(Merge(2), name='merge')
        mult = pipeline.create_node(MRCalc(), name="convert1",
                                    requirements=[mrtrix3_req])
        operands.inputs.in2 = incr
        mult.inputs.operation = 'add'
        # Connect inputs
        pipeline.connect_input(input, operands, 'in1')
        # Connect inter-nodes
        pipeline.connect(operands, 'out', mult, 'operands')
        # Connect outputs
        pipeline.connect_output(output, mult, 'out_file')
        return pipeline

    def tens_pipeline(self, **kwargs):  # @UnusedVariable
        return self.pipeline_factory(10, 'start', 'tens')

    def hundreds_pipeline(self, **kwargs):  # @UnusedVariable
        return self.pipeline_factory(100, 'tens', 'hundreds')

    def thousands_pipeline(self, **kwargs):  # @UnusedVariable
        return self.pipeline_factory(1000, 'hundreds', 'thousands')


class TestExistingPrereqs(BaseMultiSubjectTestCase):
    """
    This unittest tests out that partially previously calculated prereqs
    are detected and not rerun unless reprocess==True.

    The structure of the "subjects" and "sessions" stored on the XNAT archive
    is:


    -- subject1 -- visit1 -- ones
     |           |         |
     |           |         - tens
     |           |         |
     |           |         - hundreds
     |           |
     |           - visit2 -- ones
     |           |         |
     |           |         - tens
     |           |
     |           - visit3 -- ones
     |                     |
     |                     - hundreds
     |                     |
     |                     - thousands
     |
     - subject2 -- visit1 -- ones
     |           |         |
     |           |         - tens
     |           |
     |           - visit2 -- ones
     |           |         |
     |           |         - tens
     |           |
     |           - visit3 -- ones
     |                     |
     |                     - tens
     |                     |
     |                     - hundreds
     |                     |
     |                     - thousands
     |
     - subject3 -- visit1 -- ones
     |           |
     |           - visit2 -- ones
     |           |         |
     |           |         - tens
     |           |
     |           - visit3 -- ones
     |                     |
     |                     - tens
     |                     |
     |                     - thousands
     |
     - subject4 -- visit1 -- ones
                 |
                 - visit2 -- ones
                 |         |
                 |         - tens
                 |
                 - visit3 -- ones
                           |
                           - tens
                           |
                           - hundreds
                           |
                           - thousands

    For prexisting sessions the values in the existing images are multiplied by
    5, i.e. preexisting tens actually contains voxels of value 50, hundreds 500
    """

    saved_structure = {
        'subject1': {
            'visit1': ['ones', 'tens', 'hundreds'],
            'visit2': ['ones', 'tens'],
            'visit3': ['ones', 'hundreds', 'thousands']},
        'subject2': {
            'visit1': ['ones', 'tens'],
            'visit2': ['ones', 'tens'],
            'visit3': ['ones', 'tens', 'hundreds', 'thousands']},
        'subject3': {
            'visit1': ['ones'],
            'visit2': ['ones', 'tens'],
            'visit3': ['ones', 'tens', 'thousands']},
        'subject4': {
            'visit1': ['ones'],
            'visit2': ['ones', 'tens'],
            'visit3': ['ones', 'tens', 'hundreds', 'thousands']}}

    study_name = 'existing'

    def test_per_session_prereqs(self):
        study = self.create_study(
            ExistingPrereqStudy, self.study_name, inputs=[
                DatasetMatch('start', mrtrix_format, 'ones')])
        study.data('thousands')
        targets = {
            'subject1': {
                'visit1': 1100,
                'visit2': 1110,
                'visit3': 1000},
            'subject2': {
                'visit1': 1110,
                'visit2': 1110,
                'visit3': 1000},
            'subject3': {
                'visit1': 1111,
                'visit2': 1110,
                'visit3': 1000},
            'subject4': {
                'visit1': 1111,
                'visit2': 1110,
                'visit3': 1000}}
        for subj_id, visits in self.saved_structure.iteritems():
            for visit_id in visits:
                self.assertStatEqual('mean', 'thousands.mif',
                                     targets[subj_id][visit_id],
                                     self.study_name,
                                     subject=subj_id, visit=visit_id,
                                     frequency='per_session')

#     def test_explicit_prereqs(self):
#         study = self.create_study(
#             ExistingPrereqStudy, self.study_name, inputs=[
#                 DatasetMatch('ones', mrtrix_format, 'ones')])
#         study.data('thousands')
#         targets = {
#             'subject1': {
#                 'visit1': 1100,
#                 'visit2': 1110,
#                 'visit3': 1000},
#             'subject2': {
#                 'visit1': 1110,
#                 'visit2': 1110,
#                 'visit3': 1000},
#             'subject3': {
#                 'visit1': 1111,
#                 'visit2': 1110,
#                 'visit3': 1000},
#             'subject4': {
#                 'visit1': 1111,
#                 'visit2': 1110,
#                 'visit3': 1000}}
#         for subj_id, visits in self.saved_structure.iteritems():
#             for visit_id in visits:
#                 self.assertStatEqual('mean', 'thousands.mif',
#                                      targets[subj_id][visit_id],
#                                      self.study_name,
#                                      subject=subj_id, session=visit_id,
#                                      frequency='per_session')
