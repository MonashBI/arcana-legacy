import os.path
# from nipype import config
# config.enable_debug_mode()
import subprocess as sp  # @IgnorePep8
import cPickle as pkl
from arcana.dataset import DatasetMatch, DatasetSpec  # @IgnorePep8
from arcana.data_format import text_format  # @IgnorePep8
from nipype.interfaces.utility import Merge  # @IgnorePep8
from arcana.study.base import Study, StudyMetaClass  # @IgnorePep8
from arcana.interfaces.mrtrix import MRConvert, MRCat, MRMath, MRCalc  # @IgnorePep8
from arcana.testing import BaseTestCase, BaseMultiSubjectTestCase  # @IgnorePep8
from arcana.node import ArcanaNodeMixin  # @IgnorePep8
from arcana.exception import (
    ArcanaModulesNotInstalledException, ArcanaCantPickleStudyError)  # @IgnorePep8
from arcana.study.multi import (
    MultiStudy, MultiStudyMetaClass, SubStudySpec)
from nipype.interfaces.base import (  # @IgnorePep8
    BaseInterface, File, TraitedSpec, traits, isdefined)
from arcana.option import OptionSpec
from arcana.data_format import DataFormat, IdentityConverter
from nipype.interfaces.utility import IdentityInterface
from arcana.exception import ArcanaNoConverterError


class ExampleStudy(Study):

    __metaclass__ = StudyMetaClass

    add_data_specs = [
        DatasetSpec('start', text_format),
        DatasetSpec('ones_slice', text_format),
        DatasetSpec('derived1_1', text_format, 'pipeline1'),
        DatasetSpec('derived1_2', text_format, 'pipeline1'),
        DatasetSpec('derived2', text_format, 'pipeline2'),
        DatasetSpec('derived3', text_format, 'pipeline3'),
        DatasetSpec('derived4', text_format, 'pipeline4'),
        DatasetSpec('subject_summary', text_format,
                    'subject_summary_pipeline',
                    frequency='per_subject'),
        DatasetSpec('visit_summary', text_format,
                    'visit_summary_pipeline',
                    frequency='per_visit'),
        DatasetSpec('project_summary', text_format,
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
            inputs=[DatasetSpec('start', text_format)],
            outputs=[DatasetSpec('derived1_1', text_format),
                     DatasetSpec('derived1_2', text_format)],
            desc="A dummy pipeline used to test 'run_pipeline' method",
            version=1,
            citations=[],
            **kwargs)
        if not pipeline.option('pipeline_option'):
            raise Exception("Pipeline option was not cascaded down to "
                            "pipeline1")
        mrconvert = pipeline.create_node(MRConvert(), name="convert1")
        mrconvert2 = pipeline.create_node(MRConvert(), name="convert2")
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
            inputs=[DatasetSpec('start', text_format),
                    DatasetSpec('derived1_1', text_format)],
            outputs=[DatasetSpec('derived2', text_format)],
            desc="A dummy pipeline used to test 'run_pipeline' method",
            version=1,
            citations=[],
            **kwargs)
        if not pipeline.option('pipeline_option'):
            raise Exception("Pipeline option was not cascaded down to "
                            "pipeline2")
        mrmath = pipeline.create_node(MRCat(), name="mrcat")
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
            inputs=[DatasetSpec('derived2', text_format)],
            outputs=[DatasetSpec('derived3', text_format)],
            desc="A dummy pipeline used to test 'run_pipeline' method",
            version=1,
            citations=[],
            **kwargs)
        mrconvert = pipeline.create_node(MRConvert(), name="convert")
        # Connect inputs
        pipeline.connect_input('derived2', mrconvert, 'in_file')
        # Connect outputs
        pipeline.connect_output('derived3', mrconvert, 'out_file')
        return pipeline

    def pipeline4(self, **kwargs):
        pipeline = self.create_pipeline(
            name='pipeline4',
            inputs=[DatasetSpec('derived1_2', text_format),
                    DatasetSpec('derived3', text_format)],
            outputs=[DatasetSpec('derived4', text_format)],
            desc="A dummy pipeline used to test 'run_pipeline' method",
            version=1,
            citations=[],
            **kwargs)
        mrmath = pipeline.create_node(MRCat(), name="mrcat")
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
            inputs=[DatasetSpec('ones_slice', text_format)],
            outputs=[DatasetSpec('subject_summary', text_format)],
            desc=("Test of project summary variables"),
            version=1,
            citations=[],
            **kwargs)
        mrmath = pipeline.create_join_visits_node(
            MRMath(), 'in_files', 'mrmath')
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
            inputs=[DatasetSpec('ones_slice', text_format)],
            outputs=[DatasetSpec('visit_summary', text_format)],
            desc=("Test of project summary variables"),
            version=1,
            citations=[],
            **kwargs)
        mrmath = pipeline.create_join_visits_node(
            MRMath(), 'in_files', 'mrmath')
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
            inputs=[DatasetSpec('ones_slice', text_format)],
            outputs=[DatasetSpec('project_summary', text_format)],
            desc=("Test of project summary variables"),
            version=1,
            citations=[],
            **kwargs)
        mrmath1 = pipeline.create_join_visits_node(
            MRMath(), 'in_files', 'mrmath1')
        mrmath2 = pipeline.create_join_subjects_node(
            MRMath(), 'in_files', 'mrmath2')
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


class TestStudy(BaseTestCase):

    SUBJECT_IDS = ['SUBJECTID1', 'SUBJECTID2', 'SUBJECTID3']
    SESSION_IDS = ['SESSIONID1', 'SESSIONID2']

    def setUp(self):
        self.reset_dirs()
        for subject_id in self.SUBJECT_IDS:
            for visit_id in self.SESSION_IDS:
                self.add_session(self.project_dir, subject_id, visit_id)
        self.study = self.create_study(
            ExampleStudy, 'dummy', inputs=[
                DatasetMatch('start', text_format, 'start'),
                DatasetMatch('ones_slice', text_format, 'ones_slice')],
            options={'pipeline_option': True})

    def tearDown(self):
        try:
            ArcanaNodeMixin.unload_module('mrtrix')
        except ArcanaModulesNotInstalledException:
            pass

    def test_pipeline_prerequisites(self):
        self.study.data('derived4')[0]
        for dataset in ExampleStudy.data_specs():
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
        DatasetSpec('start', text_format),
        DatasetSpec('tens', text_format, 'tens_pipeline'),
        DatasetSpec('hundreds', text_format, 'hundreds_pipeline'),
        DatasetSpec('thousands', text_format, 'thousands_pipeline')]

    def pipeline_factory(self, incr, input, output):  # @ReservedAssignment
        pipeline = self.create_pipeline(
            name=output,
            inputs=[DatasetSpec(input, text_format)],
            outputs=[DatasetSpec(output, text_format)],
            desc=(
                "A dummy pipeline used to test 'partial-complete' method"),
            version=1,
            citations=[])
        # Nodes
        operands = pipeline.create_node(Merge(2), name='merge')
        mult = pipeline.create_node(MRCalc(), name="convert1")
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
                DatasetMatch('start', text_format, 'ones')])
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


test1_format = DataFormat('test1', extension='.t1')
test2_format = DataFormat('test2', extension='.t2',
                          converters={'test1': IdentityConverter})
test3_format = DataFormat('test3', extension='.t3')

DataFormat.register(test1_format)
DataFormat.register(test2_format)
DataFormat.register(test3_format)


class TestInputValidationStudy(Study):

    __metaclass__ = StudyMetaClass

    add_data_specs = [
        DatasetSpec('a', test2_format),
        DatasetSpec('b', test3_format),
        DatasetSpec('c', test2_format, 'identity_pipeline'),
        DatasetSpec('d', test3_format, 'identity_pipeline')]

    def identity_pipeline(self, **kwargs):
        pipeline = self.create_pipeline(
            name='pipeline',
            inputs=[DatasetSpec('a', test2_format),
                    DatasetSpec('b', test3_format)],
            outputs=[DatasetSpec('c', test2_format),
                     DatasetSpec('d', test3_format)],
            desc="A dummy pipeline used to test study input validation",
            version=1,
            citations=[],
            **kwargs)
        identity = pipeline.create_node(IdentityInterface(['a', 'b']),
                                        name='identity')
        pipeline.connect_input('a', identity, 'a')
        pipeline.connect_input('b', identity, 'b')
        pipeline.connect_output('c', identity, 'a')
        pipeline.connect_output('d', identity, 'b')


class TestInputValidation(BaseTestCase):

    def setUp(self):
        self.reset_dirs()
        os.makedirs(self.session_dir)
        for spec in TestInputValidationStudy.data_specs():
            with open(os.path.join(self.session_dir, spec.name) +
                      spec.format.ext, 'w') as f:
                f.write(spec.name)

    def test_input_validation(self):
        self.create_study(
            TestInputValidationStudy,
            'test_input_validation',
            inputs=[
                DatasetMatch('a', test1_format, 'a'),
                DatasetMatch('b', test3_format, 'b'),
                DatasetMatch('c', test1_format, 'a'),
                DatasetMatch('d', test3_format, 'd')])

    def test_input_validation_fail(self):
        self.assertRaises(
            ArcanaNoConverterError,
            self.create_study,
            TestInputValidationStudy,
            'test_validation_fail',
            inputs=[
                DatasetMatch('a', test3_format, 'a'),
                DatasetMatch('b', test3_format, 'b'),
                DatasetMatch('c', test3_format, 'a'),
                DatasetMatch('d', test3_format, 'd')])


class BasicTestClass(Study):

    __metaclass__ = StudyMetaClass

    add_data_specs = [DatasetSpec('dataset', text_format),
                      DatasetSpec('out_dataset', text_format,
                                  'pipeline')]

    def pipeline(self, **kwargs):
        pipeline = self.create_pipeline(
            'pipeline',
            inputs=[DatasetSpec('dataset', text_format)],
            outputs=[DatasetSpec('out_dataset', text_format)],
            desc='a dummy pipeline',
            citations=[],
            version=1,
            **kwargs)
        ident = pipeline.create_node(IdentityInterface(['dataset']),
                                     name='ident')
        pipeline.connect_input('dataset', ident, 'dataset')
        pipeline.connect_output('out_dataset', ident, 'dataset')
        return pipeline


class TestGeneratedPickle(BaseTestCase):

    def test_generated_cls_pickle(self):
        GeneratedClass = StudyMetaClass(
            'GeneratedClass', (BasicTestClass,), {})
        study = self.create_study(
            GeneratedClass,
            'gen_cls',
            inputs=[DatasetMatch('dataset', text_format, 'dataset')])
        pkl_path = os.path.join(self.work_dir, 'gen_cls.pkl')
        with open(pkl_path, 'w') as f:
            pkl.dump(study, f)
        del GeneratedClass
        with open(pkl_path) as f:
            regen = pkl.load(f)
        regen.data('out_dataset')[0]
        self.assertDatasetCreated('out_dataset.txt', 'gen_cls')

    def test_multi_study_generated_cls_pickle(self):
        cls_dct = {
            'add_sub_study_specs': [
                SubStudySpec('ss1', BasicTestClass),
                SubStudySpec('ss2', BasicTestClass)]}
        MultiGeneratedClass = MultiStudyMetaClass(
            'MultiGeneratedClass', (MultiStudy,), cls_dct)
        study = self.create_study(
            MultiGeneratedClass,
            'multi_gen_cls',
            inputs=[DatasetMatch('ss1_dataset', text_format, 'dataset'),
                    DatasetMatch('ss2_dataset', text_format, 'dataset')])
        pkl_path = os.path.join(self.work_dir, 'multi_gen_cls.pkl')
        with open(pkl_path, 'w') as f:
            pkl.dump(study, f)
        del MultiGeneratedClass
        with open(pkl_path) as f:
            regen = pkl.load(f)
        regen.data('ss2_out_dataset')[0]
        self.assertDatasetCreated('ss2_out_dataset.txt',
                                  'multi_gen_cls')

    def test_genenerated_method_pickle_fail(self):
        cls_dct = {
            'add_sub_study_specs': [
                SubStudySpec('ss1', BasicTestClass),
                SubStudySpec('ss2', BasicTestClass)],
            'default_dataset_pipeline': MultiStudy.translate(
                'ss1', 'pipeline')}
        MultiGeneratedClass = MultiStudyMetaClass(
            'MultiGeneratedClass', (MultiStudy,), cls_dct)
        study = self.create_study(
            MultiGeneratedClass,
            'multi_gen_cls',
            inputs=[DatasetMatch('ss1_dataset', text_format, 'dataset'),
                    DatasetMatch('ss2_dataset', text_format, 'dataset')])
        pkl_path = os.path.join(self.work_dir, 'multi_gen_cls.pkl')
        with open(pkl_path, 'w') as f:
            self.assertRaises(
                ArcanaCantPickleStudyError,
                pkl.dump,
                study,
                f)


#     def test_explicit_prereqs(self):
#         study = self.create_study(
#             ExistingPrereqStudy, self.study_name, inputs=[
#                 DatasetMatch('ones', text_format, 'ones')])
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
