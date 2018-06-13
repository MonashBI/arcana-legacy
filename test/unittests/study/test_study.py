from future import standard_library
standard_library.install_aliases()
from builtins import str  # @IgnorePep8
import os.path  # @IgnorePep8
# from nipype import config
# config.enable_debug_mode()
from arcana.dataset import DatasetMatch, DatasetSpec  # @IgnorePep8
from arcana.file_format import text_format  # @IgnorePep8
from arcana.study.base import Study, StudyMetaClass  # @IgnorePep8
from arcana.testing import (  # @IgnorePep8
    BaseTestCase, BaseMultiSubjectTestCase, TestMath)  # @IgnorePep8
from arcana.exception import (  # @IgnorePep8
    ArcanaNameError, ArcanaCantPickleStudyError)  # @IgnorePep8
from arcana.study.multi import (  # @IgnorePep8
    MultiStudy, MultiStudyMetaClass, SubStudySpec)
from nipype.interfaces.base import (  # @IgnorePep8
    BaseInterface, File, TraitedSpec, traits, isdefined)
from arcana.parameter import ParameterSpec  # @IgnorePep8
from arcana.file_format import FileFormat, IdentityConverter  # @IgnorePep8
from nipype.interfaces.utility import IdentityInterface  # @IgnorePep8
from arcana.exception import ArcanaNoConverterError  # @IgnorePep8
from arcana.repository import Project, Subject, Session, Visit  # @IgnorePep8
from arcana.dataset import Dataset  # @IgnorePep8
from future.utils import PY2  # @IgnorePep8
from future.utils import with_metaclass  # @IgnorePep8
if PY2:
    import pickle as pkl  # @UnusedImport
else:
    import pickle as pkl  # @Reimport


class ExampleStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        DatasetSpec('one', text_format),
        DatasetSpec('ten', text_format),
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

    add_parameter_specs = [
        ParameterSpec('pipeline_parameter', False)]

    def pipeline1(self, **kwargs):
        pipeline = self.create_pipeline(
            name='pipeline1',
            inputs=[DatasetSpec('one', text_format)],
            outputs=[DatasetSpec('derived1_1', text_format),
                     DatasetSpec('derived1_2', text_format)],
            desc="A dummy pipeline used to test 'run_pipeline' method",
            version=1,
            citations=[],
            **kwargs)
        if not self.parameter('pipeline_parameter'):
            raise Exception("Pipeline parameter was not accessible")
        indent = pipeline.create_node(IdentityInterface(['file']),
                                      name="ident1")
        indent2 = pipeline.create_node(IdentityInterface(['file']),
                                       name="ident2")
        # Connect inputs
        pipeline.connect_input('one', indent, 'file')
        pipeline.connect_input('one', indent2, 'file')
        # Connect outputs
        pipeline.connect_output('derived1_1', indent, 'file')
        pipeline.connect_output('derived1_2', indent2, 'file')
        return pipeline

    def pipeline2(self, **kwargs):
        pipeline = self.create_pipeline(
            name='pipeline2',
            inputs=[DatasetSpec('one', text_format),
                    DatasetSpec('derived1_1', text_format)],
            outputs=[DatasetSpec('derived2', text_format)],
            desc="A dummy pipeline used to test 'run_pipeline' method",
            version=1,
            citations=[],
            **kwargs)
        if not self.parameter('pipeline_parameter'):
            raise Exception("Pipeline parameter was not cascaded down to "
                            "pipeline2")
        math = pipeline.create_node(TestMath(), name="math")
        math.inputs.op = 'add'
        math.inputs.as_file = True
        # Connect inputs
        pipeline.connect_input('one', math, 'x')
        pipeline.connect_input('derived1_1', math, 'y')
        # Connect outputs
        pipeline.connect_output('derived2', math, 'z')
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
        indent = pipeline.create_node(IdentityInterface(['file']),
                                      name="ident")
        # Connect inputs
        pipeline.connect_input('derived2', indent, 'file')
        # Connect outputs
        pipeline.connect_output('derived3', indent, 'file')
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
        math = pipeline.create_node(TestMath(), name="mrcat")
        math.inputs.op = 'mul'
        math.inputs.as_file = True
        # Connect inputs
        pipeline.connect_input('derived1_2', math, 'x')
        pipeline.connect_input('derived3', math, 'y')
        # Connect outputs
        pipeline.connect_output('derived4', math, 'z')
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
            inputs=[DatasetSpec('one', text_format)],
            outputs=[DatasetSpec('subject_summary', text_format)],
            desc=("Test of project summary variables"),
            version=1,
            citations=[],
            **kwargs)
        math = pipeline.create_join_visits_node(
            TestMath(), joinfield='x', name='math')
        math.inputs.op = 'add'
        math.inputs.as_file = True
        # Connect inputs
        pipeline.connect_input('one', math, 'x')
        # Connect outputs
        pipeline.connect_output('subject_summary', math, 'z')
        pipeline.assert_connected()
        return pipeline

    def visit_summary_pipeline(self, **kwargs):
        pipeline = self.create_pipeline(
            name="visit_summary",
            inputs=[DatasetSpec('one', text_format)],
            outputs=[DatasetSpec('visit_summary', text_format)],
            desc=("Test of project summary variables"),
            version=1,
            citations=[],
            **kwargs)
        math = pipeline.create_join_subjects_node(
            TestMath(), joinfield='x', name='math')
        math.inputs.op = 'add'
        math.inputs.as_file = True
        # Connect inputs
        pipeline.connect_input('one', math, 'x')
        # Connect outputs
        pipeline.connect_output('visit_summary', math, 'z')
        pipeline.assert_connected()
        return pipeline

    def project_summary_pipeline(self, **kwargs):
        pipeline = self.create_pipeline(
            name="project_summary",
            inputs=[DatasetSpec('one', text_format)],
            outputs=[DatasetSpec('project_summary', text_format)],
            desc=("Test of project summary variables"),
            version=1,
            citations=[],
            **kwargs)
        math1 = pipeline.create_join_visits_node(
            TestMath(), joinfield='x', name='math1')
        math2 = pipeline.create_join_subjects_node(
            TestMath(), joinfield='x', name='math2')
        math1.inputs.op = 'add'
        math2.inputs.op = 'add'
        math1.inputs.as_file = True
        math2.inputs.as_file = True
        # Connect inputs
        pipeline.connect_input('one', math1, 'x')
        pipeline.connect(math1, 'z', math2, 'x')
        # Connect outputs
        pipeline.connect_output('project_summary', math2, 'z')
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


class TestStudy(BaseMultiSubjectTestCase):

    SUBJECT_IDS = ['SUBJECTID1', 'SUBJECTID2', 'SUBJECTID3']
    VISIT_IDS = ['VISITID1', 'VISITID2']
    DATASET_CONTENTS = {'one': '1', 'ten': '10'}

    @property
    def input_tree(self):
        sessions = []
        for subj_id in self.SUBJECT_IDS:
            for visit_id in self.VISIT_IDS:
                sessions.append(
                    Session(subj_id, visit_id, datasets=[
                        Dataset('one', text_format,
                                subject_id=subj_id, visit_id=visit_id),
                        Dataset('ten', text_format, subject_id=subj_id,
                                visit_id=visit_id)]))
        subjects = [Subject(i, sessions=[s for s in sessions
                                          if s.subject_id == i])
                     for i in self.SUBJECT_IDS]
        visits = [Visit(i, sessions=[s for s in sessions
                                     if s.visit == i])
                     for i in self.VISIT_IDS]
        return Project(subjects=subjects, visits=visits)

    def make_study(self):
        return self.create_study(
            ExampleStudy, 'dummy', inputs=[
                DatasetMatch('one', text_format, 'one'),
                DatasetMatch('ten', text_format, 'ten')],
            parameters={'pipeline_parameter': True})

    def test_run_pipeline_with_prereqs(self):
        study = self.make_study()
        study.data('derived4')[0]
        for dataset in ExampleStudy.data_specs():
            if dataset.frequency == 'per_session' and dataset.derived:
                for subject_id in self.SUBJECT_IDS:
                    for visit_id in self.VISIT_IDS:
                        self.assertDatasetCreated(
                            dataset.name + dataset.format.extension,
                            study.name, subject=subject_id,
                            visit=visit_id)

    def test_subject_summary(self):
        study = self.make_study()
        summaries = study.data('subject_summary')
        ref = str(float(len(self.VISIT_IDS)))
        for dataset in summaries:
            self.assertContentsEqual(dataset, ref,
                                     str(dataset.visit_id))

    def test_visit_summary(self):
        study = self.make_study()
        summaries = study.data('visit_summary')
        ref = str(float(len(self.SUBJECT_IDS)))
        for dataset in summaries:
            self.assertContentsEqual(dataset, ref,
                                     str(dataset.visit_id))

    def test_project_summary(self):
        study = self.make_study()
        study.data('project_summary')
        summary = study.data('project_summary')[0]
        ref = str(float(len(self.SUBJECT_IDS) * len(self.VISIT_IDS)))
        self.assertContentsEqual(summary, ref)

    def test_subject_ids_access(self):
        study = self.make_study()
        study.data('subject_ids')
        for visit_id in self.VISIT_IDS:
            subject_ids_path = self.output_file_path(
                'subject_ids.txt', study.name,
                visit=visit_id, frequency='per_visit')
            with open(subject_ids_path) as f:
                ids = f.read().split('\n')
            self.assertEqual(sorted(ids), sorted(self.SUBJECT_IDS))

    def test_visit_ids_access(self):
        study = self.make_study()
        study.data('visit_ids')
        for subject_id in self.SUBJECT_IDS:
            visit_ids_path = self.output_file_path(
                'visit_ids.txt', study.name,
                subject=subject_id, frequency='per_subject')
            with open(visit_ids_path) as f:
                ids = f.read().split('\n')
            self.assertEqual(sorted(ids), sorted(self.VISIT_IDS))


class ExistingPrereqStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        DatasetSpec('one', text_format),
        DatasetSpec('ten', text_format, 'tens_pipeline'),
        DatasetSpec('hundred', text_format, 'hundreds_pipeline'),
        DatasetSpec('thousand', text_format, 'thousands_pipeline')]

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
        math = pipeline.create_node(TestMath(), name="math")
        math.inputs.y = incr
        math.inputs.op = 'add'
        math.inputs.as_file = True
        # Connect inputs
        pipeline.connect_input(input, math, 'x')
        # Connect outputs
        pipeline.connect_output(output, math, 'z')
        return pipeline

    def tens_pipeline(self, **kwargs):  # @UnusedVariable
        return self.pipeline_factory(10, 'one', 'ten')

    def hundreds_pipeline(self, **kwargs):  # @UnusedVariable
        return self.pipeline_factory(100, 'ten', 'hundred')

    def thousands_pipeline(self, **kwargs):  # @UnusedVariable
        return self.pipeline_factory(1000, 'hundred', 'thousand')


class TestExistingPrereqs(BaseMultiSubjectTestCase):
    """
    This unittest tests out that partially previously calculated prereqs
    are detected and not rerun unless reprocess==True.
    """

    PROJECT_STRUCTURE = {
        'subject1': {
            'visit1': ['one', 'ten', 'hundred'],
            'visit2': ['one', 'ten'],
            'visit3': ['one', 'hundred', 'thousand']},
        'subject2': {
            'visit1': ['one'],
            'visit2': ['one', 'ten'],
            'visit3': ['one', 'ten', 'hundred', 'thousand']}}

    DATASET_CONTENTS = {'one': 1.0, 'study_ten': 10.0,
                        'study_hundred': 100.0,
                        'study_thousand': 1000.0}

    STUDY_NAME = 'study'

    @property
    def input_tree(self):
        sessions = []
        visit_ids = set()
        for subj_id, visits in list(self.PROJECT_STRUCTURE.items()):
            for visit_id, datasets in list(visits.items()):
                sessions.append(Session(subj_id, visit_id, datasets=[
                    Dataset(('{}_{}'.format(self.STUDY_NAME, d)
                             if d != 'one' else d),
                            text_format, subject_id=subj_id,
                            visit_id=visit_id) for d in datasets]))
                visit_ids.add(visit_id)
        subjects = [Subject(i, sessions=[s for s in sessions
                                         if s.subject_id == i])
                    for i in self.PROJECT_STRUCTURE]
        visits = [Visit(i, sessions=[s for s in sessions
                                     if s.visit == i])
                  for i in visit_ids]
        return Project(subjects=subjects, visits=visits)

    def test_per_session_prereqs(self):
        study = self.create_study(
            ExistingPrereqStudy, self.STUDY_NAME, inputs=[
                DatasetMatch('one', text_format, 'one')])
        study.data('thousand')
        targets = {
            'subject1': {
                'visit1': 1100.0,
                'visit2': 1110.0,
                'visit3': 1000.0},
            'subject2': {
                'visit1': 1111.0,
                'visit2': 1110.0,
                'visit3': 1000.0}}
        tree = self.repository.get_tree()
        for subj_id, visits in self.PROJECT_STRUCTURE.items():
            for visit_id in visits:
                session = tree.subject(subj_id).session(visit_id)
                try:
                    dataset = session.dataset('thousand')
                except ArcanaNameError:
                    if session.derived is None:
                        derived_session = session
                    else:
                        derived_session = session.derived
                    dataset = derived_session.dataset(
                        '{}_thousand'.format(self.STUDY_NAME))
                self.assertContentsEqual(
                    dataset, targets[subj_id][visit_id],
                    "{}:{}".format(subj_id, visit_id))


test1_format = FileFormat('test1', extension='.t1')
test2_format = FileFormat('test2', extension='.t2',
                          converters={'test1': IdentityConverter})
test3_format = FileFormat('test3', extension='.t3')

FileFormat.register(test1_format)
FileFormat.register(test2_format)
FileFormat.register(test3_format)


class TestInputValidationStudy(with_metaclass(StudyMetaClass, Study)):

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


class BasicTestClass(with_metaclass(StudyMetaClass, Study)):

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

    INPUT_DATASETS = {'dataset': 'foo'}

    def test_generated_cls_pickle(self):
        GeneratedClass = StudyMetaClass(
            'GeneratedClass', (BasicTestClass,), {})
        study = self.create_study(
            GeneratedClass,
            'gen_cls',
            inputs=[DatasetMatch('dataset', text_format, 'dataset')])
        pkl_path = os.path.join(self.work_dir, 'gen_cls.pkl')
        with open(pkl_path, 'wb') as f:
            pkl.dump(study, f)
        del GeneratedClass
        with open(pkl_path, 'rb') as f:
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
        with open(pkl_path, 'wb') as f:
            pkl.dump(study, f)
        del MultiGeneratedClass
        with open(pkl_path, 'rb') as f:
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
