from future import standard_library
standard_library.install_aliases()
from builtins import str  # @IgnorePep8
import os.path  # @IgnorePep8
# from nipype import config
# config.enable_debug_mode()
from arcana.data import FilesetMatch, FilesetSpec  # @IgnorePep8
from arcana.data.file_format.standard import text_format  # @IgnorePep8
from arcana.study.base import Study, StudyMetaClass  # @IgnorePep8
from arcana.testing import (  # @IgnorePep8
    BaseTestCase, BaseMultiSubjectTestCase, TestMath)  # @IgnorePep8
from arcana.exception import (  # @IgnorePep8
    ArcanaCantPickleStudyError)  # @IgnorePep8
from arcana.study.multi import (  # @IgnorePep8
    MultiStudy, MultiStudyMetaClass, SubStudySpec)
from nipype.interfaces.base import (  # @IgnorePep8
    BaseInterface, File, TraitedSpec, traits, isdefined)
from arcana.parameter import ParameterSpec  # @IgnorePep8
from arcana.data.file_format import FileFormat, IdentityConverter  # @IgnorePep8
from nipype.interfaces.utility import IdentityInterface  # @IgnorePep8
from arcana.exception import ArcanaNoConverterError  # @IgnorePep8
from arcana.repository import Tree, Subject, Session, Visit  # @IgnorePep8
from arcana.data import Fileset  # @IgnorePep8
from future.utils import PY2  # @IgnorePep8
from future.utils import with_metaclass  # @IgnorePep8
if PY2:
    import pickle as pkl  # @UnusedImport
else:
    import pickle as pkl  # @Reimport


class ExampleStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        FilesetSpec('one', text_format),
        FilesetSpec('ten', text_format),
        FilesetSpec('derived1_1', text_format, 'pipeline1'),
        FilesetSpec('derived1_2', text_format, 'pipeline1'),
        FilesetSpec('derived2', text_format, 'pipeline2'),
        FilesetSpec('derived3', text_format, 'pipeline3'),
        FilesetSpec('derived4', text_format, 'pipeline4'),
        FilesetSpec('subject_summary', text_format,
                    'subject_summary_pipeline',
                    frequency='per_subject'),
        FilesetSpec('visit_summary', text_format,
                    'visit_summary_pipeline',
                    frequency='per_visit'),
        FilesetSpec('project_summary', text_format,
                    'project_summary_pipeline',
                    frequency='per_study'),
        FilesetSpec('subject_ids', text_format,
                    'subject_ids_access_pipeline',
                    frequency='per_visit'),
        FilesetSpec('visit_ids', text_format,
                    'visit_ids_access_pipeline',
                    frequency='per_subject')]

    add_parameter_specs = [
        ParameterSpec('pipeline_parameter', False)]

    def pipeline1(self, **mods):
        pipeline = self.pipeline(
            name='pipeline1',
            desc="A dummy pipeline used to test 'run_pipeline' method",
            references=[],
            mods=mods)
        if not self.parameter('pipeline_parameter'):
            raise Exception("Pipeline parameter was not accessible")
        indent = pipeline.add("ident1", IdentityInterface(['file']))
        indent2 = pipeline.add("ident2", IdentityInterface(['file']))
        # Connect inputs
        pipeline.connect_input('one', indent, 'file')
        pipeline.connect_input('one', indent2, 'file')
        # Connect outputs
        pipeline.connect_output('derived1_1', indent, 'file')
        pipeline.connect_output('derived1_2', indent2, 'file')
        return pipeline

    def pipeline2(self, **mods):
        pipeline = self.pipeline(
            name='pipeline2',
            desc="A dummy pipeline used to test 'run_pipeline' method",
            references=[],
            mods=mods)
        if not self.parameter('pipeline_parameter'):
            raise Exception("Pipeline parameter was not cascaded down to "
                            "pipeline2")
        math = pipeline.add("math", TestMath())
        math.inputs.op = 'add'
        math.inputs.as_file = True
        # Connect inputs
        pipeline.connect_input('one', math, 'x')
        pipeline.connect_input('derived1_1', math, 'y')
        # Connect outputs
        pipeline.connect_output('derived2', math, 'z')
        return pipeline

    def pipeline3(self, **mods):
        pipeline = self.pipeline(
            name='pipeline3',
            desc="A dummy pipeline used to test 'run_pipeline' method",
            references=[],
            mods=mods)
        indent = pipeline.add('ident', IdentityInterface(['file']))
        # Connect inputs
        pipeline.connect_input('derived2', indent, 'file')
        # Connect outputs
        pipeline.connect_output('derived3', indent, 'file')
        return pipeline

    def pipeline4(self, **mods):
        pipeline = self.pipeline(
            name='pipeline4',
            desc="A dummy pipeline used to test 'run_pipeline' method",
            references=[],
            mods=mods)
        math = pipeline.add("mrcat", TestMath())
        math.inputs.op = 'mul'
        math.inputs.as_file = True
        # Connect inputs
        pipeline.connect_input('derived1_2', math, 'x')
        pipeline.connect_input('derived3', math, 'y')
        # Connect outputs
        pipeline.connect_output('derived4', math, 'z')
        return pipeline

    def visit_ids_access_pipeline(self, **mods):
        pipeline = self.pipeline(
            name='visit_ids_access',
            desc=(
                "A dummy pipeline used to test access to 'session' IDs"),
            references=[],
            mods=mods)
        sessions_to_file = pipeline.add(
            'sess_to_file', IteratorToFile(), joinsource='visits',
            joinfield='ids')
        pipeline.connect_input('visit_id', sessions_to_file, 'ids')
        pipeline.connect_output('visit_ids', sessions_to_file,
                                'out_file')
        return pipeline

    def subject_ids_access_pipeline(self, **mods):
        pipeline = self.pipeline(
            name='subject_ids_access',
            desc=(
                "A dummy pipeline used to test access to 'subject' IDs"),
            references=[],
            mods=mods)
        subjects_to_file = pipeline.add(
            'subjects_to_file', IteratorToFile(), joinfield='ids',
            joinsource='subjects')
        pipeline.connect_input('subject_id', subjects_to_file, 'ids')
        pipeline.connect_output('subject_ids', subjects_to_file,
                                'out_file')
        return pipeline

    def subject_summary_pipeline(self, **mods):
        pipeline = self.pipeline(
            name="subject_summary",
            desc=("Test of project summary variables"),
            references=[],
            mods=mods)
        math = pipeline.add(
            'math', TestMath(), joinfield='x', joinsource='visits')
        math.inputs.op = 'add'
        math.inputs.as_file = True
        # Connect inputs
        pipeline.connect_input('one', math, 'x')
        # Connect outputs
        pipeline.connect_output('subject_summary', math, 'z')
        return pipeline

    def visit_summary_pipeline(self, **mods):
        pipeline = self.pipeline(
            name="visit_summary",
            desc=("Test of project summary variables"),
            references=[],
            mods=mods)
        math = pipeline.add('math', TestMath(), joinfield='x',
                            joinsource='subjects')
        math.inputs.op = 'add'
        math.inputs.as_file = True
        # Connect inputs
        pipeline.connect_input('one', math, 'x')
        # Connect outputs
        pipeline.connect_output('visit_summary', math, 'z')
        return pipeline

    def project_summary_pipeline(self, **mods):
        pipeline = self.pipeline(
            name="project_summary",
            desc=("Test of project summary variables"),
            references=[],
            mods=mods)
        math1 = pipeline.add(
            'math1', TestMath(), joinfield='x', joinsource='visits')
        math2 = pipeline.add(
            'math2', TestMath(), joinfield='x', joinsource='subjects')
        math1.inputs.op = 'add'
        math2.inputs.op = 'add'
        math1.inputs.as_file = True
        math2.inputs.as_file = True
        # Connect inputs
        pipeline.connect_input('one', math1, 'x')
        pipeline.connect(math1, 'z', math2, 'x')
        # Connect outputs
        pipeline.connect_output('project_summary', math2, 'z')
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
    DATASET_CONTENTS = {'one_input': '1', 'ten_input': '10'}

    @property
    def input_tree(self):
        sessions = []
        for subj_id in self.SUBJECT_IDS:
            for visit_id in self.VISIT_IDS:
                sessions.append(
                    Session(subj_id, visit_id, filesets=[
                        Fileset('one_input', text_format,
                                subject_id=subj_id, visit_id=visit_id),
                        Fileset('ten_input', text_format,
                                subject_id=subj_id,
                                visit_id=visit_id)]))
        subjects = [Subject(i, sessions=[s for s in sessions
                                          if s.subject_id == i])
                     for i in self.SUBJECT_IDS]
        visits = [Visit(i, sessions=[s for s in sessions
                                     if s.visit == i])
                     for i in self.VISIT_IDS]
        return Tree(subjects=subjects, visits=visits)

    def make_study(self):
        return self.create_study(
            ExampleStudy, 'dummy', inputs=[
                FilesetMatch('one', text_format, 'one_input'),
                FilesetMatch('ten', text_format, 'ten_input')],
            parameters={'pipeline_parameter': True})

    def test_run_pipeline_with_prereqs(self):
        study = self.make_study()
        self.assertContentsEqual(study.data('derived4'),
                                 [2.0, 2.0, 2.0, 2.0, 2.0, 2.0])

    def test_subject_summary(self):
        study = self.make_study()
        summaries = study.data('subject_summary')
        ref = float(len(self.VISIT_IDS))
        for fileset in summaries:
            self.assertContentsEqual(fileset, ref,
                                     str(fileset.visit_id))

    def test_visit_summary(self):
        study = self.make_study()
        summaries = study.data('visit_summary')
        ref = float(len(self.SUBJECT_IDS))
        for fileset in summaries:
            self.assertContentsEqual(fileset, ref,
                                     str(fileset.visit_id))

    def test_project_summary(self):
        study = self.make_study()
        ref = float(len(self.SUBJECT_IDS) * len(self.VISIT_IDS))
        self.assertContentsEqual(study.data('project_summary'), ref)

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
        FilesetSpec('one', text_format),
        FilesetSpec('ten', text_format, 'tens_pipeline'),
        FilesetSpec('hundred', text_format, 'hundreds_pipeline'),
        FilesetSpec('thousand', text_format, 'thousands_pipeline')]

    def pipeline_factory(self, incr, input, output, mods):  # @ReservedAssignment
        pipeline = self.pipeline(
            name=output,
            desc="A dummy pipeline used to test 'partial-complete' method",
            references=[], mods=mods)
        # Nodes
        math = pipeline.add("math", TestMath())
        math.inputs.y = incr
        math.inputs.op = 'add'
        math.inputs.as_file = True
        # Connect inputs
        pipeline.connect_input(input, math, 'x')
        # Connect outputs
        pipeline.connect_output(output, math, 'z')
        return pipeline

    def tens_pipeline(self, **mods):  # @UnusedVariable
        return self.pipeline_factory(10, 'one', 'ten', mods=mods)

    def hundreds_pipeline(self, **mods):  # @UnusedVariable
        return self.pipeline_factory(100, 'ten', 'hundred', mods=mods)

    def thousands_pipeline(self, **mods):  # @UnusedVariable
        return self.pipeline_factory(1000, 'hundred', 'thousand', mods=mods)


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

    DATASET_CONTENTS = {'one': 1.0, 'ten': 10.0, 'hundred': 100.0,
                        'thousand': 1000.0}

    STUDY_NAME = 'study'

    @property
    def input_tree(self):
        sessions = []
        visit_ids = set()
        for subj_id, visits in list(self.PROJECT_STRUCTURE.items()):
            for visit_id, filesets in list(visits.items()):
                sessions.append(Session(subj_id, visit_id, filesets=[
                    Fileset(d, text_format, subject_id=subj_id,
                            visit_id=visit_id, from_study=(
                                (self.STUDY_NAME
                                 if d != 'one' else None)))
                    for d in filesets]))
                visit_ids.add(visit_id)
        subjects = [Subject(i, sessions=[s for s in sessions
                                         if s.subject_id == i])
                    for i in self.PROJECT_STRUCTURE]
        visits = [Visit(i, sessions=[s for s in sessions
                                     if s.visit == i])
                  for i in visit_ids]
        return Tree(subjects=subjects, visits=visits)

    def test_per_session_prereqs(self):
        study = self.create_study(
            ExistingPrereqStudy, self.STUDY_NAME, inputs=[
                FilesetMatch('one', text_format, 'one')])
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
        tree = self.repository.tree()
        for subj_id, visits in self.PROJECT_STRUCTURE.items():
            for visit_id in visits:
                session = tree.subject(subj_id).session(visit_id)
                fileset = session.fileset('thousand',
                                          study=self.STUDY_NAME)
                self.assertContentsEqual(
                    fileset, targets[subj_id][visit_id],
                    "{}:{}".format(subj_id, visit_id))
                if subj_id == 'subject1' and visit_id == 'visit3':
                    self.assertNotIn(
                        'ten', [d.name for d in session.filesets],
                        "'ten' should not be generated for "
                        "subject1:visit3 as hundred and thousand are "
                        "already present")


test1_format = FileFormat('test1', extension='.t1')
test2_format = FileFormat('test2', extension='.t2',
                          converters={'test1': IdentityConverter})
test3_format = FileFormat('test3', extension='.t3')

FileFormat.register(test1_format)
FileFormat.register(test2_format)
FileFormat.register(test3_format)


class TestInputValidationStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        FilesetSpec('a', test2_format),
        FilesetSpec('b', test3_format),
        FilesetSpec('c', test2_format, 'identity_pipeline'),
        FilesetSpec('d', test3_format, 'identity_pipeline')]

    def identity_pipeline(self, **mods):
        pipeline = self.pipeline(
            name='pipeline',
            desc="A dummy pipeline used to test study input validation",
            references=[],
            mods=mods)
        identity = pipeline.add('identity', IdentityInterface(['a', 'b']))
        pipeline.connect_input('a', identity, 'a')
        pipeline.connect_input('b', identity, 'b')
        pipeline.connect_output('c', identity, 'a')
        pipeline.connect_output('d', identity, 'b')


class TestInputValidation(BaseTestCase):

    def setUp(self):
        self.reset_dirs()
        os.makedirs(self.session_dir)
        for spec in TestInputValidationStudy.data_specs():
            if spec.format == test2_format:
                ext = test1_format.ext
            else:
                ext = spec.format.ext
            with open(os.path.join(self.session_dir, spec.name) +
                      ext, 'w') as f:
                f.write(spec.name)

    def test_input_validation(self):
        self.create_study(
            TestInputValidationStudy,
            'test_input_validation',
            inputs=[
                FilesetMatch('a', test1_format, 'a'),
                FilesetMatch('b', test3_format, 'b'),
                FilesetMatch('c', test1_format, 'a'),
                FilesetMatch('d', test3_format, 'd')])


class TestInputValidationFail(BaseTestCase):

    def setUp(self):
        self.reset_dirs()
        os.makedirs(self.session_dir)
        for spec in TestInputValidationStudy.data_specs():
            with open(os.path.join(self.session_dir, spec.name) +
                      test3_format.ext, 'w') as f:
                f.write(spec.name)

    def test_input_validation_fail(self):
        self.assertRaises(
            ArcanaNoConverterError,
            self.create_study,
            TestInputValidationStudy,
            'test_validation_fail',
            inputs=[
                FilesetMatch('a', test3_format, 'a'),
                FilesetMatch('b', test3_format, 'b'),
                FilesetMatch('c', test3_format, 'a'),
                FilesetMatch('d', test3_format, 'd')])


class BasicTestClass(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [FilesetSpec('fileset', text_format),
                      FilesetSpec('out_fileset', text_format, 'a_pipeline')]

    def a_pipeline(self, **mods):
        pipeline = self.pipeline(
            'a_pipeline',
            desc='a dummy pipeline',
            references=[],
            mods=mods)
        ident = pipeline.add('ident', IdentityInterface(['fileset']))
        pipeline.connect_input('fileset', ident, 'fileset')
        pipeline.connect_output('out_fileset', ident, 'fileset')
        return pipeline


class TestGeneratedPickle(BaseTestCase):

    INPUT_DATASETS = {'fileset': 'foo'}

    def test_generated_cls_pickle(self):
        GeneratedClass = StudyMetaClass(
            'GeneratedClass', (BasicTestClass,), {})
        study = self.create_study(
            GeneratedClass,
            'gen_cls',
            inputs=[FilesetMatch('fileset', text_format, 'fileset')])
        pkl_path = os.path.join(self.work_dir, 'gen_cls.pkl')
        with open(pkl_path, 'wb') as f:
            pkl.dump(study, f)
        del GeneratedClass
        with open(pkl_path, 'rb') as f:
            regen = pkl.load(f)
        self.assertContentsEqual(regen.data('out_fileset'), 'foo')

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
            inputs=[FilesetMatch('ss1_fileset', text_format, 'fileset'),
                    FilesetMatch('ss2_fileset', text_format, 'fileset')])
        pkl_path = os.path.join(self.work_dir, 'multi_gen_cls.pkl')
        with open(pkl_path, 'wb') as f:
            pkl.dump(study, f)
        del MultiGeneratedClass
        with open(pkl_path, 'rb') as f:
            regen = pkl.load(f)
        self.assertContentsEqual(regen.data('ss2_out_fileset'), 'foo')

    def test_genenerated_method_pickle_fail(self):
        cls_dct = {
            'add_sub_study_specs': [
                SubStudySpec('ss1', BasicTestClass),
                SubStudySpec('ss2', BasicTestClass)],
            'default_fileset_pipeline': MultiStudy.translate(
                'ss1', 'pipeline')}
        MultiGeneratedClass = MultiStudyMetaClass(
            'MultiGeneratedClass', (MultiStudy,), cls_dct)
        study = self.create_study(
            MultiGeneratedClass,
            'multi_gen_cls',
            inputs=[FilesetMatch('ss1_fileset', text_format, 'fileset'),
                    FilesetMatch('ss2_fileset', text_format, 'fileset')])
        pkl_path = os.path.join(self.work_dir, 'multi_gen_cls.pkl')
        with open(pkl_path, 'w') as f:
            self.assertRaises(
                ArcanaCantPickleStudyError,
                pkl.dump,
                study,
                f)


#     def test_explicit_prereqs(self):
#         study = self.create_study(
#             ExistingPrereqStudy, self.from_study, inputs=[
#                 FilesetMatch('ones', text_format, 'ones')])
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
#                                      self.from_study,
#                                      subject=subj_id, session=visit_id,
#                                      frequency='per_session')
