from future import standard_library
standard_library.install_aliases()
from builtins import str  # @IgnorePep8
import os.path  # @IgnorePep8
# from nipype import config
# config.enable_debug_mode()
from arcana.data.file_format import text_format  # @IgnorePep8
from arcana.study.base import Study, StudyMetaClass  # @IgnorePep8
from arcana.utils.testing import (  # @IgnorePep8
    BaseTestCase, BaseMultiSubjectTestCase, TestMath)  # @IgnorePep8
from arcana.exceptions import (  # @IgnorePep8
    ArcanaCantPickleStudyError, ArcanaUsageError)  # @IgnorePep8
from arcana.study.multi import (  # @IgnorePep8
    MultiStudy, MultiStudyMetaClass, SubStudySpec)
from nipype.interfaces.base import (  # @IgnorePep8
    BaseInterface, File, TraitedSpec, traits, isdefined)
from arcana.study.parameter import ParamSpec  # @IgnorePep8
from arcana.data.file_format import FileFormat, IdentityConverter  # @IgnorePep8
from nipype.interfaces.utility import IdentityInterface  # @IgnorePep8
from arcana.exceptions import ArcanaNoConverterError  # @IgnorePep8
from arcana.repository import Tree  # @IgnorePep8
from arcana.data import (  # @IgnorePep8
    Fileset, FieldSpec, InputFilesetSpec, InputFilesets, FilesetSpec)
from future.utils import PY2  # @IgnorePep8
from future.utils import with_metaclass  # @IgnorePep8
import logging  # @IgnorePep8
if PY2:
    import pickle as pkl  # @UnusedImport
else:
    import pickle as pkl  # @Reimport


class ExampleStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        InputFilesetSpec('one', text_format),
        InputFilesetSpec('ten', text_format),
        FilesetSpec('derived1_1', text_format, 'pipeline1'),
        FilesetSpec('derived1_2', text_format, 'pipeline1'),
        FilesetSpec('derived2', text_format, 'pipeline2'),
        FilesetSpec('derived3', text_format, 'pipeline3'),
        FilesetSpec('derived4', text_format, 'pipeline4'),
        FieldSpec('derived5a', str, 'pipeline5',
                  pipeline_args={'arg': 'a'}),
        FieldSpec('derived5b', str, 'pipeline5',
                  pipeline_args={'arg': 'b'}),
        FilesetSpec('subject_summary', text_format,
                    'subject_summary_pipeline',
                    frequency='per_subject'),
        FilesetSpec('visit_summary', text_format,
                    'visit_summary_pipeline',
                    frequency='per_visit'),
        FilesetSpec('study_summary', text_format,
                    'study_summary_pipeline',
                    frequency='per_study'),
        FilesetSpec('subject_ids', text_format,
                    'subject_ids_access_pipeline',
                    frequency='per_visit'),
        FilesetSpec('visit_ids', text_format,
                    'visit_ids_access_pipeline',
                    frequency='per_subject')]

    add_param_specs = [
        ParamSpec('pipeline_parameter', False)]

    def pipeline1(self, **name_maps):
        pipeline = self.new_pipeline(
            name='pipeline1',
            desc="A dummy pipeline used to test 'run_pipeline' method",
            citations=[],
            name_maps=name_maps)
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

    def pipeline2(self, **name_maps):
        pipeline = self.new_pipeline(
            name='pipeline2',
            desc="A dummy pipeline used to test 'run_pipeline' method",
            citations=[],
            name_maps=name_maps)
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

    def pipeline3(self, **name_maps):
        pipeline = self.new_pipeline(
            name='pipeline3',
            desc="A dummy pipeline used to test 'run_pipeline' method",
            citations=[],
            name_maps=name_maps)
        indent = pipeline.add('ident', IdentityInterface(['file']))
        # Connect inputs
        pipeline.connect_input('derived2', indent, 'file')
        # Connect outputs
        pipeline.connect_output('derived3', indent, 'file')
        return pipeline

    def pipeline4(self, **name_maps):
        pipeline = self.new_pipeline(
            name='pipeline4',
            desc="A dummy pipeline used to test 'run_pipeline' method",
            citations=[],
            name_maps=name_maps)
        math = pipeline.add("mrcat", TestMath())
        math.inputs.op = 'mul'
        math.inputs.as_file = True
        # Connect inputs
        pipeline.connect_input('derived1_2', math, 'x')
        pipeline.connect_input('derived3', math, 'y')
        # Connect outputs
        pipeline.connect_output('derived4', math, 'z')
        return pipeline

    def pipeline5(self, arg, **name_maps):

        pipeline = self.new_pipeline(
            name='pipeline5{}'.format(arg),
            desc="A dummy pipeline used to test constructor arguments",
            citations=[],
            name_maps=name_maps)

        pipeline.add(
            "ident",
            IdentityInterface(
                fields=['value', 'dummy'],
                value=arg),
            inputs={
                'dummy': ('one', text_format)},
            outputs={
                'derived5{}'.format(arg): ('value', str)})

        return pipeline

    def visit_ids_access_pipeline(self, **name_maps):
        pipeline = self.new_pipeline(
            name='visit_ids_access',
            desc=(
                "A dummy pipeline used to test access to 'session' IDs"),
            citations=[],
            name_maps=name_maps)
        visits_to_file = pipeline.add(
            'visits_to_file', IteratorToFile(), joinsource=self.VISIT_ID,
            joinfield='ids')
        pipeline.connect_input(self.VISIT_ID, visits_to_file, 'ids')
        pipeline.connect_input(self.SUBJECT_ID, visits_to_file, 'fixed_id')
        pipeline.connect_output('visit_ids', visits_to_file, 'out_file')
        return pipeline

    def subject_ids_access_pipeline(self, **name_maps):
        pipeline = self.new_pipeline(
            name='subject_ids_access',
            desc=(
                "A dummy pipeline used to test access to 'subject' IDs"),
            citations=[],
            name_maps=name_maps)
        subjects_to_file = pipeline.add(
            'subjects_to_file', IteratorToFile(), joinfield='ids',
            joinsource=self.SUBJECT_ID)
        pipeline.connect_input(self.SUBJECT_ID, subjects_to_file, 'ids')
        pipeline.connect_input(self.VISIT_ID, subjects_to_file, 'fixed_id')
        pipeline.connect_output('subject_ids', subjects_to_file, 'out_file')
        return pipeline

    def subject_summary_pipeline(self, **name_maps):
        pipeline = self.new_pipeline(
            name="subject_summary",
            desc=("Test of project summary variables"),
            citations=[],
            name_maps=name_maps)
        math = pipeline.add(
            'math', TestMath(), joinfield='x', joinsource=self.VISIT_ID)
        math.inputs.op = 'add'
        math.inputs.as_file = True
        # Connect inputs
        pipeline.connect_input('one', math, 'x')
        # Connect outputs
        pipeline.connect_output('subject_summary', math, 'z')
        return pipeline

    def visit_summary_pipeline(self, **name_maps):
        pipeline = self.new_pipeline(
            name="visit_summary",
            desc=("Test of project summary variables"),
            citations=[],
            name_maps=name_maps)
        math = pipeline.add('math', TestMath(), joinfield='x',
                            joinsource=self.SUBJECT_ID)
        math.inputs.op = 'add'
        math.inputs.as_file = True
        # Connect inputs
        pipeline.connect_input('one', math, 'x')
        # Connect outputs
        pipeline.connect_output('visit_summary', math, 'z')
        return pipeline

    def study_summary_pipeline(self, **name_maps):
        pipeline = self.new_pipeline(
            name="study_summary",
            desc=("Test of project summary variables"),
            citations=[],
            name_maps=name_maps)
        math1 = pipeline.add(
            'math1', TestMath(), joinfield='x', joinsource=self.VISIT_ID)
        math2 = pipeline.add(
            'math2', TestMath(), joinfield='x', joinsource=self.SUBJECT_ID)
        math1.inputs.op = 'add'
        math2.inputs.op = 'add'
        math1.inputs.as_file = True
        math2.inputs.as_file = True
        # Connect inputs
        pipeline.connect_input('one', math1, 'x')
        pipeline.connect(math1, 'z', math2, 'x')
        # Connect outputs
        pipeline.connect_output('study_summary', math2, 'z')
        return pipeline


class IteratorToFileInputSpec(TraitedSpec):
    ids = traits.List(traits.Str(), desc="ID of the iterable")
    fixed_id = traits.Str(desc="The other ID that will remain fixed")
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
        filesets = []
        for subj_id in self.SUBJECT_IDS:
            for visit_id in self.VISIT_IDS:
                filesets.append(
                    Fileset('one_input', text_format,
                            subject_id=subj_id, visit_id=visit_id))
                filesets.append(
                    Fileset('ten_input', text_format,
                            subject_id=subj_id,
                            visit_id=visit_id))
        return Tree.construct(self.repository, filesets=filesets)

    def make_study(self):
        return self.create_study(
            ExampleStudy, 'dummy', inputs=[
                InputFilesets('one', 'one_input', text_format),
                InputFilesets('ten', 'ten_input', text_format)],
            parameters={'pipeline_parameter': True})

    def test_run_pipeline_with_prereqs(self):
        study = self.make_study()
        self.assertContentsEqual(study.data('derived4'),
                                 [2.0, 2.0, 2.0, 2.0, 2.0, 2.0])

    def test_pipeline_args(self):
        study = self.make_study()
        a = next(iter(study.data('derived5a'))).value
        b = next(iter(study.data('derived5b'))).value
        self.assertEqual(a, 'a')
        self.assertEqual(b, 'b')

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

    def test_study_summary(self):
        study = self.make_study()
        ref = float(len(self.SUBJECT_IDS) * len(self.VISIT_IDS))
        self.assertContentsEqual(study.data('study_summary'), ref)

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
        InputFilesetSpec('one', text_format),
        FilesetSpec('ten', text_format, 'ten_pipeline'),
        FilesetSpec('hundred', text_format, 'hundred_pipeline'),
        FilesetSpec('thousand', text_format, 'thousand_pipeline')]

    def pipeline_factory(self, incr, input, output, name_maps):  # @ReservedAssignment @IgnorePep8
        pipeline = self.new_pipeline(
            name=output + '_pipeline',
            desc="A dummy pipeline used to test 'partial-complete' method",
            citations=[], name_maps=name_maps)
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

    def ten_pipeline(self, **name_maps):  # @UnusedVariable
        return self.pipeline_factory(10, 'one', 'ten', name_maps=name_maps)

    def hundred_pipeline(self, **name_maps):  # @UnusedVariable
        return self.pipeline_factory(100, 'ten', 'hundred',
                                     name_maps=name_maps)

    def thousand_pipeline(self, **name_maps):  # @UnusedVariable
        return self.pipeline_factory(1000, 'hundred', 'thousand',
                                     name_maps=name_maps)


class TestExistingPrereqs(BaseMultiSubjectTestCase):
    """
    This unittest tests out that partially previously calculated prereqs
    are detected and not rerun unless reprocess==True.
    """

    PROJECT_STRUCTURE = {
        'subject1': {
            'visit1': ['one', 'ten', 'hundred'],
            'visit2': ['one', 'ten'],
            'visit3': ['one', 'ten', 'hundred', 'thousand']},
        'subject2': {
            'visit1': ['one'],
            'visit2': ['one', 'ten'],
            'visit3': ['one', 'ten', 'hundred', 'thousand']}}

    DATASET_CONTENTS = {'one': 1.0, 'ten': 10.0, 'hundred': 100.0,
                        'thousand': 1000.0}

    STUDY_NAME = 'study'

    @property
    def input_tree(self):
        filesets = []
        for subj_id, visit_ids in list(self.PROJECT_STRUCTURE.items()):
            for visit_id, fileset_names in list(visit_ids.items()):
                # Create filesets
                for name in fileset_names:
                    from_study = self.STUDY_NAME if name != 'one' else None
                    filesets.append(
                        Fileset(name, text_format, subject_id=subj_id,
                                visit_id=visit_id, from_study=from_study))
        return Tree.construct(self.repository, filesets=filesets)

    def add_sessions(self):
        BaseMultiSubjectTestCase.add_sessions(self)
        # Create a study object, in order to generate appropriate provenance
        # for the existing "derived" data
        derived_filesets = [f for f in self.DATASET_CONTENTS
                            if f != 'one']
        study = self.create_study(
            ExistingPrereqStudy, self.STUDY_NAME,
            repository=self.local_repository,
            inputs=[InputFilesets('one', 'one', text_format)])
        # Get all pipelines in the study
        pipelines = {n: getattr(study, '{}_pipeline'.format(n))()
                     for n in derived_filesets}
        for node in study.tree:
            for fileset in node.filesets:
                if fileset.basename != 'one' and fileset.exists:
                    # Generate expected provenance record for each pipeline
                    # and save in the local repository
                    pipelines[fileset.name].cap()
                    record = pipelines[fileset.name].expected_record(node)
                    self.local_repository.put_record(record)
        study.clear_caches()  # Reset repository trees

    def test_per_session_prereqs(self):
        # Generate all data for 'thousand' spec
        study = self.create_study(
            ExistingPrereqStudy, self.STUDY_NAME,
            inputs=[InputFilesets('one', 'one', text_format)])
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
                                          from_study=self.STUDY_NAME)
                fileset.format = text_format
                self.assertContentsEqual(
                    fileset, targets[subj_id][visit_id],
                    "{}:{}".format(subj_id, visit_id))


test1_format = FileFormat('test1', extension='.t1')
test2_format = FileFormat('test2', extension='.t2')
test3_format = FileFormat('test3', extension='.t3')

test2_format.set_converter(test1_format, IdentityConverter)


class TestInputValidationStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        InputFilesetSpec('a', (test1_format, test2_format)),
        InputFilesetSpec('b', test3_format),
        FilesetSpec('c', test2_format, 'identity_pipeline'),
        FilesetSpec('d', test3_format, 'identity_pipeline')]

    def identity_pipeline(self, **name_maps):
        pipeline = self.new_pipeline(
            name='pipeline',
            desc="A dummy pipeline used to test study input validation",
            citations=[],
            name_maps=name_maps)
        identity = pipeline.add('identity', IdentityInterface(['a', 'b']))
        pipeline.connect_input('a', identity, 'a')
        pipeline.connect_input('b', identity, 'b')
        pipeline.connect_output('c', identity, 'a')
        pipeline.connect_output('d', identity, 'b')


class TestInputValidation(BaseTestCase):

    def setUp(self):
        self.reset_dirs()
        os.makedirs(self.session_dir)
        for spec_name, fformat in (('a', test1_format), ('b', test3_format),
                                   ('c', test1_format), ('d', test3_format)):
            with open(os.path.join(self.session_dir, spec_name) +
                      fformat.ext, 'w') as f:
                f.write(spec_name)

    def test_input_validation(self):
        self.create_study(
            TestInputValidationStudy,
            'test_input_validation',
            inputs=[
                InputFilesets('a', 'a', test1_format),
                InputFilesets('b', 'b', test3_format),
                InputFilesets('c', 'a', test1_format),
                InputFilesets('d', 'd', test3_format)])


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
            ArcanaUsageError,
            self.create_study,
            TestInputValidationStudy,
            'test_validation_fail',
            inputs=[
                InputFilesets('a', 'a', test3_format),
                InputFilesets('b', 'b', test3_format)])


class TestInputNoConverter(BaseTestCase):

    def setUp(self):
        self.reset_dirs()
        os.makedirs(self.session_dir)
        for spec in TestInputValidationStudy.data_specs():
            if spec.name == 'a':
                ext = test1_format.ext
            else:
                ext = test3_format.ext
            with open(os.path.join(self.session_dir, spec.name) + ext,
                      'w') as f:
                f.write(spec.name)

    def test_input_validation_fail(self):
        self.assertRaises(
            ArcanaNoConverterError,
            self.create_study,
            TestInputValidationStudy,
            'test_validation_fail',
            inputs=[
                InputFilesets('a', 'a', test1_format),
                InputFilesets('b', 'b', test3_format),
                InputFilesets('c', 'c', test3_format),
                InputFilesets('d', 'd', test3_format)])


class AlwaysRaisedError(Exception):
    pass


class ErrorInterfaceInputSpec(TraitedSpec):

    in_file = File('a file')


class ErrorInterfaceOutputSpec(TraitedSpec):

    out_file = File('a file')


class ErrorInterface(BaseInterface):

    input_spec = ErrorInterfaceInputSpec
    output_spec = ErrorInterfaceOutputSpec

    def _run_interface(self, runtime):
        raise AlwaysRaisedError


class BasicTestStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        InputFilesetSpec('fileset', text_format),
        FilesetSpec('out_fileset', text_format, 'a_pipeline'),
        FilesetSpec('raise_error', text_format, 'raise_error_pipeline')]

    def a_pipeline(self, **name_maps):

        pipeline = self.new_pipeline(
            'a_pipeline',
            desc='a dummy pipeline',
            citations=[],
            name_maps=name_maps)

        pipeline.add(
            'ident',
            IdentityInterface(
                ['fileset']),
            inputs={
                'fileset': ('fileset', text_format)},
            outputs={
                'out_fileset': ('fileset', text_format)})

        return pipeline

    def raise_error_pipeline(self, **name_maps):

        pipeline = self.new_pipeline(
            'raise_error_pipeline',
            desc='a pipeline that always throws an error',
            citations=[],
            name_maps=name_maps)

        pipeline.add(
            'error',
            ErrorInterface(),
            inputs={
                'in_file': ('fileset', text_format)},
            outputs={
                'raise_error': ('out_file', text_format)})

        return pipeline


class TestInterfaceErrorHandling(BaseTestCase):

    INPUT_FILESETS = {'fileset': 'foo'}

    def test_raised_error(self):
        study = self.create_study(
            BasicTestStudy,
            'base',
            inputs=[InputFilesets('fileset', 'fileset', text_format)])

        # Disable error logs as it should always throw an error
        logger = logging.getLogger('nipype.workflow')
        orig_level = logger.level
        logger.setLevel(50)
        self.assertRaises(
            RuntimeError,
            study.data,
            'raise_error')
        logger.setLevel(orig_level)


class TestGeneratedPickle(BaseTestCase):

    INPUT_FILESETS = {'fileset': 'foo'}

    def test_generated_cls_pickle(self):
        GeneratedClass = StudyMetaClass(
            'GeneratedClass', (BasicTestStudy,), {})
        study = self.create_study(
            GeneratedClass,
            'gen_cls',
            inputs=[InputFilesets('fileset', 'fileset', text_format)])
        pkl_path = os.path.join(self.work_dir, 'gen_cls.pkl')
        with open(pkl_path, 'wb') as f:
            pkl.dump(study, f)
        del GeneratedClass
        with open(pkl_path, 'rb') as f:
            regen = pkl.load(f)
        self.assertContentsEqual(regen.data('out_fileset'), 'foo')

    def test_multi_study_generated_cls_pickle(self):
        cls_dct = {
            'add_substudy_specs': [
                SubStudySpec('ss1', BasicTestStudy),
                SubStudySpec('ss2', BasicTestStudy)]}
        MultiGeneratedClass = MultiStudyMetaClass(
            'MultiGeneratedClass', (MultiStudy,), cls_dct)
        study = self.create_study(
            MultiGeneratedClass,
            'multi_gen_cls',
            inputs=[InputFilesets('ss1_fileset', 'fileset', text_format),
                    InputFilesets('ss2_fileset', 'fileset', text_format)])
        pkl_path = os.path.join(self.work_dir, 'multi_gen_cls.pkl')
        with open(pkl_path, 'wb') as f:
            pkl.dump(study, f)
        del MultiGeneratedClass
        with open(pkl_path, 'rb') as f:
            regen = pkl.load(f)
        self.assertContentsEqual(regen.data('ss2_out_fileset'), 'foo')

    def test_genenerated_method_pickle_fail(self):
        cls_dct = {
            'add_substudy_specs': [
                SubStudySpec('ss1', BasicTestStudy),
                SubStudySpec('ss2', BasicTestStudy)],
            'default_fileset_pipeline': MultiStudy.translate(
                'ss1', 'pipeline')}
        MultiGeneratedClass = MultiStudyMetaClass(
            'MultiGeneratedClass', (MultiStudy,), cls_dct)
        study = self.create_study(
            MultiGeneratedClass,
            'multi_gen_cls',
            inputs=[InputFilesets('ss1_fileset', 'fileset', text_format),
                    InputFilesets('ss2_fileset', 'fileset', text_format)])
        pkl_path = os.path.join(self.work_dir, 'multi_gen_cls.pkl')
        with open(pkl_path, 'w') as f:
            self.assertRaises(
                ArcanaCantPickleStudyError,
                pkl.dump,
                study,
                f)
