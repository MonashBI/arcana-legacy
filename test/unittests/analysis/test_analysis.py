from builtins import str
import os.path
# from nipype import config
# config.enable_debug_mode()
from arcana.data.file_format import text_format
from arcana.analysis.base import Analysis, AnalysisMetaClass
from arcana.utils.testing import (
    BaseTestCase, BaseMultiSubjectTestCase, TestMath)
from arcana.exceptions import (
    ArcanaCantPickleAnalysisError, ArcanaUsageError)
from arcana.analysis.multi import (
    MultiAnalysis, MultiAnalysisMetaClass, SubCompSpec)
from nipype.interfaces.base import (
    BaseInterface, File, TraitedSpec, traits, isdefined)
from arcana.analysis.parameter import ParamSpec
from arcana.data.file_format import FileFormat, IdentityConverter
from nipype.interfaces.utility import IdentityInterface
from arcana.exceptions import ArcanaNoConverterError
from arcana.repository import Tree
from arcana.data import (
    Fileset, FieldSpec, InputFilesetSpec, FilesetFilter, FilesetSpec)
from future.utils import PY2
from future.utils import with_metaclass
import logging
if PY2:
    import pickle as pkl  # @UnusedImport
else:
    import pickle as pkl  # @Reimport


class ExampleAnalysis(with_metaclass(AnalysisMetaClass, Analysis)):

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
        FilesetSpec('analysis_summary', text_format,
                    'analysis_summary_pipeline',
                    frequency='per_dataset'),
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

    def analysis_summary_pipeline(self, **name_maps):
        pipeline = self.new_pipeline(
            name="analysis_summary",
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
        pipeline.connect_output('analysis_summary', math2, 'z')
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


class TestAnalysis(BaseMultiSubjectTestCase):

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
        return Tree.construct(self.dataset.repository, filesets=filesets)

    def make_analysis(self):
        return self.create_analysis(
            ExampleAnalysis, 'dummy', inputs=[
                FilesetFilter('one', 'one_input', text_format),
                FilesetFilter('ten', 'ten_input', text_format)],
            parameters={'pipeline_parameter': True})

    def test_run_pipeline_with_prereqs(self):
        analysis = self.make_analysis()
        self.assertContentsEqual(analysis.data('derived4', derive=True),
                                 [2.0, 2.0, 2.0, 2.0, 2.0, 2.0])

    def test_pipeline_args(self):
        analysis = self.make_analysis()
        a = next(iter(analysis.data('derived5a', derive=True))).value
        b = next(iter(analysis.data('derived5b', derive=True))).value
        self.assertEqual(a, 'a')
        self.assertEqual(b, 'b')

    def test_subject_summary(self):
        analysis = self.make_analysis()
        summaries = analysis.data('subject_summary', derive=True)
        ref = float(len(self.VISIT_IDS))
        for fileset in summaries:
            self.assertContentsEqual(fileset, ref,
                                     str(fileset.visit_id))

    def test_visit_summary(self):
        analysis = self.make_analysis()
        summaries = analysis.data('visit_summary', derive=True)
        ref = float(len(self.SUBJECT_IDS))
        for fileset in summaries:
            self.assertContentsEqual(fileset, ref,
                                     str(fileset.visit_id))

    def test_analysis_summary(self):
        analysis = self.make_analysis()
        ref = float(len(self.SUBJECT_IDS) * len(self.VISIT_IDS))
        self.assertContentsEqual(analysis.data('analysis_summary', derive=True), ref)

    def test_subject_ids_access(self):
        analysis = self.make_analysis()
        analysis.data('subject_ids', derive=True)
        for visit_id in self.VISIT_IDS:
            subject_ids_path = self.output_file_path(
                'subject_ids.txt', analysis.name,
                visit=visit_id, frequency='per_visit')
            with open(subject_ids_path) as f:
                ids = f.read().split('\n')
            self.assertEqual(sorted(ids), sorted(self.SUBJECT_IDS))

    def test_visit_ids_access(self):
        analysis = self.make_analysis()
        analysis.data('visit_ids', derive=True)
        for subject_id in self.SUBJECT_IDS:
            visit_ids_path = self.output_file_path(
                'visit_ids.txt', analysis.name,
                subject=subject_id, frequency='per_subject')
            with open(visit_ids_path) as f:
                ids = f.read().split('\n')
            self.assertEqual(sorted(ids), sorted(self.VISIT_IDS))


class ExistingPrereqAnalysis(with_metaclass(AnalysisMetaClass, Analysis)):

    add_data_specs = [
        InputFilesetSpec('one', text_format),
        FilesetSpec('ten', text_format, 'ten_pipeline'),
        FilesetSpec('hundred', text_format, 'hundred_pipeline'),
        FilesetSpec('thousand', text_format, 'thousand_pipeline')]

    def pipeline_factory(self, incr, input, output, name_maps):
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

    def ten_pipeline(self, **name_maps):
        return self.pipeline_factory(10, 'one', 'ten', name_maps=name_maps)

    def hundred_pipeline(self, **name_maps):
        return self.pipeline_factory(100, 'ten', 'hundred',
                                     name_maps=name_maps)

    def thousand_pipeline(self, **name_maps):
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

    STUDY_NAME = 'analysis'

    @property
    def input_tree(self):
        filesets = []
        for subj_id, visit_ids in list(self.PROJECT_STRUCTURE.items()):
            for visit_id, fileset_names in list(visit_ids.items()):
                # Create filesets
                for name in fileset_names:
                    from_analysis = self.STUDY_NAME if name != 'one' else None
                    filesets.append(
                        Fileset(name, text_format, subject_id=subj_id,
                                visit_id=visit_id,
                                from_analysis=from_analysis))
        return Tree.construct(self.dataset.repository, filesets=filesets)

    def add_sessions(self):
        BaseMultiSubjectTestCase.add_sessions(self)
        # Create a analysis object, in order to generate appropriate provenance
        # for the existing "derived" data
        derived_filesets = [f for f in self.DATASET_CONTENTS
                            if f != 'one']
        analysis = self.create_analysis(
            ExistingPrereqAnalysis, self.STUDY_NAME,
            dataset=self.local_dataset,
            inputs=[FilesetFilter('one', 'one', text_format)])
        # Get all pipelines in the analysis
        pipelines = {n: getattr(analysis, '{}_pipeline'.format(n))()
                     for n in derived_filesets}
        for node in analysis.dataset.tree:
            for fileset in node.filesets:
                if fileset.basename != 'one' and fileset.exists:
                    # Generate expected provenance record for each pipeline
                    # and save in the local dataset
                    pipelines[fileset.name].cap()
                    record = pipelines[fileset.name].expected_record(node)
                    self.local_dataset.put_record(record)
        analysis.clear_caches()  # Reset dataset trees

    def test_per_session_prereqs(self):
        # Generate all data for 'thousand' spec
        analysis = self.create_analysis(
            ExistingPrereqAnalysis, self.STUDY_NAME,
            inputs=[FilesetFilter('one', 'one', text_format)])
        analysis.data('thousand', derive=True)
        targets = {
            'subject1': {
                'visit1': 1100.0,
                'visit2': 1110.0,
                'visit3': 1000.0},
            'subject2': {
                'visit1': 1111.0,
                'visit2': 1110.0,
                'visit3': 1000.0}}
        tree = self.dataset.tree
        for subj_id, visits in self.PROJECT_STRUCTURE.items():
            for visit_id in visits:
                session = tree.subject(subj_id).session(visit_id)
                fileset = session.fileset('thousand',
                                          from_analysis=self.STUDY_NAME)
                fileset.format = text_format
                self.assertContentsEqual(
                    fileset, targets[subj_id][visit_id],
                    "{}:{}".format(subj_id, visit_id))


test1_format = FileFormat('test1', extension='.t1')
test2_format = FileFormat('test2', extension='.t2')
test3_format = FileFormat('test3', extension='.t3')

test2_format.set_converter(test1_format, IdentityConverter)


class TestInputValidationAnalysis(with_metaclass(AnalysisMetaClass, Analysis)):

    add_data_specs = [
        InputFilesetSpec('a', (test1_format, test2_format)),
        InputFilesetSpec('b', test3_format),
        FilesetSpec('c', test2_format, 'identity_pipeline'),
        FilesetSpec('d', test3_format, 'identity_pipeline')]

    def identity_pipeline(self, **name_maps):
        pipeline = self.new_pipeline(
            name='pipeline',
            desc="A dummy pipeline used to test analysis input validation",
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
            with open(os.path.join(self.session_dir, spec_name)
                      + fformat.ext, 'w') as f:
                f.write(spec_name)

    def test_input_validation(self):
        self.create_analysis(
            TestInputValidationAnalysis,
            'test_input_validation',
            inputs=[
                FilesetFilter('a', 'a', test1_format),
                FilesetFilter('b', 'b', test3_format),
                FilesetFilter('c', 'a', test1_format),
                FilesetFilter('d', 'd', test3_format)])


class TestInputValidationFail(BaseTestCase):

    def setUp(self):
        self.reset_dirs()
        os.makedirs(self.session_dir)
        for spec in TestInputValidationAnalysis.data_specs():
            with open(os.path.join(self.session_dir, spec.name)
                      + test3_format.ext, 'w') as f:
                f.write(spec.name)

    def test_input_validation_fail(self):
        self.assertRaises(
            ArcanaUsageError,
            self.create_analysis,
            TestInputValidationAnalysis,
            'test_validation_fail',
            inputs=[
                FilesetFilter('a', 'a', test3_format),
                FilesetFilter('b', 'b', test3_format)])


class TestInputNoConverter(BaseTestCase):

    def setUp(self):
        self.reset_dirs()
        os.makedirs(self.session_dir)
        for spec in TestInputValidationAnalysis.data_specs():
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
            self.create_analysis,
            TestInputValidationAnalysis,
            'test_validation_fail',
            inputs=[
                FilesetFilter('a', 'a', test1_format),
                FilesetFilter('b', 'b', test3_format),
                FilesetFilter('c', 'c', test3_format),
                FilesetFilter('d', 'd', test3_format)])


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


class BasicTestAnalysis(with_metaclass(AnalysisMetaClass, Analysis)):

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
        analysis = self.create_analysis(
            BasicTestAnalysis,
            'base',
            inputs=[FilesetFilter('fileset', 'fileset', text_format)])

        # Disable error logs as it should always throw an error
        logger = logging.getLogger('nipype.workflow')
        orig_level = logger.level
        logger.setLevel(50)
        self.assertRaises(
            RuntimeError,
            analysis.data,
            'raise_error')
        logger.setLevel(orig_level)


class TestGeneratedPickle(BaseTestCase):

    INPUT_FILESETS = {'fileset': 'foo'}

    def test_generated_cls_pickle(self):
        GeneratedClass = AnalysisMetaClass(
            'GeneratedClass', (BasicTestAnalysis,), {})
        analysis = self.create_analysis(
            GeneratedClass,
            'gen_cls',
            inputs=[FilesetFilter('fileset', 'fileset', text_format)])
        pkl_path = os.path.join(self.work_dir, 'gen_cls.pkl')
        with open(pkl_path, 'wb') as f:
            pkl.dump(analysis, f)
        del GeneratedClass
        with open(pkl_path, 'rb') as f:
            regen = pkl.load(f)
        self.assertContentsEqual(regen.data('out_fileset', derive=True), 'foo')

    def test_multi_analysis_generated_cls_pickle(self):
        cls_dct = {
            'add_subcomp_specs': [
                SubCompSpec('ss1', BasicTestAnalysis),
                SubCompSpec('ss2', BasicTestAnalysis)]}
        MultiGeneratedClass = MultiAnalysisMetaClass(
            'MultiGeneratedClass', (MultiAnalysis,), cls_dct)
        analysis = self.create_analysis(
            MultiGeneratedClass,
            'multi_gen_cls',
            inputs=[FilesetFilter('ss1_fileset', 'fileset', text_format),
                    FilesetFilter('ss2_fileset', 'fileset', text_format)])
        pkl_path = os.path.join(self.work_dir, 'multi_gen_cls.pkl')
        with open(pkl_path, 'wb') as f:
            pkl.dump(analysis, f)
        del MultiGeneratedClass
        with open(pkl_path, 'rb') as f:
            regen = pkl.load(f)
        self.assertContentsEqual(regen.data('ss2_out_fileset', derive=True), 'foo')

    def test_genenerated_method_pickle_fail(self):
        cls_dct = {
            'add_subcomp_specs': [
                SubCompSpec('ss1', BasicTestAnalysis),
                SubCompSpec('ss2', BasicTestAnalysis)],
            'default_fileset_pipeline': MultiAnalysis.translate(
                'ss1', 'pipeline')}
        MultiGeneratedClass = MultiAnalysisMetaClass(
            'MultiGeneratedClass', (MultiAnalysis,), cls_dct)
        analysis = self.create_analysis(
            MultiGeneratedClass,
            'multi_gen_cls',
            inputs=[FilesetFilter('ss1_fileset', 'fileset', text_format),
                    FilesetFilter('ss2_fileset', 'fileset', text_format)])
        pkl_path = os.path.join(self.work_dir, 'multi_gen_cls.pkl')
        with open(pkl_path, 'w') as f:
            self.assertRaises(
                ArcanaCantPickleAnalysisError,
                pkl.dump,
                analysis,
                f)
