from future import standard_library
standard_library.install_aliases()
import tempfile  # @IgnorePep8
import shutil  # @IgnorePep8
import os.path  # @IgnorePep8
import unittest  # @IgnorePep8
from unittest import TestCase  # @IgnorePep8
from nipype.interfaces.utility import IdentityInterface  # @IgnorePep8
from arcana.testing import BaseTestCase, BaseMultiSubjectTestCase  # @IgnorePep8
from arcana.study.base import Study, StudyMetaClass  # @IgnorePep8
from arcana.study.parameter import SwitchSpec  # @IgnorePep8
from arcana.data import AcquiredFilesetSpec, FilesetSpec, FieldSpec, FilesetSelector  # @IgnorePep8
from arcana.data.file_format.standard import text_format, FileFormat  # @IgnorePep8
from arcana.exception import ArcanaDesignError # @IgnorePep8
from future.utils import PY2  # @IgnorePep8
from future.utils import with_metaclass  # @IgnorePep8
if PY2:
    import pickle as pkl  # @UnusedImport
else:
    import pickle as pkl  # @Reimport

# For testing DICOM tag matching
dicom_format = FileFormat(name='dicom', extension=None,
                          directory=True, within_dir_exts=['.dcm'])
FileFormat.register(dicom_format)


class TestFilesetSpecPickle(TestCase):

    filesets = []
    fields = []

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.pkl_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)
        shutil.rmtree(self.pkl_dir)

    def test_fileset_and_field(self):
        objs = [FilesetSpec('a', text_format,
                            'dummy_pipeline1'),
                FieldSpec('b', int, 'dummy_pipeline2')]
        for i, obj in enumerate(objs):
            fname = os.path.join(self.pkl_dir, '{}.pkl'.format(i))
            with open(fname, 'wb') as f:
                pkl.dump(obj, f)
            with open(fname, 'rb') as f:
                re_obj = pkl.load(f)
            self.assertEqual(obj, re_obj)


class TestMatchStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        AcquiredFilesetSpec('gre_phase', dicom_format),
        AcquiredFilesetSpec('gre_mag', dicom_format)]

    def dummy_pipeline1(self):
        pass

    def dummy_pipeline2(self):
        pass


class TestFilesetSelecting(BaseMultiSubjectTestCase):

    @unittest.skip("Test not implemented")
    def test_match_pattern(self):
        pass


class TestDicomTagMatch(BaseTestCase):

    IMAGE_TYPE_TAG = ('0008', '0008')
    GRE_PATTERN = 'gre_field_mapping_3mm.*'
    PHASE_IMAGE_TYPE = ['ORIGINAL', 'PRIMARY', 'P', 'ND']
    MAG_IMAGE_TYPE = ['ORIGINAL', 'PRIMARY', 'M', 'ND', 'NORM']
    DICOM_MATCH = [
        FilesetSelector('gre_phase', dicom_format, GRE_PATTERN,
                        dicom_tags={IMAGE_TYPE_TAG: PHASE_IMAGE_TYPE},
                        is_regex=True),
        FilesetSelector('gre_mag', dicom_format, GRE_PATTERN,
                        dicom_tags={IMAGE_TYPE_TAG: MAG_IMAGE_TYPE},
                        is_regex=True)]

    INPUTS_FROM_REF_DIR = True

    def test_dicom_match(self):
        study = self.create_study(
            TestMatchStudy, 'test_dicom',
            inputs=self.DICOM_MATCH)
        phase = list(study.data('gre_phase'))[0]
        mag = list(study.data('gre_mag'))[0]
        self.assertEqual(phase.name, 'gre_field_mapping_3mm_phase')
        self.assertEqual(mag.name, 'gre_field_mapping_3mm_mag')

    def test_order_match(self):
        study = self.create_study(
            TestMatchStudy, 'test_dicom',
            inputs=[
                FilesetSelector('gre_phase', dicom_format,
                                pattern=self.GRE_PATTERN, order=1,
                                is_regex=True),
                FilesetSelector('gre_mag', dicom_format,
                                pattern=self.GRE_PATTERN, order=0,
                                is_regex=True)])
        phase = list(study.data('gre_phase'))[0]
        mag = list(study.data('gre_mag'))[0]
        self.assertEqual(phase.name, 'gre_field_mapping_3mm_phase')
        self.assertEqual(mag.name, 'gre_field_mapping_3mm_mag')


class TestDerivableStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        AcquiredFilesetSpec('required', text_format),
        AcquiredFilesetSpec('optional', text_format, optional=True),
        FilesetSpec('derivable', text_format, 'pipeline1'),
        FilesetSpec('missing_input', text_format, 'pipeline2'),
        FilesetSpec('another_derivable', text_format, 'pipeline3'),
        FilesetSpec('requires_switch', text_format, 'pipeline3'),
        FilesetSpec('requires_switch2', text_format, 'pipeline4'),
        FilesetSpec('requires_foo', text_format, 'pipeline5'),
        FilesetSpec('requires_bar', text_format, 'pipeline5')]

    add_param_specs = [
        SwitchSpec('switch', False),
        SwitchSpec('branch', 'foo', ('foo', 'bar', 'wee'))]

    def pipeline1(self, **name_maps):
        pipeline = self.pipeline(
            'pipeline1',
            desc="",
            references=[],
            name_maps=name_maps)
        identity = pipeline.add('identity', IdentityInterface(['a']))
        pipeline.connect_input('required', identity, 'a')
        pipeline.connect_output('derivable', identity, 'a')
        return pipeline

    def pipeline2(self, **name_maps):
        pipeline = self.pipeline(
            'pipeline2',
            desc="",
            references=[],
            name_maps=name_maps)
        identity = pipeline.add('identity', IdentityInterface(['a', 'b']))
        pipeline.connect_input('required', identity, 'a')
        pipeline.connect_input('optional', identity, 'b')
        pipeline.connect_output('missing_input', identity, 'a')
        return pipeline

    def pipeline3(self, **name_maps):
        pipeline = self.pipeline(
            'pipeline3',
            desc="",
            references=[],
            name_maps=name_maps)
        identity = pipeline.add('identity', IdentityInterface(['a', 'b']))
        pipeline.connect_input('required', identity, 'a')
        pipeline.connect_input('required', identity, 'b')
        pipeline.connect_output('another_derivable', identity, 'a')
        if self.branch('switch'):
            pipeline.connect_output('requires_switch', identity, 'b')
        return pipeline

    def pipeline4(self, **name_maps):
        pipeline = self.pipeline(
            'pipeline4',
            desc="",
            references=[],
            name_maps=name_maps)
        identity = pipeline.add('identity', IdentityInterface(['a']))
        pipeline.connect_input('requires_switch', identity, 'a')
        pipeline.connect_output('requires_switch2', identity, 'a')
        return pipeline

    def pipeline5(self, **name_maps):
        pipeline = self.pipeline(
            'pipeline5',
            desc="",
            references=[],
            name_maps=name_maps)
        identity = pipeline.add('identity', IdentityInterface(['a']))
        pipeline.connect_input('required', identity, 'a')
        if self.branch('branch', 'foo'):
            pipeline.connect_output('requires_foo', identity, 'a')
        elif self.branch('branch', 'bar'):
            pipeline.connect_output('requires_bar', identity, 'a')
        else:
            self.unhandled_branch('branch')
        return pipeline


class TestDerivable(BaseTestCase):

    INPUT_DATASETS = {'required': 'blah'}

    def test_derivable(self):
        # Test vanilla study
        study = self.create_study(
            TestDerivableStudy,
            'study',
            inputs=[FilesetSelector('required', text_format, 'required')])
        self.assertTrue(study.spec('derivable').derivable)
        self.assertTrue(
            study.spec('another_derivable').derivable)
        self.assertFalse(
            study.spec('missing_input').derivable)
        self.assertFalse(
            study.spec('requires_switch').derivable)
        self.assertFalse(
            study.spec('requires_switch2').derivable)
        self.assertTrue(study.spec('requires_foo').derivable)
        self.assertFalse(study.spec('requires_bar').derivable)
        # Test study with 'switch' enabled
        study_with_switch = self.create_study(
            TestDerivableStudy,
            'study_with_switch',
            inputs=[FilesetSelector('required', text_format, 'required')],
            parameters={'switch': True})
        self.assertTrue(
            study_with_switch.spec('requires_switch').derivable)
        self.assertTrue(
            study_with_switch.spec('requires_switch2').derivable)
        # Test study with branch=='bar'
        study_bar_branch = self.create_study(
            TestDerivableStudy,
            'study_bar_branch',
            inputs=[FilesetSelector('required', text_format, 'required')],
            parameters={'branch': 'bar'})
        self.assertFalse(study_bar_branch.spec('requires_foo').derivable)
        self.assertTrue(study_bar_branch.spec('requires_bar').derivable)
        # Test study with optional input
        study_with_input = self.create_study(
            TestDerivableStudy,
            'study_with_inputs',
            inputs=[FilesetSelector('required', text_format, 'required'),
                    FilesetSelector('optional', text_format, 'required')])
        self.assertTrue(
            study_with_input.spec('missing_input').derivable)
        study_unhandled = self.create_study(
            TestDerivableStudy,
            'study_unhandled',
            inputs=[FilesetSelector('required', text_format, 'required')],
            parameters={'branch': 'wee'})
        self.assertRaises(
            ArcanaDesignError,
            getattr,
            study_unhandled.spec('requires_foo'),
            'derivable')
