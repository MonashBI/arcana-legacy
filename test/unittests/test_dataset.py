import tempfile
import shutil
import os.path
import cPickle as pkl
from unittest import TestCase
from nipype.interfaces.utility import IdentityInterface
from nianalysis.testing import BaseTestCase, BaseMultiSubjectTestCase
from nianalysis.study.base import Study, StudyMetaClass
from nianalysis.option import OptionSpec
from nianalysis.dataset import DatasetSpec, FieldSpec, DatasetMatch
from mbianalysis.data_format import (
    nifti_gz_format, dicom_format, text_format)


class TestDatasetSpecPickle(TestCase):

    datasets = []
    fields = []

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.pkl_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)
        shutil.rmtree(self.pkl_dir)

    def test_dataset_and_field(self):
        objs = [DatasetSpec('a', nifti_gz_format,
                            'dummy_pipeline1'),
                FieldSpec('b', int, 'dummy_pipeline2')]
        for i, obj in enumerate(objs):
            fname = os.path.join(self.pkl_dir, '{}.pkl'.format(i))
            with open(fname, 'w') as f:
                pkl.dump(obj, f)
            with open(fname) as f:
                re_obj = pkl.load(f)
            self.assertEqual(obj, re_obj)


class TestMatchStudy(Study):

    __metaclass__ = StudyMetaClass

    add_data_specs = [
        DatasetSpec('gre_phase', dicom_format),
        DatasetSpec('gre_mag', dicom_format)]

    def dummy_pipeline1(self):
        pass

    def dummy_pipeline2(self):
        pass


class TestDatasetMatching(BaseMultiSubjectTestCase):
    pass


class TestDicomTagMatch(BaseTestCase):

    IMAGE_TYPE_TAG = ('0008', '0008')
    GRE_PATTERN = 'gre_field_mapping_3mm.*'
    PHASE_IMAGE_TYPE = ['ORIGINAL', 'PRIMARY', 'P', 'ND']
    MAG_IMAGE_TYPE = ['ORIGINAL', 'PRIMARY', 'M', 'ND', 'NORM']
    DICOM_MATCH = [
        DatasetMatch('gre_phase', dicom_format, GRE_PATTERN,
                     dicom_tags={IMAGE_TYPE_TAG: PHASE_IMAGE_TYPE},
                     is_regex=True),
        DatasetMatch('gre_mag', dicom_format, GRE_PATTERN,
                     dicom_tags={IMAGE_TYPE_TAG: MAG_IMAGE_TYPE},
                     is_regex=True)]

    def test_dicom_match(self):
        study = self.create_study(
            TestMatchStudy, 'test_dicom',
            inputs=self.DICOM_MATCH)
        phase = study.data('gre_phase')[0]
        mag = study.data('gre_mag')[0]
        self.assertEqual(phase.name, 'gre_field_mapping_3mm_phase')
        self.assertEqual(mag.name, 'gre_field_mapping_3mm_mag')

    def test_order_match(self):
        study = self.create_study(
            TestMatchStudy, 'test_dicom',
            inputs=[
                DatasetMatch('gre_phase', dicom_format,
                             pattern=self.GRE_PATTERN, order=1,
                             is_regex=True),
                DatasetMatch('gre_mag', dicom_format,
                             pattern=self.GRE_PATTERN, order=0,
                             is_regex=True)])
        phase = study.data('gre_phase')[0]
        mag = study.data('gre_mag')[0]
        self.assertEqual(phase.name, 'gre_field_mapping_3mm_phase')
        self.assertEqual(mag.name, 'gre_field_mapping_3mm_mag')


class TestDerivableStudy(Study):

    __metaclass__ = StudyMetaClass

    add_data_specs = [
        DatasetSpec('required', text_format),
        DatasetSpec('optional', text_format, optional=True),
        DatasetSpec('derivable', text_format, 'pipeline1'),
        DatasetSpec('mising_input', text_format, 'pipeline2'),
        DatasetSpec('wrong_option', text_format, 'pipeline3')]

    add_option_specs = [
        OptionSpec()]

    def pipeline1(self):
        pipeline = self.create_pipeline(
            'pipeline1',
            inputs=[DatasetSpec('required', text_format)],
            outputs=[DatasetSpec('derivable', text_format)],
            version=1)
        identity = pipeline.create_node(IdentityInterface(['a']))
        pipeline.connect_input('required', identity, 'a')
        pipeline.connect_output('derivable', identity, 'a')
        return pipeline

    def pipeline2(self):
        pipeline = self.create_pipeline(
            'pipeline2',
            inputs=[DatasetSpec('required', text_format),
                    DatasetSpec('mising_input', text_format)],
            outputs=[DatasetSpec('derivable', text_format)],
            version=1)
        identity = pipeline.create_node(
            IdentityInterface(['a', 'b']))
        pipeline.connect_input('required', identity, 'a')
        pipeline.connect_input('mising_input', identity, 'b')
        pipeline.connect_output('derivable', identity, 'a')
        return pipeline


class TestDerivable(BaseTestCase):

    def test_derivable(self):
        study = self.create_study(
            TestDerivableStudy,
            inputs=[DatasetMatch('required', text_format, 'required')])
        self.assertTrue(study.bound_data_spec('derivable').derivable)
        self.assertFalse(
            study.bound_data_spec('mising_input').derivable)
        self.assertFalse(
            study.bound_data_spec('wrong_option').derivable)

    