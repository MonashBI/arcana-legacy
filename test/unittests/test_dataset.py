import tempfile
import shutil
import os.path
import cPickle as pkl
import pydicom
from unittest import TestCase
from nianalysis.testing import BaseTestCase, BaseMultiSubjectTestCase
from nianalysis.study.base import Study, StudyMetaClass
from nianalysis.dataset import DatasetSpec, FieldSpec, DatasetMatch
from nianalysis.data_formats import nifti_gz_format, dicom_format


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


class TestDicomTagMatch(BaseMultiSubjectTestCase):

    IMAGE_TYPE_TAG = ('0008', '0008')
    PHASE_IMAGE_TYPE = 'ORIGINAL\PRIMARY\P\ND'
    MAG_IMAGE_TYPE = 'ORIGINAL\PRIMARY\M\ND\NORM'
    GRE_TYPE = 'gre_field_mapping_3mm'

    INPUTS = [
        DatasetMatch(
            'gre_phase', GRE_TYPE, format=dicom_format,
            dicom_tags={IMAGE_TYPE_TAG, PHASE_IMAGE_TYPE}),
        DatasetMatch(
            'gre_mag', GRE_TYPE, format=dicom_format,
            dicom_tags={IMAGE_TYPE_TAG, MAG_IMAGE_TYPE})]

    def test_dicom_match(self):
        study = self.create_study(
            TestMatchStudy, 'test_dicom', inputs=self.INPUTS)
        self._test_dicom_match(study)

    def _test_dicom_match(self, study):
        phase = study.data('gre_phase')[0]
        mag = study.data('gre_mag')[0]
        phase_type = '\\'.join(
            self.read_dicom(phase)[self.IMAGE_TYPE_TAG])
        mag_type = '\\'.join(
            self.read_dicom(mag)[self.IMAGE_TYPE_TAG])
        self.assertEqual(phase_type, self.PHASE_IMAGE_TYPE)
        self.assertEqual(mag_type, self.MAG_IMAGE_TYPE)

    def read_dicom(self, dataset):
        fnames = os.listdir(dataset.path)
        with open(os.path.join(dataset.path, fnames[0])) as f:
            dcm = pydicom.dcmread(f)
        return dcm
