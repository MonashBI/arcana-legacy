import tempfile
import shutil
import os.path
import cPickle as pkl
from unittest import TestCase
from nianalysis.testing import BaseMultiSubjectTestCase
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

    def test_dicom_match(self):
        image_type_tag = ('0008', '0008')
        gre_pattern = 'gre_field_mapping_3mm.*'
        phase_image_type = ['ORIGINAL', 'PRIMARY', 'P', 'ND']
        mag_image_type = ['ORIGINAL', 'PRIMARY', 'M', 'ND', 'NORM']
        study = self.create_study(
            TestMatchStudy, 'test_dicom',
            inputs=[
                DatasetMatch(
                    'gre_phase', gre_pattern, format=dicom_format,
                    dicom_tags={image_type_tag:
                                phase_image_type},
                    is_regex=True),
                DatasetMatch(
                    'gre_mag', gre_pattern, format=dicom_format,
                    dicom_tags={image_type_tag:
                                mag_image_type},
                    is_regex=True)])
        phase = study.data('gre_phase')[0]
        mag = study.data('gre_mag')[0]
        self.assertEqual(phase.name, 'gre_field_mapping_3mm_phase')
        self.assertEqual(mag.name, 'gre_field_mapping_3mm_mag')
