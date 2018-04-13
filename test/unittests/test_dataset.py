import tempfile
import shutil
import os.path
import cPickle as pkl
from unittest import TestCase
from nianalysis.study.base import Study
from nianalysis.dataset import DatasetSpec, FieldSpec
from nianalysis.data_formats import nifti_gz_format


class DummyStudy(Study):

    def dummy_pipeline1(self):
        pass

    def dummy_pipeline2(self):
        pass


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
