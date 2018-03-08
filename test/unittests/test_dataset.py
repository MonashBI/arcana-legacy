from unittest import TestCase
from nianalysis.exceptions import NiAnalysisUsageError
from nianalysis.dataset import Dataset, Field
from nianalysis.data_formats import nifti_gz_format


class TestDataset(TestCase):

    def test_unprocessed_per_nonsession(self):
        self.assertRaises(
            NiAnalysisUsageError,
            Dataset,
            'a_dataset',
            format=nifti_gz_format,
            processed=False,
            multiplicity='per_subject')
        self.assertRaises(
            NiAnalysisUsageError,
            Field,
            'a_field',
            dtype=int,
            processed=False,
            multiplicity='per_subject')
        Dataset(
            'a_dataset',
            format=nifti_gz_format,
            processed=True,
            multiplicity='per_subject')
        Field(
            'a_field',
            dtype=int,
            processed=True,
            multiplicity='per_subject')
