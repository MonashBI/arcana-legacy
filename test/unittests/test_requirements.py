from unittest import TestCase
from nianalysis.exceptions import (
    NiAnalysisError, NiAnalysisRequirementVersionException)
from nianalysis.requirements import (
    Requirement, split_version, date_split)
from mbianalysis.requirements import matlab_version_split


a_req = Requirement('a', min_version=(2, 0, 1))
b_req = Requirement('b', min_version=(0, 0, 9), max_version=(1, 0, 1))
c_req = Requirement('c', min_version=(1, 8, 13), max_version=(2, 22, 0))


class TestRequirements(TestCase):

    def test_version_split(self):
        self.assertEqual(split_version('12.2.9'), (12, 2, 9))
        self.assertEqual(matlab_version_split('2015b'), (2015, 'b'))
        self.assertEqual(matlab_version_split('r2014a'), (2014, 'a'))
        self.assertEqual(matlab_version_split('R2017b'), (2017, 'b'))
        self.assertEqual(matlab_version_split('R2017B'), (2017, 'b'))
        self.assertEqual(date_split('2017-02-08'), (2017, 2, 8))

    def test_later_or_equal(self):
        self.assertTrue(Requirement.later_or_equal_version(
            (12, 9, 1), (11, 10, 1)))
        self.assertFalse(Requirement.later_or_equal_version(
            (1, 12, 100), (11, 10, 1)))
        self.assertTrue(Requirement.later_or_equal_version(
            (12, 9, 1), (12, 9, 1)))
        self.assertTrue(Requirement.later_or_equal_version(
            (12, 9, 2), (12, 9, 1)))
        self.assertTrue(Requirement.later_or_equal_version(
            (2015, 'b'), (2015, 'a')))
        self.assertFalse(Requirement.later_or_equal_version(
            (2015, 'a'), (2015, 'b')))
        self.assertTrue(Requirement.later_or_equal_version(
            (2016, 'a'), (2015, 'b')))

    def test_best_version(self):
        self.assertEqual(
            a_req.best_version(['2.0.0', '2.0.1']), '2.0.1')
        self.assertEqual(
            a_req.best_version(['2.0.0', '3.0.9', 'waa']), '3.0.9')
        self.assertEqual(
            b_req.best_version(['0.0.8', '0.0.9']), '0.0.9')
        self.assertEqual(
            c_req.best_version(['1.9.v4', '1.2.9', '3.0.1']), '1.9.v4')

    def test_exceptions(self):
        self.assertRaises(
            NiAnalysisError,
            Requirement,
            'anything',
            min_version=(2, 1, 10),
            max_version=(2, 0, 11))
        self.assertRaises(
            NiAnalysisRequirementVersionException,
            a_req.best_version,
            ['2.0.0', '1.9.1'])
        self.assertRaises(
            NiAnalysisRequirementVersionException,
            b_req.best_version,
            ['2.2.0', '3.4.1'])
