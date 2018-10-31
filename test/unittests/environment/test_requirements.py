from unittest import TestCase
from arcana.exception import (
    ArcanaError, ArcanaVersionException)
from arcana.environment.requirement import (
    PythonPackageRequirement, Version)
from arcana.environment.requirement.matlab import matlab_req


a_req = PythonPackageRequirement('a')
a_req_rnge = a_req.v('2.0.1', '10.2.1')
b_req = PythonPackageRequirement('b')
b_req_rnge = b_req.v('0.0.9', '1.0.1')
c_req = PythonPackageRequirement('c')
c_req_rnge = c_req.v('1.8.13', '2.22.0')


class TestRequirements(TestCase):

    def test_version_split(self):
        self.assertEqual(Version.parse('12.2.9'), ((12, 2, 9), None, None))
        self.assertEqual(matlab_req.parse_version('2015b'),
                         (2015, 'b'))
        self.assertEqual(matlab_req.parse_version('r2014a'),
                         (2014, 'a'))
        self.assertEqual(matlab_req.parse_version('R2017b'),
                         (2017, 'b'))
        self.assertEqual(matlab_req.parse_version('R2017B'),
                         (2017, 'b'))

    def test_best_version(self):
        self.assertEqual(
            a_req_rnge.latest_within(['2.0.0', '2.0.1']),
            a_req.v('2.0.1'))
        self.assertEqual(
            a_req_rnge.latest_within(['2.0.0', '3.0.9', 'waa'],
                                     ignore_unrecognised=True),
            a_req.v('3.0.9'))
        self.assertEqual(
            b_req_rnge.latest_within(['0.0.8', '0.0.9']),
            b_req.v('0.0.9'))
        self.assertEqual(
            c_req_rnge.latest_within(['1.9.v4', '1.2.9', '3.0.1']),
            c_req.v('1.9.v4'))

    def test_exceptions(self):
        req = PythonPackageRequirement('anything')
        self.assertRaises(
            ArcanaError,
            req.v,
            (2, 1, 10),
            (2, 0, 11))
        self.assertRaises(
            ArcanaVersionException,
            a_req_rnge.latest_within,
            ['2.0.0', '1.9.1'])
        self.assertRaises(
            ArcanaVersionException,
            b_req_rnge.latest_within,
            ['2.2.0', '3.4.1'])
