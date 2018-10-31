from unittest import TestCase
from arcana.exception import (
    ArcanaError, ArcanaVersionException)
from arcana.environment.requirement import (
    PythonPackageRequirement, Version)
from arcana.environment.requirement.matlab import matlab_req, MatlabVersion


a_req = PythonPackageRequirement('a')
a_req_rnge = a_req.v('2.0.1', '10.2.1')
b_req = PythonPackageRequirement('b')
b_req_rnge = b_req.v('0.0.9', '1.0.1')
c_req = PythonPackageRequirement('c')
c_req_rnge = c_req.v('1.8.13', '2.22.0')


class TestRequirements(TestCase):

    def test_version_split(self):
        self.assertEqual(Version.parse('12.2.9'), ((12, 2, 9), None, None))
        self.assertEqual(Version.parse('0.1a2'),
                         ((0, 1), ('a', 2), None))
        self.assertEqual(Version.parse('0.1.3a2'),
                         ((0, 1, 3), ('a', 2), None))
        self.assertEqual(Version.parse('0.1.3beta4'),
                         ((0, 1, 3), ('b', 4), None))
        self.assertEqual(Version.parse('0.1.3beta4.dev12'),
                         ((0, 1, 3), ('b', 4), 12))
        self.assertEqual(Version.parse('4.0.5_RC10'),
                         ((4, 0, 5), ('rc', 10), None))
        self.assertEqual(Version.parse('4.0.5rc2'),
                         ((4, 0, 5), ('rc', 2), None))
        self.assertEqual(MatlabVersion.parse('2015b'),
                         ((2015, 'b'), None, None))
        self.assertEqual(MatlabVersion.parse('r2014a'),
                         ((2014, 'a'), None, None))
        self.assertEqual(MatlabVersion.parse('R2017b'),
                         ((2017, 'b'), None, None))
        self.assertEqual(MatlabVersion.parse('R2017B'),
                         ((2017, 'b'), None, None))

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
            c_req_rnge.latest_within(['1.9.4', '1.2.9', '3.0.1']),
            c_req.v('1.9.4'))

    def test_exceptions(self):
        req = PythonPackageRequirement('anything')
        self.assertRaises(
            ArcanaError,
            req.v,
            '2.1.10',
            '2.0.11')
        self.assertRaises(
            ArcanaVersionException,
            a_req_rnge.latest_within,
            ['2.0.0', '1.9.1'])
        self.assertRaises(
            ArcanaVersionException,
            b_req_rnge.latest_within,
            ['2.2.0', '3.4.1'])
