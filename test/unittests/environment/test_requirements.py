from unittest import TestCase
from arcana.exceptions import (
    ArcanaError, ArcanaVersionError)
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
        self.assertEqual(a_req.v('12.2.9').sequence, (12, 2, 9))
        a_ver = a_req.v('0.1a2')
        self.assertEqual(a_ver.sequence, (0, 1))
        self.assertEqual(a_ver.prerelease, ('a', 2))
        a_ver = a_req.v('0.1.3a2')
        self.assertEqual(a_ver.sequence, (0, 1, 3))
        self.assertEqual(a_ver.prerelease, ('a', 2))
        a_ver = a_req.v('0.1.3beta4')
        self.assertEqual(a_ver.sequence, (0, 1, 3))
        self.assertEqual(a_ver.prerelease, ('b', 4))
        a_ver = a_req.v('0.1.3beta4.dev12')
        self.assertEqual(a_ver.sequence, (0, 1, 3))
        self.assertEqual(a_ver.prerelease, ('b', 4))
        self.assertEqual(a_ver.dev, 12)
        a_ver = a_req.v('4.0.5_RC10')
        self.assertEqual(a_ver.sequence, (4, 0, 5))
        self.assertEqual(a_ver.prerelease, ('rc', 10))
        a_ver = a_req.v('4.0.5rc2')
        self.assertEqual(a_ver.sequence, (4, 0, 5))
        self.assertEqual(a_ver.prerelease, ('rc', 2))
        self.assertEqual(matlab_req.v('2015b').sequence,
                         (2015, 'b'))
        self.assertEqual(matlab_req.v('r2014a').sequence,
                         (2014, 'a'))
        self.assertEqual(matlab_req.v('R2017b').sequence,
                         (2017, 'b'))
        self.assertEqual(matlab_req.v('R2017B').sequence, (2017, 'b'))

    def test_latest_version(self):
        self.assertEqual(
            a_req_rnge.latest_within([a_req.v('2.0.0'), a_req.v('2.0.1')]),
            a_req.v('2.0.1'))
        self.assertEqual(
            a_req_rnge.latest_within([a_req.v('2.0.0'), a_req.v('3.0.9')]),
            a_req.v('3.0.9'))
        self.assertEqual(
            b_req_rnge.latest_within([b_req.v('0.0.8'), b_req.v('0.0.9')]),
            b_req.v('0.0.9'))
        self.assertEqual(
            c_req_rnge.latest_within([c_req.v('1.9.4'), c_req.v('1.2.9'),
                                      c_req.v('3.0.1')]),
            c_req.v('1.9.4'))

    def test_exceptions(self):
        req = PythonPackageRequirement('anything')
        self.assertRaises(
            ArcanaError,
            req.v,
            '2.1.10',
            '2.0.11')
        self.assertRaises(
            ArcanaVersionError,
            a_req_rnge.latest_within,
            ['2.0.0', '1.9.1'])
        self.assertRaises(
            ArcanaVersionError,
            b_req_rnge.latest_within,
            ['2.2.0', '3.4.1'])
