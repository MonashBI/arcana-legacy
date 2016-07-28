import os.path
import subprocess as sp
from unittest import TestCase

test_data_dir = os.path.join(os.path.dirname(__file__), '..', 'test', '_data')


class BaseImageTestCase(TestCase):

    ARCHIVE_PATH = os.path.join(os.environ['HOME'], 'Data', 'MBI', 'noddi')
    EXAMPLE_INPUT_PROJECT = 'example_input'
    PILOT_PROJECT = 'pilot'
    EXAMPLE_OUTPUT_PROJECT = 'example_output'
    SUBJECT = 'SUBJECT1'
    SESSION = 'SESSION1'

    def _session_dir(self, project):
        return os.path.join(self.ARCHIVE_PATH, project, self.SUBJECT,
                            self.SESSION)

    def _remove_generated_files(self, project):
        # Remove processed scans
        for fname in os.listdir(self._session_dir(project)):
            if fname.startswith(self.DATASET_NAME):
                os.remove(os.path.join(self._session_dir(project), fname))

    def assertImagesMatch(self, a, b):
        try:
            sp.check_output('diff {}.nii {}.nii'.format(a, b), shell=True)
        except sp.CalledProcessError as e:
            if e.output == "Binary files {} and {} differ\n".format(a, b):
                self.assert_(
                    False,
                    "Images {} and {} do not match exactly".format(a, b))
            else:
                raise

    def assertImagesAlmostMatch(self, a, b, mean_threshold, stdev_threshold):
        cmd = ("mrcalc -quiet {a} {b} -subtract - | mrstats - | "
               "grep -v channel | awk '{{print $4 \" \" $6}}'"
               .format(a=a, b=b))
        out = sp.check_output(cmd, shell=True)
        mean, stdev = (float(x) for x in out.split())
        self.assert_(
            abs(mean) < mean_threshold and stdev < stdev_threshold,
            ("Mean ({mean}) or standard deviation ({stdev}) of difference "
             "between images {a} and {b} differ more than threshold(s) "
             "({thresh_mean} and {thresh_stdev} respectively)"
             .format(mean=mean, stdev=stdev, thresh_mean=mean_threshold,
                     thresh_stdev=stdev_threshold, a=a, b=b)))


class DummyTestCase(BaseImageTestCase):

    def __init__(self):
        self.setUp()

    def __del__(self):
        self.tearDown()

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def assert_(self, statement, message=None):
        if not statement:
            message = "'{}' is not true".format(statement)
            print message
        else:
            print "Test successful"

    def assertEqual(self, first, second, message=None):
        if first != second:
            if message is None:
                message = '{} and {} are not equal'.format(repr(first),
                                                           repr(second))
            print message
        else:
            print "Test successful"

    def assertAlmostEqual(self, first, second, message=None):
        if first != second:
            if message is None:
                message = '{} and {} are not equal'.format(repr(first),
                                                           repr(second))
            print message
        else:
            print "Test successful"

    def assertLess(self, first, second, message=None):
        if first >= second:
            if message is None:
                message = '{} is not less than {}'.format(repr(first),
                                                          repr(second))
            print message
        else:
            print "Test successful"
