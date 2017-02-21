import os.path
import subprocess as sp
import shutil
from unittest import TestCase
import nianalysis
from nianalysis.archive.local import LocalArchive
from nianalysis.archive.xnat import download_all_datasets
import sys

test_data_dir = os.path.join(os.path.dirname(__file__), '..', 'test', '_data')

unittest_base_dir = os.path.abspath(os.path.join(
    os.path.dirname(nianalysis.__file__), '..', 'test', 'unittests'))


class PipelineTeseCase(TestCase):

    ARCHIVE_PATH = os.path.join(test_data_dir, 'archive')
    WORK_PATH = os.path.join(test_data_dir, 'work')
    CACHE_BASE_PATH = os.path.join(test_data_dir, 'cache')
    SUBJECT = 'SUBJECT'
    SESSION = 'SESSION'
    SERVER = 'https://mbi-xnat.erc.monash.edu.au'
    USER = 'unittest'
    PASSWORD = 'Test123!'
    XNAT_TEST_PROJECT = 'TEST001'

    def setUp(self):
        self._create_project(self.project_dir, self.SUBJECT, self.SESSION)

    def _create_project(self, project_dir, subject, session,
                        required_datasets=None):
        self._delete_project(project_dir)
        session_dir = os.path.join(project_dir, subject, session)
        os.makedirs(session_dir)
        cache_dir = os.path.join(self.CACHE_BASE_PATH, session)
        # Get the name of the session on XNAT that contains the test datasets
        rel_module_path = sys.modules[self.__module__].__file__[
            len(unittest_base_dir) + 1:].split(os.path.sep)
        module_id = (''.join(rel_module_path[:-1]) +
                     os.path.splitext(rel_module_path[-1])[0][5:])
        xnat_session_name = '{}_{}_{}'.format(
            self.XNAT_TEST_PROJECT, module_id, type(self).__name__[4:]).upper()
        print xnat_session_name
        download_all_datasets(cache_dir, self.SERVER, self.USER, self.PASSWORD,
                              xnat_session_name, overwrite=False)
        for f in os.listdir(cache_dir):
            if required_datasets is None or f in required_datasets:
                shutil.copy(os.path.join(cache_dir, f),
                            os.path.join(session_dir, f))

    def _delete_project(self, project_dir):
        # Clean out any existing archive files
        shutil.rmtree(project_dir, ignore_errors=True)

    @property
    def session_dir(self):
        return self.get_session_dir(self.name)

    @property
    def archive(self):
        return LocalArchive(self.ARCHIVE_PATH)

    @property
    def project_dir(self):
        return os.path.join(self.ARCHIVE_PATH, self.name)

    @property
    def work_dir(self):
        return os.path.join(self.WORK_PATH, self.name)

    @property
    def name(self):
        return self.TEST_MODULE + '_' + self.TEST_NAME

    def create_study(self, study_cls, name, input_datasets):
        return study_cls(
            name=name,
            project_id=self.name,
            archive=self.archive,
            input_datasets=input_datasets)

    def assertDatasetCreated(self, dataset_name):
        self.assertTrue(os.path.exists(os.path.join(
            self.session_dir, '{}_{}'.format(self.TEST_NAME, dataset_name))),
            "Dataset '{}' was not created in pipeline test".format(
                dataset_name))

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
        # Should probably look into ITK fuzzy matching methods
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

    @classmethod
    def get_session_dir(cls, project):
        return os.path.join(cls.ARCHIVE_PATH, project, cls.SUBJECT,
                            cls.SESSION)

    @classmethod
    def remove_generated_files(cls, project, study=None):
        # Remove processed datasets
        for fname in os.listdir(cls.get_session_dir(project)):
            if study is None or fname.startswith(study + '_'):
                os.remove(os.path.join(cls.get_session_dir(project), fname))


class DummyTestCase(PipelineTeseCase):

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
