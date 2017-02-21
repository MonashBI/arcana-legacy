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
        self.delete_project(self.project_dir)
        self.add_session(self.project_dir, self.SUBJECT, self.SESSION)

    def add_session(self, project_dir, subject, session,
                    required_datasets=None):
        session_dir = os.path.join(project_dir, subject, session)
        os.makedirs(session_dir)
        cache_dir = os.path.join(self.CACHE_BASE_PATH, session)
        download_all_datasets(
            cache_dir, self.SERVER, self.USER, self.PASSWORD,
            '{}_{}'.format(self.XNAT_TEST_PROJECT, self.name), overwrite=False)
        for f in os.listdir(cache_dir):
            if required_datasets is None or f in required_datasets:
                shutil.copy(os.path.join(cache_dir, f),
                            os.path.join(session_dir, f))

    def delete_project(self, project_dir):
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
        """
        Get unique name for test class from module path and its class name to
        be used for storing test data on XNAT and creating unique work/project
        dirs
        """
        module_path = os.path.abspath(sys.modules[self.__module__].__file__)
        rel_module_path = module_path[len(unittest_base_dir) + 1:]
        path_parts = rel_module_path.split(os.path.sep)
        module_name = (''.join(path_parts[:-1]) +
                       os.path.splitext(path_parts[-1])[0][5:]).upper()
        test_class_name = type(self).__name__[4:].upper()
        return module_name + '_' + test_class_name

    def create_study(self, study_cls, name, input_datasets):
        return study_cls(
            name=name,
            project_id=self.name,
            archive=self.archive,
            input_datasets=input_datasets)

    def assertDatasetCreated(self, dataset_name, study_name):
        self.assertTrue(os.path.exists(self.output_file_path(dataset_name,
                                                             study_name)),
                        "Dataset '{}' was not created in pipeline test".format(
                            dataset_name))

    def assertImagesMatch(self, output, ref, study_name):
        out_path = self.output_file_path(output, study_name)
        ref_path = self.ref_file_path(ref)
        try:
            sp.check_output('diff {}.nii {}.nii'
                            .format(out_path, ref_path), shell=True)
        except sp.CalledProcessError as e:
            if e.output == "Binary files {} and {} differ\n".format(
                    out_path, ref_path):
                self.assert_(
                    False,
                    "Images {} and {} do not match exactly".format(out_path,
                                                                   ref_path))
            else:
                raise

    def assertImagesAlmostMatch(self, out, ref, mean_threshold,
                                stdev_threshold, study_name):
        out_path = self.output_file_path(out, study_name)
        ref_path = self.ref_file_path(ref)
        # Should probably look into ITK fuzzy matching methods
        cmd = ("mrcalc -quiet {a} {b} -subtract - | mrstats - | "
               "grep -v channel | awk '{{print $4 \" \" $6}}'"
               .format(a=out_path, b=ref_path))
        out = sp.check_output(cmd, shell=True)
        mean, stdev = (float(x) for x in out.split())
        self.assert_(
            abs(mean) < mean_threshold and stdev < stdev_threshold,
            ("Mean ({mean}) or standard deviation ({stdev}) of difference "
             "between images {a} and {b} differ more than threshold(s) "
             "({thresh_mean} and {thresh_stdev} respectively)"
             .format(mean=mean, stdev=stdev, thresh_mean=mean_threshold,
                     thresh_stdev=stdev_threshold, a=out_path, b=ref_path)))

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

    def output_file_path(self, fname, study_name):
        return os.path.join(
            self.session_dir, '{}_{}'.format(study_name, fname))

    def ref_file_path(self, fname):
        return os.path.join(self.session_dir, fname)


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
