import os.path
import subprocess as sp
import shutil
from unittest import TestCase
import errno
import sys
import warnings
import nianalysis
from nianalysis.archive.local import (
    LocalArchive, SUMMARY_NAME)
from nianalysis.archive.xnat import download_all_datasets
from nianalysis.exceptions import NiAnalysisError
from nianalysis.nodes import NiAnalysisNodeMixin  # @IgnorePep8
from nianalysis.exceptions import NiAnalysisModulesNotInstalledException  # @IgnorePep8



test_data_dir = os.path.join(os.path.dirname(__file__), '..', 'test', '_data')

unittest_base_dir = os.path.abspath(os.path.join(
    os.path.dirname(nianalysis.__file__), '..', 'test', 'unittests'))


class BaseTestCase(TestCase):

    ARCHIVE_PATH = os.path.join(test_data_dir, 'archive')
    WORK_PATH = os.path.join(test_data_dir, 'work')
    CACHE_BASE_PATH = os.path.join(test_data_dir, 'cache')
    SUBJECT = 'SUBJECT'
    SESSION = 'SESSION'
    SERVER = 'https://mbi-xnat.erc.monash.edu.au'
    XNAT_TEST_PROJECT = 'TEST001'

    def setUp(self):
        self.reset_dirs()
        self.add_session(self.project_dir, self.SUBJECT, self.SESSION)

    def add_session(self, project_dir, subject, session,
                    required_datasets=None):
        session_dir = os.path.join(project_dir, subject, session)
        os.makedirs(session_dir)
        try:
            download_all_datasets(
                self.cache_dir, self.SERVER, self.xnat_session_name,
                overwrite=False)
        except Exception as e:
            warnings.warn(
                "Could not download datasets from '{}_{}' session on MBI-XNAT,"
                " attempting with what has already been downloaded:\n\n{}"
                .format(self.XNAT_TEST_PROJECT, self.name, e))
        for f in os.listdir(self.cache_dir):
            if required_datasets is None or f in required_datasets:
                src_path = os.path.join(self.cache_dir, f)
                dst_path = os.path.join(session_dir, f)
                if os.path.isdir(src_path):
                    shutil.copytree(src_path, dst_path)
                elif os.path.isfile(src_path):
                    shutil.copy(src_path, dst_path)
                else:
                    assert False

    def delete_project(self, project_dir):
        # Clean out any existing archive files
        shutil.rmtree(project_dir, ignore_errors=True)

    def reset_dirs(self):
        shutil.rmtree(self.project_dir, ignore_errors=True)
        shutil.rmtree(self.work_dir, ignore_errors=True)
        self.create_dirs()

    def create_dirs(self):
        for d in (self.project_dir, self.work_dir, self.cache_dir):
            if not os.path.exists(d):
                os.makedirs(d)

    @property
    def xnat_session_name(self):
        return '{}_{}'.format(self.XNAT_TEST_PROJECT, self.name)

    @property
    def session_dir(self):
        return self.get_session_dir(self.name, self.SUBJECT, self.SESSION)

    @property
    def cache_dir(self):
        return os.path.join(self.CACHE_BASE_PATH, self.name)

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
        rel_module_path = module_path[(len(unittest_base_dir) + 1):]
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

    def assertDatasetCreated(self, dataset_name, study_name, subject=None,
                             session=None, multiplicity='per_session'):
        output_dir = self.get_session_dir(
            self.project_dir, subject, session, multiplicity)
        out_path = self.output_file_path(
            dataset_name, study_name, subject, session, multiplicity)
        self.assertTrue(
            os.path.exists(out_path),
            ("Dataset '{}' (expected at '{}') was not created by unittest"
             " ('{}' found in '{}' instead)".format(
                 dataset_name, out_path, "', '".join(os.listdir(output_dir)),
                 output_dir)))

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

    def assertStatEqual(self, stat, dataset_name, target, study_name,
                        subject=None, session=None,
                        multiplicity='per_session'):
            try:
                NiAnalysisNodeMixin.load_module('mrtrix')
            except NiAnalysisModulesNotInstalledException:
                pass
            val = float(sp.check_output(
                'mrstats {} -output {}'.format(
                    self.output_file_path(
                        dataset_name, study_name,
                        subject=subject, session=session,
                        multiplicity=multiplicity),
                    stat),
                shell=True))
            self.assertEqual(
                val, target, (
                    "{} value of '{}' ({}) does not equal target ({}) "
                    "for subject {} visit {}"
                    .format(stat, dataset_name, val, target,
                            subject, session)))

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

    def get_session_dir(self, project=None, subject=None, session=None,
                        multiplicity='per_session'):
        if project is None:
            project = self.name
        if subject is None and multiplicity in ('per_session', 'per_subject'):
            subject = self.SUBJECT
        if session is None and multiplicity in ('per_session', 'per_visit'):
            session = self.SESSION
        if multiplicity == 'per_session':
            assert subject is not None
            assert session is not None
            path = os.path.join(self.ARCHIVE_PATH, project, subject,
                                session)
        elif multiplicity == 'per_subject':
            assert subject is not None
            assert session is None
            path = os.path.join(self.ARCHIVE_PATH, project, subject,
                                SUMMARY_NAME)
        elif multiplicity == 'per_visit':
            assert session is not None
            assert subject is None
            path = os.path.join(self.ARCHIVE_PATH, project, SUMMARY_NAME,
                                session)
        elif multiplicity == 'per_project':
            assert subject is None
            assert session is None
            path = os.path.join(self.ARCHIVE_PATH, project, SUMMARY_NAME,
                                SUMMARY_NAME)
        else:
            assert False
        return os.path.abspath(path)

    @classmethod
    def remove_generated_files(cls, project, study=None):
        # Remove processed datasets
        for fname in os.listdir(cls.get_session_dir(project)):
            if study is None or fname.startswith(study + '_'):
                os.remove(os.path.join(cls.get_session_dir(project), fname))

    def output_file_path(self, fname, study_name, subject=None, session=None,
                         multiplicity='per_session'):
        return os.path.join(
            self.get_session_dir(subject=subject, session=session,
                                 multiplicity=multiplicity),
            '{}_{}'.format(study_name, fname))

    def ref_file_path(self, fname, subject=None, session=None):
        return os.path.join(self.session_dir, fname,
                            subject=subject, session=session)


class BaseMultiSubjectTestCase(BaseTestCase):

    def setUp(self):
        self.reset_dirs()
        self.add_sessions(self.project_dir)

    def add_sessions(self, project_dir, required_datasets=None):
        try:
            download_all_datasets(
                self.cache_dir, self.SERVER,
                '{}_{}'.format(self.XNAT_TEST_PROJECT, self.name),
                overwrite=False)
        except Exception as e:
            warnings.warn(
                "Could not download datasets from '{}_{}' session on MBI-XNAT,"
                " attempting with what has already been downloaded:\n\n{}"
                .format(self.XNAT_TEST_PROJECT, self.name, e))
        for fname in os.listdir(self.cache_dir):
            parts = fname.split('_')
            if len(parts) < 3:
                raise NiAnalysisError(
                    "'{}' in multi-subject test session '{}' needs to be "
                    "prepended with subject and session IDs (delimited by '_')"
                    .format(fname, self.xnat_session_name))
            subject, session = parts[:2]
            dataset = '_'.join(parts[2:])
            if required_datasets is None or dataset in required_datasets:
                session_dir = os.path.join(project_dir, subject, session)
                try:
                    os.makedirs(session_dir)
                except OSError as e:
                    if e.errno != errno.EEXIST:
                        raise
                src_path = os.path.join(self.cache_dir, fname)
                dst_path = os.path.join(session_dir, dataset)
                if os.path.isdir(src_path):
                    shutil.copytree(src_path, dst_path)
                elif os.path.isfile(src_path):
                    shutil.copy(src_path, dst_path)
                else:
                    assert False

    def session_dir(self, subject, session):
        return self.get_session_dir(self.name, subject, session)


class DummyTestCase(BaseTestCase):

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
