import os.path as op
import os
import subprocess as sp
import operator
import shutil
from unittest import TestCase
from functools import reduce
import errno
import sys
import json
import filecmp
from copy import deepcopy
import logging
import arcana
from arcana.dataset import Dataset
from arcana.utils import classproperty
from arcana.archive.local import (
    LocalArchive, SUMMARY_NAME)
from arcana.runner import LinearRunner
from arcana.exception import ArcanaError
from arcana.node import ArcanaNodeMixin
from arcana.exception import (
    ArcanaModulesNotInstalledException)
from arcana.archive.local import (
    SUMMARY_NAME as LOCAL_SUMMARY_NAME, FIELDS_FNAME)
from nipype.interfaces.base import (
    traits, TraitedSpec, BaseInterface, isdefined)

logger = logging.getLogger('Arcana')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

logging.getLogger("urllib3").setLevel(logging.WARNING)


class BaseTestCase(TestCase):

    SUBJECT = 'SUBJECT'
    VISIT = 'VISIT'

    # Whether to copy reference datasets from reference directory
    INPUTS_FROM_REF_DIR = False

    # The path to the test directory, which should sit along side the
    # the package directory. Note this will not work when Arcana
    # is installed by a package manager.
    BASE_TEST_DIR = op.abspath(op.join(
        op.dirname(arcana.__file__), '..', 'test'))

    @classproperty
    @classmethod
    def test_data_dir(cls):
        return op.join(cls.BASE_TEST_DIR, 'data')

    @classproperty
    @classmethod
    def unittest_root(cls):
        return op.join(cls.BASE_TEST_DIR, 'unittests')

    @classproperty
    @classmethod
    def archive_path(cls):
        return op.join(cls.test_data_dir, 'archive')

    @classproperty
    @classmethod
    def work_path(cls):
        return op.join(cls.test_data_dir, 'work')

    @classproperty
    @classmethod
    def cache_path(cls):
        return op.join(cls.test_data_dir, 'cache')

    def setUp(self):
        self.reset_dirs()
        if self.INPUTS_FROM_REF_DIR:
            datasets = {}
            for fname in os.listdir(self.ref_dir):
                if fname.startswith('.'):
                    continue
                dataset = Dataset.from_path(op.join(self.ref_dir,
                                                    fname))
                datasets[dataset.name] = dataset
        else:
            datasets = getattr(self, 'INPUT_DATASETS', None)
        self.add_session(datasets=datasets,
                         fields=getattr(self, 'INPUT_FIELDS', None))

    def add_session(self, datasets=None, fields=None, project_dir=None,
                    subject=None, visit=None):
        if project_dir is None:
            project_dir = self.project_dir
        if datasets is None:
            datasets = {}
        if subject is None:
            subject = self.SUBJECT
        if visit is None:
            visit = self.VISIT
        session_dir = op.join(project_dir, subject, visit)
        os.makedirs(session_dir)
        for name, dataset in datasets.items():
            if isinstance(dataset, Dataset):
                dst_path = op.join(session_dir,
                                   name + dataset.format.ext_str)
                if dataset.format.directory:
                    shutil.copytree(dataset.path, dst_path)
                else:
                    shutil.copy(dataset.path, dst_path)
            elif isinstance(dataset, basestring):
                # Write string as text file
                with open(op.join(session_dir,
                                  name + '.txt'), 'w') as f:
                    f.write(dataset)
            else:
                raise ArcanaError(
                    "Unrecognised dataset ({}) in {} test setup. Can "
                    "be either a Dataset or basestring object"
                    .format(dataset, self))
        if fields is not None:
            with open(op.join(session_dir,
                                   FIELDS_FNAME), 'w') as f:
                json.dump(fields, f)

    def delete_project(self, project_dir):
        # Clean out any existing archive files
        shutil.rmtree(project_dir, ignore_errors=True)

    def reset_dirs(self):
        shutil.rmtree(self.project_dir, ignore_errors=True)
        shutil.rmtree(self.work_dir, ignore_errors=True)
        self.create_dirs()

    def create_dirs(self):
        for d in (self.project_dir, self.work_dir):
            if not op.exists(d):
                os.makedirs(d)

    @property
    def archive_tree(self):
        return self.archive.get_tree()

    @property
    def xnat_session_name(self):
        return '{}_{}'.format(self.XNAT_TEST_PROJECT, self.name)

    @property
    def session_dir(self):
        return self.get_session_dir(self.SUBJECT, self.VISIT)

    @property
    def session(self):
        return self.archive_tree.subject(
            self.SUBJECT).session(self.VISIT)

    @property
    def archive(self):
        return self.local_archive

    @property
    def local_archive(self):
        try:
            return self._local_archive
        except AttributeError:
            self._local_archive = LocalArchive(self.project_dir)
            return self._local_archive

    @property
    def runner(self):
        return LinearRunner(self.work_dir)

    @property
    def project_dir(self):
        return op.join(self.archive_path, self.name)

    @property
    def work_dir(self):
        return op.join(self.work_path, self.name)

    @property
    def cache_dir(self):
        return op.join(self.cache_path, self.name)

    @property
    def ref_dir(self):
        return op.join(self.test_data_dir, 'reference', self.name)

    @property
    def name(self):
        return self._get_name(type(self))

    @property
    def project_id(self):
        return self.name  # To allow override in deriving classes

    def _get_name(self, cls):
        """
        Get unique name for test class from module path and its class name to
        be used for storing test data on XNAT and creating unique work/project
        dirs
        """
        module_path = op.abspath(sys.modules[cls.__module__].__file__)
        rel_module_path = module_path[(len(self.unittest_root) + 1):]
        path_parts = rel_module_path.split(op.sep)
        module_name = (''.join(path_parts[:-1]) +
                       op.splitext(path_parts[-1])[0][5:]).upper()
        test_class_name = cls.__name__[4:].upper()
        return module_name + '_' + test_class_name

    def create_study(self, study_cls, name, inputs, archive=None,
                     runner=None, **kwargs):
        """
        Creates a study using default archive and runners.

        Parameters
        ----------
        study_cls : Study
            The class to initialise
        name : str
            Name of the study
        inputs : List[BaseSpec]
            List of inputs to the study
        archive : BaseArchive | None
            The archive to use (a default local archive is used if one
            isn't provided
        runner : Runner | None
            The runner to use (a default LinearRunner is used if one
            isn't provided
        """
        if archive is None:
            archive = self.archive
        if runner is None:
            runner = self.runner
        return study_cls(
            name=name,
            archive=archive,
            runner=runner,
            inputs=inputs,
            **kwargs)

    def assertDatasetCreated(self, dataset_name, study_name, subject=None,
                             visit=None, frequency='per_session'):
        output_dir = self.get_session_dir(subject, visit, frequency)
        out_path = self.output_file_path(
            dataset_name, study_name, subject, visit, frequency)
        self.assertTrue(
            op.exists(out_path),
            ("Dataset '{}' (expected at '{}') was not created by unittest"
             " ('{}' found in '{}' instead)".format(
                 dataset_name, out_path, "', '".join(os.listdir(output_dir)),
                 output_dir)))

    def assertContentsEqual(self, dataset, reference, context=None):
        with open(dataset.path) as f:
            contents = f.read()
        msg = ("Contents of {} ({}) do not match reference ({})"
               .format(dataset, contents, reference))
        if context is not None:
            msg += 'for ' + context
        self.assertEqual(contents, str(reference), msg)

    def assertCreated(self, dataset):
        self.assertTrue(
            os.path.exists(dataset.path),
            "{} was not created".format(dataset))

    def assertField(self, name, ref_value, study_name, subject=None,
                    visit=None, frequency='per_session',
                    to_places=None):
        esc_name = study_name + '_' + name
        output_dir = self.get_session_dir(subject, visit, frequency)
        try:
            with open(op.join(output_dir, FIELDS_FNAME)) as f:
                fields = json.load(f)
        except OSError as e:
            if e.errno == errno.ENOENT:
                raise ArcanaError(
                    "No fields were created by pipeline in study '{}'"
                    .format(study_name))
        try:
            value = fields[esc_name]
        except KeyError:
            raise ArcanaError(
                "Field '{}' was not created by pipeline in study '{}'. "
                "Created fields were ('{}')"
                .format(esc_name, study_name, "', '".join(fields)))
        msg = ("Field value '{}' for study '{}', {}, does not match "
               "reference value ({})".format(name, study_name, value,
                                             ref_value))
        if to_places is not None:
            self.assertAlmostEqual(
                value, ref_value, to_places,
                '{} to {} decimal places'.format(msg, to_places))
        else:
            self.assertEqual(value, ref_value, msg)

    def assertDatasetsEqual(self, dataset1, dataset2, error_msg=None):
        msg = "{} does not match {}".format(dataset1, dataset2)
        if msg is not None:
            msg += ':\n' + error_msg
        self.assertTrue(filecmp.cmp(dataset1.path, dataset2.path,
                                    shallow=False), msg=msg)

    def assertStatEqual(self, stat, dataset_name, target, study_name,
                        subject=None, visit=None,
                        frequency='per_session'):
            try:
                ArcanaNodeMixin.load_module('mrtrix')
            except ArcanaModulesNotInstalledException:
                pass
            val = float(sp.check_output(
                'mrstats {} -output {}'.format(
                    self.output_file_path(
                        dataset_name, study_name,
                        subject=subject, visit=visit,
                        frequency=frequency),
                    stat),
                shell=True))
            self.assertEqual(
                val, target, (
                    "{} value of '{}' ({}) does not equal target ({}) "
                    "for subject {} visit {}"
                    .format(stat, dataset_name, val, target,
                            subject, visit)))

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

    def get_session_dir(self, subject=None, visit=None,
                        frequency='per_session'):
        if subject is None and frequency in ('per_session', 'per_subject'):
            subject = self.SUBJECT
        if visit is None and frequency in ('per_session', 'per_visit'):
            visit = self.VISIT
        if frequency == 'per_session':
            assert subject is not None
            assert visit is not None
            path = op.join(self.project_dir, subject, visit)
        elif frequency == 'per_subject':
            assert subject is not None
            assert visit is None
            path = op.join(
                self.project_dir, subject, SUMMARY_NAME)
        elif frequency == 'per_visit':
            assert visit is not None
            assert subject is None
            path = op.join(self.project_dir, SUMMARY_NAME, visit)
        elif frequency == 'per_project':
            assert subject is None
            assert visit is None
            path = op.join(self.project_dir, SUMMARY_NAME, SUMMARY_NAME)
        else:
            assert False
        return op.abspath(path)

    @classmethod
    def remove_generated_files(cls, study=None):
        # Remove derived datasets
        for fname in os.listdir(cls.get_session_dir()):
            if study is None or fname.startswith(study + '_'):
                os.remove(op.join(cls.get_session_dir(), fname))

    def output_file_path(self, fname, study_name, subject=None, visit=None,
                         frequency='per_session', **kwargs):
        return op.join(
            self.get_session_dir(subject=subject, visit=visit,
                                 frequency=frequency, **kwargs),
            '{}_{}'.format(study_name, fname))

    def ref_file_path(self, fname, subject=None, session=None):
        return op.join(self.session_dir, fname,
                            subject=subject, session=session)


class BaseMultiSubjectTestCase(BaseTestCase):

    SUMMARY_NAME = LOCAL_SUMMARY_NAME

    def setUp(self):
        self.reset_dirs()
        self.add_sessions(self.project_dir)

    def add_sessions(self, project_dir):
        self.local_tree = deepcopy(self.input_tree)
        if project_dir is not None:  # For local archive
            proj_summ_path = op.join(project_dir, SUMMARY_NAME,
                                     SUMMARY_NAME)
            for dataset in self.local_tree.datasets:
                self.init_dataset(dataset, op.join(proj_summ_path,
                                                   dataset.fname()))
            self.init_fields(proj_summ_path, self.local_tree.fields)
            for visit in self.local_tree.visits:
                visit_summ_path = op.join(project_dir, SUMMARY_NAME,
                                          visit.id)
                for dataset in visit.datasets:
                    self.init_dataset(dataset, op.join(visit_summ_path,
                                                       dataset.fname()))
                self.init_fields(visit_summ_path, visit.fields)
            for subject in self.local_tree.subjects:
                subj_summ_path = op.join(project_dir, subject.id,
                                         SUMMARY_NAME)
                for dataset in subject.datasets:
                    self.init_dataset(dataset, op.join(subj_summ_path,
                                                       dataset.fname()))
                self.init_fields(subj_summ_path, subject.fields)
                for session in subject.sessions:
                    sess_path = op.join(project_dir, session.subject_id,
                                        session.visit_id)
                    for dataset in session.datasets:
                        self.init_dataset(
                            dataset, op.join(sess_path,
                                             dataset.fname()))
                    self.init_fields(sess_path, session.fields)

    @property
    def subject_ids(self):
        return (d for d in os.listdir(self.project_dir)
                if d != self.SUMMARY_NAME)

    def visit_ids(self, subject_id):
        subject_dir = op.join(self.project_dir, subject_id)
        return (d for d in os.listdir(subject_dir)
                if d != self.SUMMARY_NAME)

    def session_dir(self, subject, visit):
        return self.get_session_dir(subject, visit)

    def get_session_dir(self, subject, visit, frequency='per_session'):
        return super(BaseMultiSubjectTestCase, self).get_session_dir(
            subject=subject, visit=visit, frequency=frequency)

    def init_dataset(self, dataset, path):
        dataset._path = path
        dataset._archive = self.archive
        self._make_dir(op.dirname(dataset.path))
        with open(dataset.path, 'w') as f:
            f.write(str(self.DATASET_CONTENTS[dataset.name]))

    def init_fields(self, dpath, fields):
        self._make_dir(dpath)
        dct = {f.name: f.value for f in fields}
        with open(op.join(dpath, FIELDS_FNAME), 'w') as f:
            json.dump(dct, f)

    def _make_dir(self, path):
        try:
            os.makedirs(path)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        return path


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


class TestTestCase(BaseTestCase):

    def test_test(self):
        pass


class TestMathInputSpec(TraitedSpec):

    x = traits.Either(traits.Float(), traits.File(exists=True),
                      traits.List(traits.Float),
                      traits.List(traits.File(exists=True)),
                      mandatory=True, desc='first arg')
    y = traits.Either(traits.Float(), traits.File(exists=True),
                      mandatory=False, desc='second arg')
    op = traits.Str(mandatory=True, desc='operation')

    z = traits.File(genfile=True, mandatory=False,
                    desc="Name for output file")

    as_file = traits.Bool(False, desc="Whether to write as a file",
                          usedefault=True)


class TestMathOutputSpec(TraitedSpec):

    z = traits.Either(traits.Float(), traits.File(exists=True),
                      'output')


class TestMath(BaseInterface):
    """
    A basic interface to test out the pipeline infrastructure
    """

    input_spec = TestMathInputSpec
    output_spec = TestMathOutputSpec

    def _run_interface(self, runtime):
        return runtime

    def _list_outputs(self):
        x = self.inputs.x
        y = self.inputs.y
        if isinstance(x, basestring):
            x = self._load_file(x)
        if isinstance(y, basestring):
            y = self._load_file(y)
        oper = getattr(operator, self.inputs.op)
        if isdefined(y):
            z = oper(x, y)
        elif isinstance(x, list):
            if isinstance(x[0], basestring):
                x = [self._load_file(u) for u in x]
            z = reduce(oper, x)
        else:
            raise Exception(
                "If 'y' is not provided then x needs to be list")
        outputs = self.output_spec().get()
        if self.inputs.as_file:
            z_path = op.abspath(self._gen_z_fname())
            with open(z_path, 'w') as f:
                f.write(str(z))
            outputs['z'] = z_path
        else:
            outputs['z'] = z
        return outputs

    def _gen_filename(self, name):
        if name == 'z':
            fname = self._gen_z_fname()
        else:
            assert False
        return fname

    def _gen_z_fname(self):
        if isdefined(self.inputs.z):
            fname = self.inputs.z
        else:
            fname = 'z.txt'
        return fname

    @classmethod
    def _load_file(self, path):
        with open(path) as f:
            return float(f.read())
