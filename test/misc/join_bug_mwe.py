from builtins import object
import nipype.pipeline.engine.tests.test_join
import os
import tempfile
tempdir = tempfile.mkdtemp()


class TempDir(object):

    def chdir(self):
        os.chdir(tempdir)

    @property
    def strpath(self):
        return tempdir


nipype.pipeline.engine.tests.test_join.test_name_prefix_join(TempDir())
