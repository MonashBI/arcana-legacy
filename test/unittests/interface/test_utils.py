import tempfile
import shutil
import os.path
from unittest import TestCase
import logging
from nipype.pipeline import engine as pe
from nianalysis.interfaces.utils import ZipDir, UnzipDir

logger = logging.getLogger('NiAnalysis')


class TestUtilsInterface(TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.test_path = os.path.join(self.tmpdir, 'test_dir')
        os.mkdir(self.test_path)
        with open(os.path.join(self.test_path, 'test_file'), 'w') as f:
            f.write('test')

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_zip_unzip(self):
        # Create Zip node
        zipnode = pe.Node(ZipDir(), name='zip')
        zipnode.inputs.dirname = self.test_path
        zipnode.inputs.extension = '.test'
        # Create Unzip node
        unzipnode = pe.Node(UnzipDir(), name='unzip')
        # Create workflow
        workflow = pe.Workflow('test_zip')
        workflow.connect(zipnode, 'zipped', unzipnode, 'zipped')
        workflow.connect(zipnode, 'extension', unzipnode, 'extension')
        workflow.run()
        self.assertEqual(
            os.listdir(unzipnode.get_output('unzipped')), ['test_file'])
