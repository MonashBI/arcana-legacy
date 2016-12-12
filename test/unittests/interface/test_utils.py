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
        self.work_dir = os.path.join(self.tmpdir, 'work')
        os.mkdir(self.test_path)
        os.mkdir(self.work_dir)
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
        workflow = pe.Workflow('test_zip', base_dir=self.work_dir)
        workflow.connect(zipnode, 'zipped', unzipnode, 'zipped')
        workflow.connect(zipnode, 'extension', unzipnode, 'extension')
        exc_graph = workflow.run()
        unzip_results = next(n for n in exc_graph.nodes()
                             if n.name == 'unzip').result
        self.assertEqual(
            os.listdir(unzip_results.outputs.unzipped), ['test_file'])
