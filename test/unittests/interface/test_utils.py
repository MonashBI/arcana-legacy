from builtins import next
import os.path
from arcana.utils.testing import BaseTestCase
from nipype.pipeline import engine as pe
from arcana.utils.interfaces import ZipDir, UnzipDir


class TestUtilsInterface(BaseTestCase):

    def setUp(self):
        self.reset_dirs()
        self.test_path = os.path.join(self.project_dir, 'test_dir')
        os.mkdir(self.test_path)
        with open(os.path.join(self.test_path, 'test_file'), 'w') as f:
            f.write('test')

    def test_zip_unzip(self):
        # Create Zip node
        zipnode = pe.Node(ZipDir(), name='zip')
        zipnode.inputs.dirname = self.test_path
        # Create Unzip node
        unzipnode = pe.Node(UnzipDir(), name='unzip')
        # Create workflow
        workflow = pe.Workflow('test_zip', base_dir=self.work_dir)
        workflow.connect(zipnode, 'zipped', unzipnode, 'zipped')
        exc_graph = workflow.run()
        unzip_results = next(n for n in exc_graph.nodes()
                             if n.name == 'unzip').result
        self.assertEqual(
            os.listdir(unzip_results.outputs.unzipped), ['test_file'])
