import os.path
import errno
from unittest import TestCase
from mbi_pipelines.data_access.daris import DarisSession


class TestDarisSession(TestCase):

    def setUp(self):
        self._daris = DarisSession(user='test123', password='GaryEgan1',
                                   domain='mon-daris')
        self._daris.open()

    def tearDown(self):
        self._daris.close()

    def test_list_projects(self):
        projects = dict((p.id, p) for p in self._daris.list_projects())
        self.assertEqual(
            len(projects), 4,
            "'test123' account only has access to 4 projects")
        self.assertEqual(projects[4].name, 'Barnes_Test_Area_01')


class TestDarisToken(TestCase):

    token_path = os.path.join(os.path.dirname(__file__), 'test_daris_token')

    def tearDown(self):
        # Remove token_path if present
        try:
            os.remove(self.token_path)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise

    def test_create_token_and_login(self):
        DarisSession(user='test123', password='GaryEgan1', domain='mon-daris',
                     token_path=self.token_path, app_name='unittest').open()
        with DarisSession(token_path=self.token_path,
                          app_name='unittest') as daris:
            self.assertTrue(len(daris.list_projects))
