import os.path as op
from unittest import TestCase  # @IgnorePep8
from arcana.repository import Tree
from arcana.repository.bids import BidsRepository
from arcana.utils.testing import BaseTestCase


class TestBids(TestCase):

    test_dataset = op.join(BaseTestCase.test_data_dir, 'reference', 'bids',
                            'ds000114')

    def ref_tree(self):
        return Tree.construct()

    def test_project_info(self):
        repo = BidsRepository(self.test_dataset)
        tree = repo.tree
        self.assertEqual(
            tree, self.ref_tree(),
            "Generated project doesn't match reference:{}"
            .format(tree.find_mismatch(self.local_tree)))
