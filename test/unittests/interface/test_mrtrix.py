import os.path
from nianalysis.testing import BaseTestCase
import logging
from nianalysis.nodes import Node
from nianalysis.interfaces.mrtrix import MRCalc
from nianalysis.requirements import mrtrix3rc_req

logger = logging.getLogger('NiAnalysis')


class TestMRCalcInterface(BaseTestCase):

    def test_subtract(self):
        # Create Zip node
        mrcalc = Node(MRCalc(), name='mrcalc',
                      requirements=[mrtrix3rc_req])
        mrcalc.inputs.operands = [os.path.join(self.session_dir, 'threes.mif'),
                                  os.path.join(self.session_dir, 'ones.mif')]
        mrcalc.inputs.operation = 'subtract'
        # Create workflow
        print os.getcwd()
        mrcalc.run()
        self.assertTrue(os.path.exists(os.path.join(
            mrcalc.output_dir(), 'threes_ones_subtract.mif')))
