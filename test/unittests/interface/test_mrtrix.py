import os.path
from nianalysis.testing import BaseTestCase
import logging
import tempfile
from nianalysis.nodes import Node
from nianalysis.interfaces.mrtrix import MRCalc
from nianalysis.requirements import mrtrix3_req

logger = logging.getLogger('NiAnalysis')


class TestMRCalcInterface(BaseTestCase):

    def test_subtract(self):

        tmp_dir = tempfile.mkdtemp()
        out_file = os.path.join(tmp_dir, 'out_file.mif')
        mrcalc = Node(MRCalc(), name='mrcalc',
                      requirements=[mrtrix3_req])
        mrcalc.inputs.operands = [os.path.join(self.session_dir,
                                               'threes.mif'),
                                  os.path.join(self.session_dir,
                                               'ones.mif')]
        mrcalc.inputs.operation = 'subtract'
        mrcalc.inputs.out_file = out_file
        mrcalc.run()
        self.assertTrue(os.path.exists(out_file))
