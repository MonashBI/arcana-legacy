from copy import deepcopy
from unittest import TestCase
from nianalysis.archive.base import ArchiveSinkInputSpec
import multiprocessing


def _deepcopy_input_spec():
    sink_input_spec = ArchiveSinkInputSpec()
    copy = deepcopy(sink_input_spec)
    return copy


class TestBaseArchive(TestCase):

    def test_deepcopy_recursion_loop(self, timeout=1):
        """
        Checks that the deepcopy of input_spec doesn't get caught in an
        infinite recursion
        """
        copy_process = multiprocessing.Process(target=_deepcopy_input_spec)
        try:
            copy_process.start()
            copy_process.join(timeout)
            self.assert_(copy_process.exitcode >= 0,
                         "Deepcopy of input sink timed-out")
        finally:
            copy_process.terminate()


if __name__ == '__main__':

    print(_deepcopy_input_spec())
