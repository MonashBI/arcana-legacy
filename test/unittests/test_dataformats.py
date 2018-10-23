from unittest import TestCase
from arcana.interfaces.utils import ZipDir
from arcana.data.file_format.standard import (directory_format, zip_format)


class TestConverterAvailability(TestCase):

    def test_find_converter(self):
        converter = zip_format.converter_from(directory_format)
        node, _, _ = converter.get_node('dummy')
        self.assertIsInstance(node.interface, ZipDir)
