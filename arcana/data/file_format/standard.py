from copy import copy
from arcana.node import Node
from arcana.interfaces.utils import (
    ZipDir, UnzipDir, TarGzDir, UnTarGzDir)
from .base import FileFormat, Converter


class UnzipConverter(Converter):

    requirements = []

    def get_node(self, name, **kwargs):
        convert_node = Node(UnzipDir(), name=name, memory=12000, **kwargs)
        return convert_node, 'zipped', 'unzipped'


class ZipConverter(Converter):

    requirements = []

    def get_node(self, name, **kwargs):
        convert_node = Node(ZipDir(), name=name, memory=12000, **kwargs)
        return convert_node, 'dirname', 'zipped'


class TarGzConverter(Converter):

    requirements = []

    def get_node(self, name, **kwargs):
        convert_node = Node(TarGzDir(), name=name, memory=12000, **kwargs)
        return convert_node, 'dirname', 'zipped'


class UnTarGzConverter(Converter):

    requirements = []

    def get_node(self, name, **kwargs):
        convert_node = Node(UnTarGzDir(), name=name, memory=12000, **kwargs)
        return convert_node, 'gzipped', 'gunzipped'


# General formats
directory_format = FileFormat(name='directory', extension=None,
                              directory=True,
                              converters={'zip': UnzipConverter,
                                          'targz': UnTarGzConverter})
text_format = FileFormat(name='text', extension='.txt')


# Compressed formats
zip_format = FileFormat(name='zip', extension='.zip',
                        converters={'directory': ZipConverter})
targz_format = FileFormat(name='targz', extension='.tar.gz',
                          converters={'direcctory': TarGzConverter})

# Register all data formats in module
for file_format in copy(globals()).values():
    if isinstance(file_format, FileFormat):
        FileFormat.register(file_format)
