from copy import copy
from arcana.utils.interfaces import (
    ZipDir, UnzipDir, TarGzDir, UnTarGzDir)
from .base import FileFormat, Converter, IdentityConverter  # @UnusedImport


class UnzipConverter(Converter):

    interface = UnzipDir()
    mem_gb = 12
    input = 'zipped'
    output = 'unzipped'


class ZipConverter(Converter):

    interface = ZipDir()
    mem_gb = 12
    input = 'dirname'
    output = 'zipped'


class TarGzConverter(Converter):

    interface = TarGzDir()
    mem_gb = 12
    input = 'dirname'
    output = 'zipped'


class UnTarGzConverter(Converter):

    interface = UnTarGzDir()
    mem_gb = 12
    input = 'gzipped'
    output = 'gunzipped'


# General formats
directory_format = FileFormat(name='directory', extension=None,
                              directory=True,
                              converters={'zip': UnzipConverter,
                                          'targz': UnTarGzConverter})
text_format = FileFormat(name='text', extension='.txt')
json_format = FileFormat(name='json', extension='.json')

# Compressed formats
zip_format = FileFormat(name='zip', extension='.zip',
                        converters={'directory': ZipConverter})
targz_format = FileFormat(name='targz', extension='.tar.gz',
                          converters={'direcctory': TarGzConverter})

# Register all data formats in module
for file_format in copy(globals()).values():
    if isinstance(file_format, FileFormat):
        FileFormat.register(file_format)
