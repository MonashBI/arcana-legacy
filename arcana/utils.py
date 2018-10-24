from past.builtins import basestring
from future.utils import PY3, PY2
import os.path
import errno
from arcana.exception import ArcanaUsageError
if PY2:
    from contextlib2 import ExitStack  # @UnusedImport @UnresolvedImport
else:
    from contextlib import ExitStack  # @UnusedImport @Reimport


PATH_SUFFIX = '_path'
FIELD_SUFFIX = '_field'

package_dir = os.path.join(os.path.dirname(__file__), '..')


def dir_modtime(dpath):
    """
    Returns the latest modification time of all files/subdirectories in a
    directory
    """
    return max(os.path.getmtime(d) for d, _, _ in os.walk(dpath))


double_exts = ('.tar.gz', '.nii.gz')


def split_extension(path):
    """
    A extension splitter that checks for compound extensions such as
    'file.nii.gz'

    Parameters
    ----------
    filename : str
        A filename to split into base and extension

    Returns
    -------
    base : str
        The base part of the string, i.e. 'file' of 'file.nii.gz'
    ext : str
        The extension part of the string, i.e. 'nii.gz' of 'file.nii.gz'
    """
    for double_ext in double_exts:
        if path.endswith(double_ext):
            return path[:-len(double_ext)], double_ext
    dirname = os.path.dirname(path)
    filename = os.path.basename(path)
    parts = filename.split('.')
    if len(parts) == 1:
        base = filename
        ext = None
    else:
        ext = '.' + parts[-1]
        base = '.'.join(parts[:-1])
    return os.path.join(dirname, base), ext


class classproperty(property):
    def __get__(self, cls, owner):
        return self.fget.__get__(None, owner)()


def lower(s):
    if s is None:
        return None
    return s.lower()


# Implement makedirs with 'exist_ok' kwarg for Python 2
if PY2:
    def makedirs(path, exist_ok=False, **kwargs):
        try:
            os.makedirs(path, **kwargs)
        except OSError as e:
            if not (exist_ok and e.errno == errno.EEXIST):
                raise
else:
    from os import makedirs  # @UnusedImport


if PY3:
    JSON_ENCODING = {'encoding': 'utf-8'}
else:
    JSON_ENCODING = {}


def parse_single_value(value):
    """
    Tries to convert to int, float and then gives up and assumes the value
    is of type string. Useful when excepting values that may be string
    representations of numerical values
    """
    if isinstance(value, (int, float)):
        return value
    try:
        value = int(value)
    except ValueError:
        try:
            value = float(value)
        except ValueError:
            if isinstance(value, basestring):
                value = str(value)
            else:
                raise ArcanaUsageError(
                    "Unrecognised value type {}".format(value))
    return value


def parse_value(value):
    # Split strings with commas into lists
    if isinstance(value, basestring):
        if ',' in value:
            value = value.split(',')
    # Cast all iterables (except strings) into lists
    else:
        try:
            value = list(value)
        except TypeError:
            pass
    if isinstance(value, list):
        value = [parse_single_value(v) for v in value]
        # Check to see if datatypes are consistent
        dtypes = set(type(v) for v in value)
        if dtypes == set((float, int)):
            # If both ints and floats are presents, cast to floats
            value = [float(v) for v in value]
        elif len(dtypes) > 1:
            raise ArcanaUsageError(
                "Inconsistent datatypes in values array ({})"
                .format(value))
    else:
        value = parse_single_value(value)
    return value
