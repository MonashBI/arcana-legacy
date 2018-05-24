import os.path
from arcana.exception import ArcanaError
import re


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


class NoContextWrapper(object):
    """
    Wraps an object, passing all calls through to the wrapped object
    except the __enter__ and __exit__ method, which do nothing. Used
    in cases where you want to use a file|connection handle within a
    "with" statement, except when it passed to the method from the
    calling code (presumably nested in another "with" statement).
    """

    def __init__(self, to_wrap):
        self._to_wrap = to_wrap

    def __getattr__(self, name):
        return getattr(self._to_wrap, name)

    def __enter__(self, *args, **kwargs):  # @UnusedVariable
        return self

    def __exit__(self, *args, **kwargs):
        pass
