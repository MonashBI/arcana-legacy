import os.path
from arcana.exception import ArcanaError
import re


PATH_SUFFIX = '_path'
FIELD_SUFFIX = '_field'

package_dir = os.path.join(os.path.dirname(__file__), '..')


def is_regex(s):
    "Checks to see if string contains special characters"
    return bool(re.match(r'^\w+$', s))


def nth(i):
    "Returns 1st, 2nd, 3rd, 4th, etc for a given number"
    if i == 1:
        s = '1st'
    elif i == 2:
        s = '2nd'
    elif i == 3:
        s = '3rd'
    else:
        s = '{}th'.format(i)
    return s


def dir_modtime(dpath):
    """
    Returns the latest modification time of all files/subdirectories in a
    directory
    """
    return max(os.path.getmtime(d) for d, _, _ in os.walk(dpath))


def get_fsl_reference_path():
    return os.path.join(os.environ['FSLDIR'], 'data', 'standard')


def get_atlas_path(name, dataset='brain', resolution='1mm'):
    """
    Returns the path to the atlas (or atlas mask) in the arcana repository

    Parameters
    ----------
    name : str
        Name of the Atlas, can be one of ('mni_nl6')
    atlas_type : str
        Whether to return the brain mask or the full atlas, can be one of
        'image', 'mask'
    """
    if name == 'MNI152':
        # MNI ICBM 152 non-linear 6th Generation Symmetric Average Brain
        # Stereotaxic Registration Model (http://nist.mni.mcgill.ca/?p=858)
        if resolution not in ['0.5mm', '1mm', '2mm']:
            raise ArcanaError(
                "Invalid resolution for MNI152, '{}', can be one of '0.5mm', "
                "'1mm' or '2mm'".format(resolution))
        if dataset == 'image':
            path = os.path.join(get_fsl_reference_path(),
                                'MNI152_T1_{}.nii.gz'.format(resolution))
        elif dataset == 'mask':
            path = os.path.join(get_fsl_reference_path(),
                                'MNI152_T1_{}_brain_mask.nii.gz'
                                .format(resolution))
        elif dataset == 'mask_dilated':
            if resolution != '2mm':
                raise ArcanaError(
                    "Dilated MNI masks are not available for {} resolution "
                    .format(resolution))
            path = os.path.join(get_fsl_reference_path(),
                                'MNI152_T1_{}_brain_mask_dil.nii.gz'
                                .format(resolution))
        elif dataset == 'brain':
            path = os.path.join(get_fsl_reference_path(),
                                'MNI152_T1_{}_brain.nii.gz'
                                .format(resolution))
        else:
            raise ArcanaError("Unrecognised dataset '{}'"
                                  .format(dataset))
    else:
        raise ArcanaError("Unrecognised atlas name '{}'"
                              .format(name))
    return os.path.abspath(path)


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
