
def split_extension(filename):
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
    parts = filename.split('.')
    if parts[-1] == 'gz' and parts[-2] in ('nii',):
        num_ext_parts = 2
    else:
        num_ext_parts = 1
    ext = '.'.join(parts[-num_ext_parts:])
    if ext:
        ext = '.' + ext
    base = '.'.join(parts[:-num_ext_parts])
    return base, ext


class classproperty(property):
    def __get__(self, cls, owner):
        return self.fget.__get__(None, owner)()
