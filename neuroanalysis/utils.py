
def split_extension(filename):
    parts = filename.split('.')
    if parts[-1] == 'gz' and parts[-2] in ('nii',):
        num_ext_parts = 2
    else:
        num_ext_parts = 1
    ext = '.'.join(parts[-num_ext_parts:])
    base = '.'.join(parts[:-num_ext_parts])
    return base, ext
