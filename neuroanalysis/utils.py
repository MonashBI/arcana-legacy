import os.path


def split_extension(filename):
    # FIXME: Handle 'nii.gz' extension properly
    return os.path.splitext(filename)
