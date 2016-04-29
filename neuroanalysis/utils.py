import shutil
import errno


def rmtree_ignore_missing(directory):
    try:
        shutil.rmtree(directory)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
