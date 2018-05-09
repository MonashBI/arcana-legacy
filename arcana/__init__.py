from .version_ import __version__
import os
from .study import (
    Study, StudyMetaClass, MultiStudy, MultiStudyMetaClass,
    SubStudySpec)
from .dataset import (
    DatasetSpec, DatasetMatch, FieldSpec, FieldMatch)
from .data_format import DataFormat
from .option import Option, OptionSpec
from .runner import LinearRunner, MultiProcRunner, SlurmRunner
from .archive import LocalArchive, XnatArchive
# Should be set explicitly in all FSL interfaces, but this squashes the warning
os.environ['FSLOUTPUTTYPE'] = 'NIFTI_GZ'
