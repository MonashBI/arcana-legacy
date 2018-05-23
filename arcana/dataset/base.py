import os.path
from abc import ABCMeta
from arcana.data_format import DataFormat
from copy import copy
from logging import getLogger
from arcana.exception import ArcanaError

logger = getLogger('Arcana')


class BaseDatum(object):

    MULTIPLICITY_OPTIONS = ('per_session', 'per_subject', 'per_visit',
                            'per_project')

    __metaclass__ = ABCMeta

    def __init__(self, name, frequency='per_session'):  # @ReservedAssignment @IgnorePep8
        assert name is None or isinstance(name, basestring)
        assert frequency in self.MULTIPLICITY_OPTIONS
        self._name = name
        self._frequency = frequency

    def __eq__(self, other):
        try:
            return (self.name == other.name and
                    self.frequency == other.frequency)
        except AttributeError as e:
            assert not e.message.startswith(
                "'{}'".format(self.__class__.__name__))
            return False

    def find_mismatch(self, other, indent=''):
        if self != other:
            mismatch = "\n{}{t}('{}') != {t}('{}')".format(
                indent, self.name, other.name,
                t=type(self).__name__)
        else:
            mismatch = ''
        sub_indent = indent + '  '
        if self.name != other.name:
            mismatch += ('\n{}name: self={} v other={}'
                         .format(sub_indent, self.name, other.name))
        if self.frequency != other.frequency:
            mismatch += ('\n{}frequency: self={} v other={}'
                         .format(sub_indent, self.frequency,
                                 other.frequency))
        return mismatch

    def __lt__(self, other):
        return self.name < other.name

    def __ne__(self, other):
        return not (self == other)

    def __iter__(self):
        return iter(self.to_tuple())

    @property
    def name(self):
        return self._name

    @property
    def frequency(self):
        return self._frequency

    def renamed(self, name):
        """
        Duplicate the datum and rename it
        """
        duplicate = copy(self)
        duplicate._name = name
        return duplicate

    def initkwargs(self):
        return {'name': self.name,
                'frequency': self.frequency}


class BaseDataset(BaseDatum):
    """
    An abstract base class representing either an acquired dataset or the
    specification for a derived dataset.

    Parameters
    ----------
    name : str
        The name of the dataset
    format : FileFormat
        The file format used to store the dataset. Can be one of the
        recognised formats
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_project',
        specifying whether the dataset is present for each session, subject,
        visit or project.
    """

    __metaclass__ = ABCMeta

    def __init__(self, name, format=None, frequency='per_session'):  # @ReservedAssignment @IgnorePep8
        super(BaseDataset, self).__init__(name=name, frequency=frequency)
        assert format is None or isinstance(format, DataFormat)
        self._format = format

    def __eq__(self, other):
        return (super(BaseDataset, self).__eq__(other) and
                self._format == other._format)

    def find_mismatch(self, other, indent=''):
        mismatch = super(BaseDataset, self).find_mismatch(other, indent)
        sub_indent = indent + '  '
        if self.format != other.format:
            mismatch += ('\n{}format: self={} v other={}'
                         .format(sub_indent, self.format,
                                 other.format))
        return mismatch

    @property
    def format(self):
        return self._format

    def to_tuple(self):
        return (self.name, self.format.name, self.frequency, self.derived,
                self.is_spec)

    def match(self, filename):
        base, ext = os.path.splitext(filename)
        return base == self.name and (ext == self.format.extension or
                                      self.format is None)

    def __repr__(self):
        return ("{}(name='{}', format={}, frequency={})"
                .format(self.__class__.__name__, self.name, self.format,
                        self.frequency))

    def initkwargs(self):
        dct = super(BaseDataset, self).initkwargs()
        dct['format'] = self.format
        return dct

    def fname(self, **kwargs):
        return self.basename(**kwargs) + self.format.ext_str


class BaseField(BaseDatum):
    """
    An abstract base class representing either an acquired value or the
    specification for a derived value.

    Parameters
    ----------
    name : str
        The name of the dataset
    dtype : type
        The datatype of the value. Can be one of (float, int, str)
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_project',
        specifying whether the dataset is present for each session, subject,
        visit or project.
    """

    __metaclass__ = ABCMeta

    dtypes = (int, float, str)

    def __init__(self, name, dtype, frequency):
        super(BaseField, self).__init__(name, frequency)
        if dtype not in self.dtypes:
            raise ArcanaError(
                "Invalid dtype {}, can be one of {}".format(
                    dtype, ', '.join(self._dtype_names())))
        self._dtype = dtype

    def __eq__(self, other):
        return (super(BaseField, self).__eq__(other) and
                self.dtype == other.dtype)

    def find_mismatch(self, other, indent=''):
        mismatch = super(BaseField, self).find_mismatch(other, indent)
        sub_indent = indent + '  '
        if self.dtype != other.dtype:
            mismatch += ('\n{}dtype: self={} v other={}'
                         .format(sub_indent, self.dtype,
                                 other.dtype))
        return mismatch

    @property
    def dtype(self):
        return self._dtype

    @classmethod
    def _dtype_names(cls):
        return (d.__name__ for d in cls.dtypes)

    def initkwargs(self):
        dct = super(BaseField, self).initkwargs()
        dct['dtype'] = self.dtype
        return dct
