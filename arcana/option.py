from copy import copy
from arcana.exception import ArcanaUsageError


class Option(object):

    def __init__(self, name, value):
        self._name = name
        if value is None:
            self._dtype = None
        else:
            if not isinstance(value, (int, float, basestring,
                                      tuple, list)):
                raise ArcanaUsageError(
                    "Invalid type for '{}' option default ({}), {}, "
                    "can be one of int, float or str"
                    .format(name, value, type(value)))
            self._dtype = (
                str if isinstance(value, basestring) else type(value))
        self._value = value

    @property
    def name(self):
        return self._name

    @property
    def value(self):
        return self._value

    @property
    def dtype(self):
        if self._dtype is None:
            return type(None)
        return self._dtype

    def renamed(self, name):
        """
        Duplicate the Option and rename it
        """
        duplicate = copy(self)
        duplicate._name = name
        return duplicate

    def __repr__(self):
        return "Option(name='{}', value={})".format(self.name,
                                                    self.value)


class OptionSpec(Option):

    def __init__(self, name, default, choices=None, desc=None, dtype=None):
        super(OptionSpec, self).__init__(name, default)
        self._choices = tuple(choices) if choices is not None else None
        self._desc = desc
        if dtype is not None:
            if self.default is not None and not isinstance(self.default,
                                                           dtype):
                raise ArcanaUsageError(
                    "Provided default value ({}) does not match explicit "
                    "dtype ({})".format(self.default, dtype))
            self._dtype = dtype

    @property
    def name(self):
        return self._name

    @property
    def default(self):
        return self._value

    @property
    def choices(self):
        return self._choices

    @property
    def desc(self):
        return self._desc

    def __repr__(self):
        return ("OptionSpec(name='{}', value={}, desc='{}', "
                "choices={})".format(self.name, self.value,
                                     self.desc, self.choices))
