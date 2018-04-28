from copy import copy
from nianalysis.exceptions import NiAnalysisUsageError


class OptionSpec(object):

    def __init__(self, name, default, choices=None, description=None):
        self._name = name
        if not isinstance(default, (int, float, basestring)):
            raise NiAnalysisUsageError(
                "Invalid type for '{}' option default ({}), {}, "
                "can be one of int, float or str"
                .format(name, default, type(default)))
        self._dtype = (
            str if isinstance(default, basestring) else type(default))
        self._default = default
        self._choices = tuple(choices) if choices is not None else None
        self._description = description

    @property
    def name(self):
        return self._name

    @property
    def default(self):
        return self._default

    @property
    def dtype(self):
        return self._dtype

    @property
    def choices(self):
        return self._choices

    @property
    def description(self):
        return self._description

    def renamed(self, name):
        """
        Duplicate the OptionSpec and rename it
        """
        duplicate = copy(self)
        duplicate._name = name
        return duplicate
