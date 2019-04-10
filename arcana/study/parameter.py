from past.builtins import basestring
from builtins import object
from copy import copy
from arcana.exceptions import ArcanaUsageError


class Parameter(object):
    """
    Represents a parameter passed to a Study object

    Parameters
    ----------
    name : str
        Name of the parameter
    value : float | int | str | list | tuple
        Value of the parameter
    """

    def __init__(self, name, value):
        self._name = name
        if value is None:
            self._dtype = None
        else:
            if not isinstance(value, (int, float, basestring,
                                      tuple, list)):
                raise ArcanaUsageError(
                    "Invalid type for '{}' parameter default ({}), {}, "
                    "can be one of int, float or str"
                    .format(name, value, type(value)))
            self._dtype = (str
                           if isinstance(value, basestring) else type(value))
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
        Duplicate the Parameter and rename it
        """
        duplicate = copy(self)
        duplicate._name = name
        return duplicate

    def __repr__(self):
        return "Parameter(name='{}', value={})".format(self.name,
                                                       self.value)


class ParamSpec(Parameter):
    """
    Specifies a parameter that can be passed to the study

    Parameters
    ----------
    name : str
        Name of the parameter
    default : float | int | str | list | tuple
        Default value of the parameter
    choices : List(float | int | str | list | tuple)
        Restrict valid inputs to the following choices
    desc : str
        A description of the parameter
    dtype : type | None
        The datatype of the parameter. If none will be determined from
        default value
    """

    def __init__(self, name, default, desc=None, dtype=None,
                 array=False):
        super(ParamSpec, self).__init__(name, default)
        self._desc = desc
        self._array = array
        if dtype is not None:
            if self.default is not None and (
                not array and not isinstance(self.default, dtype) or
                array and any(not isinstance(d, dtype)
                              for d in self.default)):
                raise ArcanaUsageError(
                    "Provided default value ({}) does not match explicit "
                    "dtype ({})".format(self.default, dtype))
            self._dtype = dtype

    @property
    def name(self):
        return self._name

    @property
    def array(self):
        return self._array

    @property
    def default(self):
        return self._value

    @property
    def desc(self):
        return self._desc

    def __repr__(self):
        return "ParamSpec(name='{}', default={}, desc='{}')".format(
            self.name, self.default, self.desc)

    def check_valid(self, parameter, context=None):
        if parameter.value is not None:
            error_msg = (
                "Incorrect datatype for '{}' parameter provided "
                "({}){}, Should be {}"
                .format(parameter.name, type(parameter.value),
                        'in ' + context if context is not None else '',
                        self.dtype))
            if self.array:
                if any(not isinstance(v, self.dtype) for v in parameter.value):
                    raise ArcanaUsageError(error_msg + ' array')
            elif not isinstance(parameter.value, self.dtype):
                raise ArcanaUsageError(error_msg)


class SwitchSpec(ParamSpec):
    """
    Specifies a special parameter that switches between different
    methods and/or pipeline input/outputs. Typically used to select
    between comparable methods (e.g. FSL or ANTs registration) but can
    also be used to specify whether certain methods are applied, and by
    extension some auxiliary outputs are generated

    Parameters
    ----------
    name : str
        Name of the parameter
    default : str
        Default option for the switch
    choices : list[str]
        The valid values for the switch
    desc : str
        A description of the parameter
    """

    def __init__(self, name, default, choices=None, desc=None,
                 dtype=None):
        super(SwitchSpec, self).__init__(name, default, desc=desc,
                                         dtype=dtype)
        if self.is_boolean:
            if choices is not None:
                raise ArcanaUsageError(
                    "Choices ({}) are only valid for non-boolean "
                    "switches ('{}')".format("', '".join(choices),
                                               name))
        elif choices is None:
            raise ArcanaUsageError(
                "Choices must be provided for non-boolean "
                "switches ('{}')".format(name))
        self._choices = tuple(choices) if choices is not None else None
        self._desc = desc

    @property
    def name(self):
        return self._name

    @property
    def default(self):
        return self._value

    @property
    def is_boolean(self):
        return isinstance(self.default, bool)

    @property
    def choices(self):
        return self._choices

    def check_valid(self, switch, context=''):
        super(SwitchSpec, self).check_valid(switch, context=context)
        if self.is_boolean:
            if not isinstance(switch.value, bool):
                raise ArcanaUsageError(
                    "Value provided to switch '{}'{} should be a "
                    "boolean (not {})".format(
                        self.name, context, switch.value))
        elif switch.value not in self.choices:
            raise ArcanaUsageError(
                "Value provided to switch '{}'{} ({}) is not a valid "
                "choice ('{}')".format(
                    self.name, context, switch.value,
                    "', '".join(self.choices)))

    @property
    def desc(self):
        return self._desc

    def __repr__(self):
        return ("SwitchSpec(name='{}', default={}, choices={}, "
                "desc='{}')".format(self.name, self.default,
                                    self.choices, self.desc))
