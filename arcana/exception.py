class ArcanaException(Exception):
    pass


class ArcanaRequirementVersionException(ArcanaException):
    pass


class ArcanaError(ArcanaException):
    pass


class ArcanaMissingInputError(ArcanaException):
    pass


class ArcanaCantPickleStudyError(ArcanaError):
    pass


class ArcanaBadlyFormattedLocalArchiveError(ArcanaError):
    pass


class ArcanaUsageError(ArcanaError):
    pass


class ArcanaNameError(ArcanaError):

    def __init__(self, name, msg):
        super(ArcanaNameError, self).__init__(msg)
        self.name = name


class ArcanaMissingDataException(ArcanaError):
    pass


class ArcanaDatasetMatchError(ArcanaUsageError):
    pass


class ArcanaOutputNotProducedException(ArcanaException):
    """
    Raised when a given spec is not produced due to options provided
    to the study
    """


class ArcanaDataFormatError(ArcanaUsageError):
    pass


class ArcanaDatasetNotCachedException(ArcanaException):
    pass


class AcquiredComponentException(ArcanaException):
    pass


class NoMatchingPipelineException(ArcanaException):
    pass


class ArcanaModulesNotInstalledException(ArcanaException):
    pass


class ArcanaJobSubmittedException(ArcanaException):
    """
    Signifies that a pipeline has been submitted to a scheduler and
    a return value won't be returned.
    """


class ArcanaNoRunRequiredException(ArcanaException):
    """
    Used to signify when a pipeline doesn't need to be run as all
    required outputs are already present in the archive
    """


class ArcanaDataFormatClashError(ArcanaError):
    """
    Used when two mismatching data formats are registered with the same
    name or extention
    """


class ArcanaNoConverterError(ArcanaError):
    "No converters exist between formats"


class ArcanaConverterNotAvailableError(ArcanaError):
    "The converter required to convert between formats is not "
    "available"


class ArcanaDataFormatNotRegisteredError(ArcanaError):
    pass
