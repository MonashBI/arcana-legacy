class ArcanaException(Exception):
    pass


class ArcanaRequirementVersionException(ArcanaException):
    pass


class ArcanaError(ArcanaException):
    pass


class ArcanaRequirementNotSatisfiedError(ArcanaError):
    pass


class ArcanaEnvModuleNotLoadedError(ArcanaError):
    pass


class ArcanaMissingInputError(ArcanaException):
    pass


class ArcanaCantPickleStudyError(ArcanaError):
    pass


class ArcanaBadlyFormattedDirectoryRepositoryError(ArcanaError):
    pass


class ArcanaUsageError(ArcanaError):
    pass


class ArcanaDesignError(ArcanaError):
    pass


class ArcanaNameError(ArcanaError):

    def __init__(self, name, msg):
        super(ArcanaNameError, self).__init__(msg)
        self.name = name


class ArcanaIndexError(ArcanaError):

    def __init__(self, index, msg):
        super(ArcanaIndexError, self).__init__(msg)
        self.index = index


class ArcanaMissingDataException(ArcanaError):
    pass


class ArcanaFilesetSelectorError(ArcanaUsageError):
    pass


class ArcanaOutputNotProducedException(ArcanaException):
    """
    Raised when a given spec is not produced due to switches and inputs
    provided to the study
    """


class ArcanaFileFormatError(ArcanaError):
    pass


class ArcanaFilesetNotCachedException(ArcanaException):
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
    required outputs are already present in the repository
    """


class ArcanaFileFormatClashError(ArcanaError):
    """
    Used when two mismatching data formats are registered with the same
    name or extention
    """


class ArcanaNoConverterError(ArcanaError):
    "No converters exist between formats"


class ArcanaConverterNotAvailableError(ArcanaError):
    "The converter required to convert between formats is not "
    "available"


class ArcanaFileFormatNotRegisteredError(ArcanaError):
    pass
