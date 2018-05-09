class NiAnalysisException(Exception):
    pass


class NiAnalysisRequirementVersionException(NiAnalysisException):
    pass


class NiAnalysisError(NiAnalysisException):
    pass


class NiAnalysisMissingInputError(NiAnalysisException):
    pass


class NiAnalysisBadlyFormattedLocalArchiveError(NiAnalysisError):
    pass


class NiAnalysisUsageError(NiAnalysisError):
    pass


class NiAnalysisNameError(NiAnalysisError):

    def __init__(self, name, msg):
        super(NiAnalysisNameError, self).__init__(msg)
        self.name = name


class NiAnalysisMissingDataException(NiAnalysisError):
    pass


class NiAnalysisDatasetMatchError(NiAnalysisUsageError):
    pass


class NiAnalysisOutputNotProducedException(NiAnalysisException):
    """
    Raised when a given spec is not produced due to options provided
    to the study
    """


class NiAnalysisDataFormatError(NiAnalysisUsageError):
    pass


class NiAnalysisDatasetNotCachedException(NiAnalysisException):
    pass


class AcquiredComponentException(NiAnalysisException):
    pass


class NoMatchingPipelineException(NiAnalysisException):
    pass


class NiAnalysisModulesNotInstalledException(NiAnalysisException):
    pass


class NiAnalysisJobSubmittedException(NiAnalysisException):
    """
    Signifies that a pipeline has been submitted to a scheduler and
    a return value won't be returned.
    """


class NiAnalysisNoRunRequiredException(NiAnalysisException):
    """
    Used to signify when a pipeline doesn't need to be run as all
    required outputs are already present in the archive
    """


class NiAnalysisDataFormatClashError(NiAnalysisError):
    """
    Used when two mismatching data formats are registered with the same
    name or extention
    """


class NiAnalysisNoConverterError(NiAnalysisError):
    "No converters exist between formats"


class NiAnalysisConverterNotAvailableError(NiAnalysisError):
    "The converter required to convert between formats is not "
    "available"


class NiAnalysisDataFormatNotRegisteredError(NiAnalysisError):
    pass
