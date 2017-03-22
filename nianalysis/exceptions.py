class NiAnalysisException(Exception):
    pass


class NiAnalysisRequirementVersionException(NiAnalysisException):
    pass


class NiAnalysisError(NiAnalysisException):
    pass


class NiAnalysisUsageError(NiAnalysisError):
    pass


class NiAnalysisDatasetNameError(NiAnalysisError):
    pass


class NiAnalysisMissingDatasetError(NiAnalysisError):
    pass


class AcquiredComponentException(NiAnalysisException):
    pass


class NoMatchingPipelineException(NiAnalysisException):
    pass


class DarisException(Exception):
    pass


class DarisExistingCIDException(DarisException):
    pass


class DarisNameNotFoundException(DarisException):
    pass


class XNATException(Exception):
    pass
