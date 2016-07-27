class NiAnalysisException(Exception):
    pass


class NiAnalysisError(NiAnalysisException):
    pass


class NiAnalysisScanNameError(NiAnalysisError):
    pass


class NiAnalysisMissingScanError(NiAnalysisError):
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
