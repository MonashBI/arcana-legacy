class NeuroAnalysisException(Exception):
    pass


class NeuroAnalysisError(NeuroAnalysisException):
    pass


class NeuroAnalysisScanNameError(NeuroAnalysisError):
    pass


class NeuroAnalysisMissingScanError(NeuroAnalysisError):
    pass


class AcquiredComponentException(NeuroAnalysisException):
    pass


class NoMatchingPipelineException(NeuroAnalysisException):
    pass


class DarisException(Exception):
    pass


class DarisExistingCIDException(DarisException):
    pass


class DarisNameNotFoundException(DarisException):
    pass
