class NiAnalysisException(Exception):
    pass


class NiAnalysisRequirementVersionException(NiAnalysisException):
    pass


class NiAnalysisError(NiAnalysisException):
    pass


class NiAnalysisUsageError(NiAnalysisError):
    pass


class NiAnalysisNameError(NiAnalysisError):
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


class NiAnalysisXnatArchiveException(Exception):
    pass


class NiAnalysisXnatArchiveMissingDatasetException(
        NiAnalysisXnatArchiveException):
    pass


class NiAnalysisModulesNotInstalledException(NiAnalysisException):
    pass
