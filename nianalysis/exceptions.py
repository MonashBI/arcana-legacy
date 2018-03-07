class NiAnalysisException(Exception):
    pass


class NiAnalysisRequirementVersionException(NiAnalysisException):
    pass


class NiAnalysisError(NiAnalysisException):
    pass


class NiAnalysisUsageError(NiAnalysisError):
    pass


class NiAnalysisNameError(NiAnalysisError):

    def __init__(self, name, msg):
        super(NiAnalysisNameError, self).__init__(msg)
        self.name = name


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
