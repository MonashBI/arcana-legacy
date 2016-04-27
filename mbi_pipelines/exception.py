class MBIPipelinesException(Exception):
    pass


class MBIPipelinesError(MBIPipelinesException):
    pass


class AcquiredComponentException(MBIPipelinesException):
    pass


class NoMatchingPipelineException(MBIPipelinesException):
    pass


class DarisException(Exception):
    pass


class DarisExistingCIDException(DarisException):
    pass


class DarisNameNotFoundException(DarisException):
    pass
