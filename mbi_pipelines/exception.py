class MBIPipelinesException(Exception):
    pass


class DarisException(Exception):
    pass


class DarisExistingCIDException(DarisException):
    pass


class DarisNameNotFoundException(DarisException):
    pass


class AcquiredComponentException(MBIPipelinesException):
    pass


class NoMatchingPipelineException(MBIPipelinesException):
    pass
