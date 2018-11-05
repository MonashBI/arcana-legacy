from __future__ import division
import logging
from .base import BaseEnvironment
from arcana.exception import (
    ArcanaRequirementNotFoundError, ArcanaVersionNotDectableError,
    ArcanaVersionError)

logger = logging.getLogger('arcana')


class StaticEnvironment(BaseEnvironment):
    """
    Checks to see if requirements are satisfiable by the current
    computing environment. Subclasses can also manage the loading and
    unloading of modules

    Parameters
    ----------
    fail_on_missing : bool
        Raise an error if a requirement is not satisfied
    fail_on_undetectable : bool
        Raise an error if the version of a requirement cannot be detected
    """

    def __init__(self, fail_on_missing=True, fail_on_undetectable=True):
        self._fail_on_missing = fail_on_missing
        self._fail_on_undectable = fail_on_undetectable

    def satisfy(self, *requirements):
        """
        Checks whether the given requirements are satisfiable within the given
        execution context

        Parameter
        ---------
        requirements : list(Requirement)
            List of requirements to check whether they are satisfiable
        """
        versions = []
        for req_range in requirements:
            try:
                version = req_range.requirement.detect_version()
            except ArcanaRequirementNotFoundError as e:
                if self._fail_on_missing:
                    raise
                else:
                    logger.warning(e)
            except ArcanaVersionNotDectableError as e:
                if self._fail_on_undetectable:
                    raise
                else:
                    logger.warning(e)
            if not req_range.within(version):
                raise ArcanaVersionError(
                    "Detected {} version {} is not within requested range {}"
                    .format(req_range.requirement, version, req_range))
            versions.append(version)
        return versions
