from __future__ import division
import logging
from .base import Environment
from arcana.exceptions import (
    ArcanaRequirementNotFoundError, ArcanaVersionNotDetectableError,
    ArcanaVersionError)

logger = logging.getLogger('arcana')


class StaticEnv(Environment):
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
        self._fail_on_undetectable = fail_on_undetectable
        self._detected_versions = {}

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
                version = self._detected_versions[req_range.name]
            except KeyError:
                try:
                    version = req_range.requirement.detect_version()
                except ArcanaRequirementNotFoundError as e:
                    if self._fail_on_missing:
                        raise
                    else:
                        logger.warning(e)
                except ArcanaVersionNotDetectableError as e:
                    if self._fail_on_undetectable:
                        raise
                    else:
                        logger.warning(e)
                else:
                    self._detected_versions[req_range.name] = version
            if not req_range.within(version):
                raise ArcanaVersionError(
                    "Detected {} version {} is not within requested range {}"
                    .format(req_range.requirement, version, req_range))
            versions.append(version)
        return versions
