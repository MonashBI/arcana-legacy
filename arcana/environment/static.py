from __future__ import division
from .base import BaseEnvironment


class StaticEnvironment(BaseEnvironment):
    """
    Checks to see if requirements are satisfiable by the current
    computing environment. Subclasses can also manage the loading and
    unloading of modules
    """

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
        for req in requirements:
            versions.append(req.detect_version())
        return versions

    def __eq__(self, other):
        return type(self) == type(other)

    def load(self, *requirements, **kwargs):
        pass  # No active loading is performed

    def unload(self, *requirements, **kwargs):
        pass  # No active unloading is performed
