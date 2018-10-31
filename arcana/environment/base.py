from __future__ import division


class BaseEnvironment(object):
    """
    Base class for all Environment classes
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
        raise NotImplementedError

    def load(self, *versions, **kwargs):
        """
        Loads the given requirements if necessary

        Parameter
        ---------
        versions : list(Version)
            List of versions to load
        """
        raise NotImplementedError

    def unload(self, *versions, **kwargs):
        """
        Unloads the given requirements if necessary

        Parameter
        ---------
        versions : list(Version)
            List of versions to unload
        """
        raise NotImplementedError
