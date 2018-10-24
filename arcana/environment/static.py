from __future__ import division
import logging
from arcana.exception import ArcanaRequirementNotSatisfiedError


logger = logging.getLogger('arcana')


class StaticEnvironment(object):
    """
    Checks to see if requirements are satisfiable by the current
    computing environment. Subclasses can also manage the loading and
    unloading of modules
    """

    def __eq__(self, other):
        return type(self) == type(other)

    def satisfiable(self, *requirements, **kwargs):
        """
        Checks whether the given requirements are satisfiable within the given
        execution context

        Parameter
        ---------
        requirements : list(Requirement)
            List of requirements to check whether they are satisfiable
        """
        self.load(*requirements, **kwargs)
        not_satisfied = [r for r in requirements if not r.satisfied]
        if not_satisfied:
            raise ArcanaRequirementNotSatisfiedError(
                "Could not satisfy the following requirements:\n"
                .format('\n'.join(str(r) for r in not_satisfied)))
        self.unload(*requirements, **kwargs)

    def load(self, *requirements, **kwargs):
        """
        Loads the given requirements if necessary

        Parameter
        ---------
        requirements : list(Requirement)
            List of requirements to load
        """
        pass  # Nothing is done in the basic case

    def unload(self, *requirements, **kwargs):
        """
        Unloads the given requirements if necessary

        Parameter
        ---------
        requirements : list(Requirement)
            List of requirements to unload
        """
        pass  # Nothing is done in the basic case
