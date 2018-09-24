from .base import RequirementManager


class RequirementChecker(RequirementManager):
    """
    A basic requirement manager that doesn't not attempt to load a requirement
    but simply checks whether it is satisfiable
    """

    def satisfiable(self, *requirements):
        # FIXME: Should implement some functionality to determine whether a
        #        given requirement is satisfiable.
        pass
