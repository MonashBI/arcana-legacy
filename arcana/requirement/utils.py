from __future__ import division
from builtins import str
import re
import logging
from arcana.exception import ArcanaRequirementVersionException


logger = logging.getLogger('arcana')


def split_version(version_str):
    logger.debug("splitting version string '{}'"
                 .format(version_str))
    try:
        sanitized_ver_str = re.match(r'[^\d]*(\d+(?:\.\d+)*)[^\d]*',
                                     version_str).group(1)
        return tuple(
            int(p) for p in sanitized_ver_str.split('.'))
    except (ValueError, AttributeError) as e:
        raise ArcanaRequirementVersionException(
            "Could not parse version string '{}': {}".format(
                version_str, e))


def date_split(version_str):
    try:
        return tuple(int(p) for p in version_str.split('-'))
    except ValueError as e:
        raise ArcanaRequirementVersionException(str(e))


def matlab_version_split(version_str):
    match = re.match(r'(?:r|R)?(\d+)(\w)', version_str)
    if match is None:
        raise ArcanaRequirementVersionException(
            "Do not understand Matlab version '{}'".format(version_str))
    return int(match.group(1)), match.group(2).lower()
