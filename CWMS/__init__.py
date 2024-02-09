from importlib.metadata import PackageNotFoundError, version

from .cwms_loc import *
from .cwms_ts import *
from .cwms_level import *
from .core import CwmsApiSession

try:
    __version__ = version('cwms-python')
except PackageNotFoundError:
    __version__ = 'version-unknown'

__CWBI_URL = "https://cwms-data-test.cwbi.us/cwms-data/"

def cwms_ts() -> CwmsTs:
    """
    Creates an instance of CwmsTs using a CwmsApiSession with the CWBI URL.

    Returns:
    CwmsTs: An instance of CwmsTs.
    """
    return CwmsTs(CwmsApiSession(__CWBI_URL))

def cwms_loc() -> CwmsLoc:
    """
    Creates an instance of CwmsTs using a CwmsApiSession with the CWBI URL.

    Returns:
    CwmsTs: An instance of CwmsTs.
    """
    return CwmsLoc(CwmsApiSession(__CWBI_URL))

def cwms_level() -> CwmsLevel:
    """
    Creates an instance of CwmsTs using a CwmsApiSession with the CWBI URL.

    Returns:
    CwmsTs: An instance of CwmsTs.
    """
    return CwmsLevel(CwmsApiSession(__CWBI_URL))
