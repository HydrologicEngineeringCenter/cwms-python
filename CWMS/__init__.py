from importlib.metadata import PackageNotFoundError, version

from .cwms_loc import *
from .cwms_ts import *
from .cwms_level import *
from .core import CwmsApiSession

try:
    __version__ = version('cwms-python')
except PackageNotFoundError:
    __version__ = 'version-unknown'
