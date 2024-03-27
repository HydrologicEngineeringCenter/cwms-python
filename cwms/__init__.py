from importlib.metadata import PackageNotFoundError, version

from .core import CwmsApiSession
from .levels.location_levels import *
from .locations.physical_locations import *
from .timeseries import *

try:
    __version__ = version("cwms-python")
except PackageNotFoundError:
    __version__ = "version-unknown"
