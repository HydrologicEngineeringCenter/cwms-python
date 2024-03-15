from importlib.metadata import PackageNotFoundError, version

from .locations.physical_locations import *
from .timeseries import *
from .levels.location_levels import *
from .core import CwmsApiSession

try:
    __version__ = version('cwms-python')
except PackageNotFoundError:
    __version__ = 'version-unknown'
