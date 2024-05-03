from importlib.metadata import PackageNotFoundError, version

from .api import *
from .forecast.forecast_instance import *
from .forecast.forecast_spec import *

# from .core import CwmsApiSession
from .levels.location_levels import *
from .levels.specified_levels import *
from .locations.physical_locations import *
from .timeseries.timeseries import *

try:
    __version__ = version("cwms-python")
except PackageNotFoundError:
    __version__ = "version-unknown"
