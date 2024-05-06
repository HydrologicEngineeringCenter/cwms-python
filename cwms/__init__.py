from importlib.metadata import PackageNotFoundError, version

from .api import *
from .forecast.forecast_instance import *
from .forecast.forecast_spec import *
from .levels.location_levels import *
from .levels.specified_levels import *
from .locations.physical_locations import *
from .timeseries.timeseries import *
from .timeseries.timeseries_bin import *
from .timeseries.timeseries_txt import *

try:
    __version__ = version("cwms-python")
except PackageNotFoundError:
    __version__ = "version-unknown"
