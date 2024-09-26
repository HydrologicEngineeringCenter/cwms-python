from importlib.metadata import PackageNotFoundError, version

from cwms.api import *
from cwms.catalog.catalog import *
from cwms.forecast.forecast_instance import *
from cwms.forecast.forecast_spec import *
from cwms.levels.location_levels import *
from cwms.levels.specified_levels import *
from cwms.locations.physical_locations import *
from cwms.outlets.outlets import *
from cwms.outlets.virtual_outlets import *
from cwms.projects.project_lock_rights import *
from cwms.projects.project_locks import *
from cwms.projects.projects import *
from cwms.ratings.ratings import *
from cwms.ratings.ratings_spec import *
from cwms.ratings.ratings_template import *
from cwms.standard_text.standard_text import *
from cwms.timeseries.timerseries_identifier import *
from cwms.timeseries.timeseries import *
from cwms.timeseries.timeseries_bin import *
from cwms.timeseries.timeseries_txt import *

try:
    __version__ = version("cwms-python")
except PackageNotFoundError:
    __version__ = "version-unknown"
