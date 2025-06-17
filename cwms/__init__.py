from importlib.metadata import PackageNotFoundError, version

from cwms.api import *
from cwms.catalog.blobs import *
from cwms.catalog.catalog import *
from cwms.catalog.clobs import *
from cwms.forecast.forecast_instance import *
from cwms.forecast.forecast_spec import *
from cwms.levels.location_levels import *
from cwms.levels.specified_levels import *
from cwms.locations.gate_changes import *
from cwms.locations.location_groups import *
from cwms.locations.physical_locations import *
from cwms.measurements.measurements import *
from cwms.outlets.outlets import *
from cwms.outlets.virtual_outlets import *
from cwms.projects.project_lock_rights import *
from cwms.projects.project_locks import *
from cwms.projects.projects import *
from cwms.projects.water_supply.accounting import *
from cwms.ratings.ratings import *
from cwms.ratings.ratings_spec import *
from cwms.ratings.ratings_template import *
from cwms.standard_text.standard_text import *
from cwms.timeseries.timeseries import *
from cwms.timeseries.timeseries_bin import *
from cwms.timeseries.timeseries_group import *
from cwms.timeseries.timeseries_identifier import *
from cwms.timeseries.timeseries_profile import *
from cwms.timeseries.timeseries_profile_instance import *
from cwms.timeseries.timeseries_profile_parser import *
from cwms.timeseries.timeseries_txt import *
from cwms.turbines.turbines import *

try:
    __version__ = version("cwms-python")
except PackageNotFoundError:
    __version__ = "version-unknown"
