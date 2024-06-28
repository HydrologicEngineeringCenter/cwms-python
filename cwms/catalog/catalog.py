from typing import Optional, Dict

import cwms.api as api
from cwms.types import Data


def get_catalog(dataset: str, params: Dict[str, Optional[str]]) -> Data:
    """Retrieves filters to display a catalog of data.

        Parameters
            ----------
            dataset: string
                The type of data in the list. Valid values for this field
                are:
                    1. TIMESERIES
                    2. LOCATIONS
            params: dict
                A dictionary containing the following keys:
                    page: string
                        The endpoint used to identify where the request is located.
                    page_size: integer
                        The entries per page returned. The default value is 5000.
                    unit_system: string
                        The unit system desired in response. Valid values for this
                        field are:
                            1. SI
                            2. EN
                    office: string
                        The owning office of the timeseries group.
                    like: string
                        The regex for matching against the id
                    timeseries_category_like: string
                        The regex for matching against the category id
                    timeseries_group_like: string
                        The regex for matching against the timeseries group id
                    location_category_like: string
                        The regex for matching against the location category id
                    location_group_like: string
                        The regex for matching against the location group id
                    bounding_office_like: string
                        The regex for matching against the location bounding office

            Returns
            -------
            response : dict
                The JSON response containing the time series catalog.
        """

    if dataset is None:
        raise ValueError("Cannot retrieve a time series for a catalog without a dataset")

    endpoint = f"catalog/{dataset}"

    # creates the dataframe from the timeseries data
    response = api.get(endpoint=endpoint, params=params, api_version=1)
    return Data(response, selector="time-series-catalog")