from copy import deepcopy
from enum import Enum, auto
from typing import Any, Optional

from pandas import DataFrame, Index, json_normalize, to_datetime

# Describes generic JSON serializable data.
JSON = dict[str, Any]

# Describes request parameters.
RequestParams = dict[str, Any]


class DeleteMethod(Enum):
    DELETE_ALL = auto()
    DELETE_KEY = auto()
    DELETE_DATA = auto()


class RatingMethod(Enum):
    EAGER = auto()
    LAZY = auto()
    REFERENCE = auto()


class Data:
    """Wrapper for CWMS API data."""

    def __init__(self, json: JSON, *, selector: Optional[str] = None):
        """Wrap CWMS API Data.

        Args:
            data:
            selector:
        """

        self.json = json
        self.selector = selector

        self._df: Optional[DataFrame] = None

    @staticmethod
    def to_df(json: JSON, selector: Optional[str]) -> DataFrame:
        """Create a data frame from JSON data.

        Args:
            json: JSON data returned in the API response.
            selector: Dot separated string of keys used to extract data for data frame.

        Returns:
            A data frame containing the data located
        """

        def get_df_data(data: JSON, selector: str) -> JSON:
            # get the data that will be stored in the dataframe using the selectors
            df_data = data
            for key in selector.split("."):
                if key in df_data.keys():
                    df_data = df_data[key]
            return df_data

        def rating_type(data: JSON) -> DataFrame:
            # grab the correct point values for a rating table
            df = DataFrame(data["point"]) if data["point"] else DataFrame()
            return df

        def timeseries_type(orig_json: JSON, value_json: JSON) -> DataFrame:
            # if timeseries values are present then grab the values and put into
            # dataframe else create empty dataframe
            columns = Index([sub["name"] for sub in orig_json["value-columns"]])
            if value_json:
                df = DataFrame(value_json)
                df.columns = columns
            else:
                df = DataFrame(columns=columns)

            if "date-time" in df.columns:
                df["date-time"] = to_datetime(df["date-time"], unit="ms", utc=True)
            return df

        data = deepcopy(json)

        if selector:
            df_data = get_df_data(data, selector)

            # if the dataframe is for a rating table
            if ("rating-points" in selector) and ("point" in df_data.keys()):
                df = rating_type(df_data)

            elif selector == "values":
                df = timeseries_type(data, df_data)

            else:
                df = json_normalize(df_data) if df_data else DataFrame()
        else:
            df = json_normalize(data)

        return df

    @property
    def df(self) -> DataFrame:
        """Return the data frame."""

        if not isinstance(self._df, DataFrame):
            self._df = Data.to_df(self.json, self.selector)

        return self._df
