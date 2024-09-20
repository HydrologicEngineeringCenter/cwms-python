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

        data = deepcopy(json)

        if selector:
            df_data = data
            for key in selector.split("."):
                if key in df_data.keys():
                    df_data = df_data[key]

            # if the dataframe is for a rating table
            if ("rating-points" in selector) and ("point" in df_data.keys()):
                df = DataFrame(df_data["point"])

            elif selector == "values":
                df = DataFrame(df_data)
                # if timeseries values are present then grab the values and put into dataframe
                df.columns = Index([sub["name"] for sub in data["value-columns"]])

                if "date-time" in df.columns:
                    df["date-time"] = to_datetime(df["date-time"], unit="ms", utc=True)
            else:
                df = json_normalize(df_data)
        else:
            df = json_normalize(data)

        return df

    @property
    def df(self) -> DataFrame:
        """Return the data frame."""

        if type(self._df) != DataFrame:
            self._df = Data.to_df(self.json, self.selector)

        return self._df
