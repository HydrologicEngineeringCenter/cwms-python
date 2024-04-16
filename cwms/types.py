from copy import deepcopy
from typing import Any, Optional

from pandas import DataFrame

# Describes generic JSON serializable data.
JSON = dict[str, Any]

# Describes request parameters.
RequestParams = dict[str, Any]


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
            for key in selector.split("."):
                data = data[key]

        return DataFrame(data)

    @property
    def df(self) -> DataFrame:
        """Return the data frame."""

        if type(self._df) != DataFrame:
            self._df = Data.to_df(self.json, self.selector)

        return self._df
