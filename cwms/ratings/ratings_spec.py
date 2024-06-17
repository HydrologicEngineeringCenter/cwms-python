from datetime import datetime
from typing import Optional

import pandas as pd

import cwms.api as api
from cwms.types import JSON, Data


def get_rating_specs(
    office_id: Optional[str],
    rating_id_mask: Optional[str] = None,
    page_size: int = 500000,
) -> Data:
    """Retrives a list of rating specification

      Parameters
      ----------
          office_id: string, optional
              The owning office of the rating specifications. If no office is provided information from all offices will
              be returned
          rating-id-mask: string, optional
              Posix regular expression that specifies the rating ids to be included in the reponce.  If not specified all
              rating specs shall be returned.
          page-size: int, optional, default is 5000000: Specifies the number of records to obtain in
              a single call.


    Returns
      -------
      Data : Data
          cwms data type that contains .json for json dictionary and .df for dataframe
    """
    endpoint = "ratings/spec"
    params = {
        "office": office_id,
        "rating-id-mask": rating_id_mask,
        "page-size": page_size,
    }

    response = api.get(endpoint, params)
    return Data(response, selector="specs")
