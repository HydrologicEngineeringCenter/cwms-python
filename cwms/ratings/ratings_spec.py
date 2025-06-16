from datetime import datetime
from typing import Optional

import pandas as pd

import cwms.api as api
from cwms.cwms_types import JSON, Data


def get_rating_spec(rating_id: str, office_id: str) -> Data:
    """Retrieves a single rating spec

      Parameters
      ----------
          rating_id: str
              specify the rating id to grad
          office_id: str
              The owning office of the rating specifications.


    Returns
      -------
      Data : Data
          cwms data type that contains .json for json dictionary and .df for dataframe
    """
    endpoint = f"ratings/spec/{rating_id}"
    params = {
        "office": office_id,
    }

    response = api.get(endpoint, params)
    return Data(response)


def get_rating_specs(
    office_id: Optional[str],
    rating_id_mask: Optional[str] = None,
    page_size: int = 500000,
) -> Data:
    """Retrieves a list of rating specification

      Parameters
      ----------
          office_id: string, optional
              The owning office of the rating specifications. If no office is provided information from all offices will
              be returned
          rating-id-mask: string, optional
              Posix regular expression that specifies the rating ids to be included in the response.  If not specified all
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


def delete_rating_spec(rating_id: str, office_id: str, delete_method: str) -> None:
    """
    Deletes rating spec for the given ID and office ID.

    Parameters
    ----------
    rating_id : str
        The ID of the rating spec value to be deleted.
    office_id : str
        The ID of the office that the standard text belongs to.
    delete_method : str
        Delete method for the standard text id.
        DELETE_ALL - deletes the key and the value from the rating table
        DELETE_KEY - deletes the text id key, but leaves the value in the rating table
        DELETE_DATA - deletes the value from the rating table but leaves the rating id key

    Returns
    -------
    None
    """
    delete_methods = ["DELETE_ALL", "DELETE_KEY", "DELETE_DATA"]
    if rating_id is None:
        raise ValueError("Deleting standard text requires an id")
    if office_id is None:
        raise ValueError("Deleting standard timeseries requires an office")
    if delete_method not in delete_methods:
        raise ValueError(
            "Deleting standard timeseries requires a delete method of DELETE_ALL, DELETE_KEY, or DELETE_DATA"
        )

    endpoint = f"ratings/spec/{rating_id}"
    params = {"office": office_id, "method": delete_method}

    return api.delete(endpoint, params)


def rating_spec_df_to_xml(data: pd.DataFrame) -> str:
    """
    Converts a dataframe containing rating specification parameters
    into xml to be stored into the database.

    Parameters
    ----------
    data : pd_dataframe
        pandas dataframe that contains rating specification parameters
        should follow same formate the is returned from get_rating_spec function
    Returns
    -------
    str: xml that can be used in store_rating_spec function
    """

    spec_xml = f"""<?xml version="1.0" encoding="utf-8"?>
    <rating-spec office-id="{data.loc[0,'office-id']}">
      <rating-spec-id>{data.loc[0,'rating-id']}</rating-spec-id>
      <template-id>{data.loc[0,'template-id']}</template-id>
      <location-id>{data.loc[0,'location-id']}</location-id>
      <version>{data.loc[0,'version']}</version>
      <source-agency>{data.loc[0,'source-agency']}</source-agency>
      <in-range-method>{data.loc[0,'in-range-method']}</in-range-method>
      <out-range-low-method>{data.loc[0,'out-range-low-method']}</out-range-low-method>
      <out-range-high-method>{data.loc[0,'out-range-high-method']}</out-range-high-method>
      <active>{str(data.loc[0,'active']).lower()}</active>
      <auto-update>{str(data.loc[0,'auto-update']).lower()}</auto-update>
      <auto-activate>{str(data.loc[0,'auto-activate']).lower()}</auto-activate>
      <auto-migrate-extension>{str(data.loc[0,'auto-migrate-extension']).lower()}</auto-migrate-extension>
      <ind-rounding-specs>"""

    ind_rounding = data.loc[0, "independent-rounding-specs"]
    if isinstance(ind_rounding, list):
        i = 1
        for rounding in ind_rounding:
            spec_xml = (
                spec_xml
                + f"""\n   <ind-rounding-spec position="{i}">{rounding['value']}</ind-rounding-spec>"""
            )
            i = i + 1
    spec_xml2 = f"""\n  </ind-rounding-specs>
      <dep-rounding-spec>{data.loc[0,'dependent-rounding-spec']}</dep-rounding-spec>
      <description>{data.loc[0,'description']}</description>
     </rating-spec>"""

    spec_xml = spec_xml + spec_xml2

    return spec_xml


def store_rating_spec(data: str, fail_if_exists: Optional[bool] = True) -> None:
    """
    This method is used to store a new rating spec.

    Parameters
    ----------
    data : str
        xml for saving a rating spec example

        <?xml version="1.0" encoding="UTF-8"?>
        <RatingSpec>
            <office-id>string</office-id>
            <rating-id>string</rating-id>
            <template-id>string</template-id>
            <location-id>string</location-id>
            <version>string</version>
            <source-agency>string</source-agency>
            <in-range-method>string</in-range-method>
            <out-range-low-method>string</out-range-low-method>
            <out-range-high-method>string</out-range-high-method>
            <active>true</active>
            <auto-update>true</auto-update>
            <auto-activate>true</auto-activate>
            <auto-migrate-extension>true</auto-migrate-extension>
            <independent-rounding-specs>
                <position>0</position>
                <value>string</value>
            </independent-rounding-specs>
            <dependent-rounding-spec>string</dependent-rounding-spec>
            <description>string</description>
            <effective-dates>2024-06-28T17:53:27.214Z</effective-dates>
        </RatingSpec>

    fail_if_exists : str, optional
        Throw a ClientError if the text id already exists.
        Default is `False`.

    Returns
    -------
    None
    """

    if data is None:
        raise ValueError("Cannot store a standard text without a rating xml")

    endpoint = "ratings/spec/"
    params = {"fail-if-exists": fail_if_exists}

    return api.post(endpoint, data, params, api_version=102)
