from datetime import datetime
from typing import Optional

import pandas as pd

import cwms.api as api
from cwms.cwms_types import JSON, Data


def get_rating_template(template_id: str, office_id: str) -> Data:
    """Retrives a single rating spec

      Parameters
      ----------
          template_id: str
              specify the rating template id to grab information for
          office_id: str
              The owning office of the rating template.


    Returns
      -------
      Data : Data
          cwms data type that contains .json for json dictionary and .df for dataframe
    """
    endpoint = f"ratings/template/{template_id}"
    params = {
        "office": office_id,
    }

    response = api.get(endpoint, params)
    return Data(response)


def get_rating_templates(
    office_id: Optional[str],
    template_id_mask: Optional[str] = None,
    page_size: int = 500000,
) -> Data:
    """Retrives a list of rating specification

      Parameters
      ----------
          office_id: string, optional
              The owning office of the rating template. If no office is provided information from all offices will
              be returned
          rtemplate_id_mask: string, optional
              Posix regular expression that specifies the template ids to be included in the reponce.  If not specified all
              rating templates shall be returned.
          page_size: int, optional, default is 5000000: Specifies the number of records to obtain in
              a single call.


    Returns
      -------
      Data : Data
          cwms data type that contains .json for json dictionary and .df for dataframe
    """
    endpoint = "ratings/template"
    params = {
        "office": office_id,
        "template-id-mask": template_id_mask,
        "page-size": page_size,
    }

    response = api.get(endpoint, params)
    return Data(response, selector="templates")


def delete_rating_template(
    template_id: str, office_id: str, delete_method: str
) -> None:
    """
    Deletes rating spec for the given ID and office ID.

    Parameters
    ----------
    template_id : str
        The ID of the template to be deleted.
    office_id : str
        The ID of the office that the standard text belongs to.
    delete_method : str
        Delete method for the standard text id.
        DELETE_ALL - deletes the key and the value from the template table
        DELETE_KEY - deletes the text id key, but leaves the value in the template table
        DELETE_DATA - deletes the value from the template table but leaves the template id key

    Returns
    -------
    None
    """
    delete_methods = ["DELETE_ALL", "DELETE_KEY", "DELETE_DATA"]
    if template_id is None:
        raise ValueError("Deleting standard text requires an id")
    if office_id is None:
        raise ValueError("Deleting standard timeseries requires an office")
    if delete_method not in delete_methods:
        raise ValueError(
            "Deleting standard timeseries requires a delete method of DELETE_ALL, DELETE_KEY, or DELETE_DATA"
        )

    endpoint = f"ratings/template/{template_id}"
    params = {"office": office_id, "method": delete_method}

    return api.delete(endpoint, params)


def store_rating_template(data: str, fail_if_exists: Optional[bool] = True) -> None:
    """
    This method is used to store a new rating template

    Parameters
    ----------
    data : str
        xml for saving a rating template example

        <?xml version="1.0" encoding="UTF-8"?>
        <RatingTemplate>
            <office-id>string</office-id>
            <id>string</id>
            <version>string</version>
            <description>string</description>
            <dependent-parameter>string</dependent-parameter>
            <independent-parameter-specs>
                <parameter>string</parameter>
                <in-range-method>string</in-range-method>
                <out-range-low-method>string</out-range-low-method>
                <out-range-high-method>string</out-range-high-method>
            </independent-parameter-specs>
            <rating-ids>string</rating-ids>
        </RatingTemplate>

    fail_if_exists : str, optional
        Throw a ClientError if the text id already exists.
        Default is `False`.

    Returns
    -------
    None
    """

    if data is None:
        raise ValueError("Cannot store a standard text without a rating xml")

    endpoint = "ratings/template/"
    params = {"fail-if-exists": fail_if_exists}

    return api.post(endpoint, data, params, api_version=102)
