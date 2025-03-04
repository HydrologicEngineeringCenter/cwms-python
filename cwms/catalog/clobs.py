from typing import Optional

import cwms.api as api
from cwms.cwms_types import JSON, Data


def get_clob(clob_id: str, office_id: str, clob_id_query: Optional[str] = None) -> Data:
    """Get a single clob.

    Parameters
        ----------
            clob_id: string
                Specifies the id of the clob
            office_id: string
                Specifies the office of the clob.
            clob_id_query: string
                If this query parameter is provided the id path parameter is ignored and the
                value of the query parameter is used. Note: this query parameter is necessary
                for id's that contain '/' or other special characters. Because of abuse even
                properly escaped '/' in url paths are blocked. When using this query parameter
                a valid path parameter must still be provided for the request to be properly
                routed. If your clob id contains '/' you can't specify the clob-id query
                parameter and also specify the id path parameter because firewall and/or server
                rules will deny the request even though you are specifying this override. "ignored"
                is suggested.


        Returns
        -------
            cwms data type.  data.json will return the JSON output and data.df will return a dataframe
    """

    endpoint = f"clobs/{clob_id}"
    params = {
        "office": office_id,
        "clob-id-query": clob_id_query,
    }
    response = api.get(endpoint, params)
    return Data(response)


def get_clobs(
    office_id: Optional[str] = None,
    page_size: Optional[int] = 100,
    include_values: Optional[bool] = False,
    clob_id_like: Optional[str] = None,
) -> Data:
    """Get a subset of Clobs

    Parameters
        ----------
            office_id: Optional[string]
                Specifies the office of the clob.
            page_sie: Optional[Integer]
                How many entries per page returned. Default 100.
            include_values: Optional[Boolean]
                Do you want the value associated with this particular clob (default: false)
            clob_id_like: Optional[string]
                Posix regular expression matching against the clob id

        Returns
        -------
            cwms data type.  data.json will return the JSON output and data.df will return a dataframe
    """

    endpoint = "clobs"
    params = {
        "office": office_id,
        "page-size": page_size,
        "include-values": include_values,
        "like": clob_id_like,
    }

    response = api.get(endpoint, params)
    return Data(response, selector="clobs")


def delete_clob(clob_id: str, office_id: str) -> None:
    """Deletes requested clob

    Parameters
        ----------
            clob_id: string
                Specifies the id of the clob to be deleted
            office_id: string
                Specifies the office of the clob.

        Returns
        -------
            None
    """

    endpoint = f"clobs/{clob_id}"
    params = {"office": office_id}

    return api.delete(endpoint, params=params, api_version=1)


def update_clob(data: JSON, clob_id: str, ignore_nulls: Optional[bool] = True) -> None:
    """Updates clob

    Parameters
        ----------
            Data: JSON dictionary
                JSON containing information of Clob to be updated
                   {
                        "office-id": "string",
                        "id": "string",
                        "description": "string",
                        "value": "string"
                    }
            clob_id: string
                Specifies the id of the clob to be deleted
            ignore_nulls: Boolean
                If true, null and empty fields in the provided clob will be ignored and the existing value of those fields left in place. Default: true

        Returns
        -------
            None
    """

    if not isinstance(data, dict):
        raise ValueError("Cannot store a Clob without a JSON data dictionary")

    endpoint = f"clobs/{clob_id}"
    params = {"ignore-nulls": ignore_nulls}

    return api.patch(endpoint, data, params, api_version=1)


def store_clobs(data: JSON, fail_if_exists: Optional[bool] = True) -> None:
    """Create New Clob

    Parameters
        ----------
            Data: JSON dictionary
                JSON containing information of Clob to be updated
                   {
                        "office-id": "string",
                        "id": "string",
                        "description": "string",
                        "value": "string"
                    }
            fail_if_exists: Boolean
                Create will fail if provided ID already exists. Default: true

        Returns
        -------
            None
    """

    if not isinstance(data, dict):
        raise ValueError("Cannot store a Clob without a JSON data dictionary")

    endpoint = "clobs"
    params = {"fail-if-exists": fail_if_exists}

    return api.post(endpoint, data, params, api_version=1)
