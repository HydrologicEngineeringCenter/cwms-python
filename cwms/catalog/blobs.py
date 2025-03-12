from typing import Optional

import cwms.api as api
from cwms.cwms_types import JSON, Data


def get_blob(blob_id: str, office_id: str) -> str:
    """Get a single BLOB (Binary Large Object).

    Parameters
        ----------
            blob_id: string
                Specifies the id of the blob. ALL blob ids are UPPERCASE.
            office_id: string
                Specifies the office of the blob.


        Returns
        -------
            str: the value returned based on the content-type it was stored with as a string
    """

    endpoint = f"blobs/{blob_id}"
    params = {"office": office_id}
    response = api.get(endpoint, params, api_version=1)
    return response


def get_blobs(
    office_id: Optional[str] = None,
    page_size: Optional[int] = 100,
    blob_id_like: Optional[str] = None,
) -> Data:
    """Get a subset of Blobs

    Parameters
        ----------
            office_id: Optional[string]
                Specifies the office of the blob.
            page_sie: Optional[Integer]
                How many entries per page returned. Default 100.
            blob_id_like: Optional[string]
                Posix regular expression matching against the clob id

        Returns
        -------
            cwms data type.  data.json will return the JSON output and data.df will return a dataframe
    """

    endpoint = "blobs"
    params = {"office": office_id, "page-size": page_size, "like": blob_id_like}

    response = api.get(endpoint, params, api_version=1)
    return Data(response, selector="blobs")


def store_blobs(data: JSON, fail_if_exists: Optional[bool] = True) -> None:
    """Create New Blob

    Parameters
    ----------
        **Note**: The "id" field must be uppercase, or it will be automatically cast to uppercase.

        Data: JSON dictionary
            JSON containing information of Blob to be updated.

                {
                "office-id": "string",
                "id": "STRING",
                "description": "string",
                "media-type-id": "string",
                "value": "string"
                }
        fail_if_exists: Boolean
            Create will fail if the provided ID already exists. Default: True

    Returns
    -------
        None
    """

    if not isinstance(data, dict):
        raise ValueError("Cannot store a Blob without a JSON data dictionary")

    endpoint = "blobs"
    params = {"fail-if-exists": fail_if_exists}

    return api.post(endpoint, data, params, api_version=1)
