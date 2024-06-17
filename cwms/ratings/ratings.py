from datetime import datetime
from typing import Any, Optional

import pandas as pd

import cwms.api as api
from cwms.types import JSON, Data


def rating_current_effective_date(rating_id: str, office_id: str) -> datetime:
    """Retrieve the most recent effective date for a specific rating id.

    Returns
        datatime
            the datetime of the most recent effective date for a rating id

    """
    # get all rating effective date information.
    ratings = get_ratings(rating_id=rating_id,
                          office_id=office_id, method="LAZY")

    # find the most recent effective date
    max_effective = pd.to_datetime(ratings.df["effective-date"]).max()
    return max_effective


def get_current_rating(
    rating_id: str,
    office_id: str,
    rating_table_in_df: Optional[bool] = True,
    xml: Optional[bool] = False,
) -> Data:
    """Retrives the rating table for the current active rating.  i.e. the rating table with the latest
    effective date for the rating specification

    Parameters
    ----------
        rating_id: string
            The rating-id of the effective dates to be retrieved
        office_id: string
            The owning office of the rating specifications. If no office is provided information from all offices will
            be returned
        rating_table_in_df: Bool, Optional Default = True
            define if the independant and dependant variables should be stored as a dataframe
    Returns
    -------
        Data : Data
            cwms data type that contains .json for json dictionary and .df for dataframe
    """

    max_effective = rating_current_effective_date(
        rating_id=rating_id, office_id=office_id
    )

    rating = get_ratings(
        rating_id=rating_id,
        office_id=office_id,
        begin=max_effective,
        end=max_effective,
        method="EAGER",
        rating_table_in_df=rating_table_in_df,
        xml=xml,
    )

    return rating


def get_current_rating_xml(
    rating_id: str,
    office_id: str,
    rating_table_in_df: Optional[bool] = True,
) -> Any:
    """Retrives the rating table for the current active rating.  i.e. the rating table with the latest
    effective date for the rating specification

    Parameters
    ----------
        rating_id: string
            The rating-id of the effective dates to be retrieved
        office_id: string
            The owning office of the rating specifications. If no office is provided information from all offices will
            be returned
    Returns
    -------
        rating : str
            xml data as a string
    """

    max_effective = rating_current_effective_date(
        rating_id=rating_id, office_id=office_id
    )

    rating = get_ratings_xml(
        rating_id=rating_id,
        office_id=office_id,
        begin=max_effective,
        end=max_effective,
        method="EAGER",
    )

    return rating


def get_ratings_xml(
    rating_id: str,
    office_id: str,
    begin: Optional[datetime] = None,
    end: Optional[datetime] = None,
    timezone: Optional[str] = None,
    method: Optional[str] = "EAGER",
) -> Any:
    """Retrives ratings for a specific rating-id

    Parameters
    ----------
        rating_id: string
            The rating-id of the effective dates to be retrieved
        office_id: string
            The owning office of the rating specifications. If no office is provided information from all offices will
            be returned
        begin: datetime, optional
            the start of the time window for data to be included in the response.  This is based on the effective date of the ratings
        end: datetime, optional
            the end of the time window for data to be included int he reponse. This is based on the effective date of the ratings
        timezone:
            the time zone of the values in the being and end fields if not specified UTC is used
        method:
            the retrival method used
            EAGER: retireves all ratings data include the individual dependenant and independant values
            LAZY: retrieved all rating data excluding the individual dependance and independant values
            REFERENCE: only retrievies reference data about the rating spec.
    Returns
    -------
        xml_data : str
            xml data as a string
    """
    methods = ["EAGER", "LAZY", "REFERENCE"]
    if method not in methods:
        raise ValueError("method needs to be one of EAGER, LAZY, or REFERENCE")

    endpoint = f"ratings/{rating_id}"
    params = {
        "office": office_id,
        "begin": begin.isoformat() if begin else None,
        "end": end.isoformat() if end else None,
        "timezone": timezone,
        "method": method,
    }

    xml_data = api.get_xml(endpoint, params, api_version=102)
    return xml_data


def get_ratings(
    rating_id: str,
    office_id: str,
    begin: Optional[datetime] = None,
    end: Optional[datetime] = None,
    timezone: Optional[str] = None,
    method: Optional[str] = "EAGER",
    rating_table_in_df: Optional[bool] = False,
    xml: Optional[bool] = False,
) -> Data:
    """Retrives ratings for a specific rating-id

    Parameters
    ----------
        rating_id: string
            The rating-id of the effective dates to be retrieved
        office_id: string
            The owning office of the rating specifications. If no office is provided information from all offices will
            be returned
        begin: datetime, optional
            the start of the time window for data to be included in the response.  This is based on the effective date of the ratings
        end: datetime, optional
            the end of the time window for data to be included int he reponse. This is based on the effective date of the ratings
        timezone:
            the time zone of the values in the being and end fields if not specified UTC is used
        method:
            the retrival method used
            EAGER: retireves all ratings data include the individual dependenant and independant values
            LAZY: retrieved all rating data excluding the individual dependance and independant values
            REFERENCE: only retrievies reference data about the rating spec.
    Returns
    -------
        Data : Data
            cwms data type that contains .json for json dictionary and .df for dataframe
    """
    methods = ["EAGER", "LAZY", "REFERENCE"]
    if method not in methods:
        raise ValueError("method needs to be one of EAGER, LAZY, or REFERENCE")

    endpoint = f"ratings/{rating_id}"
    params = {
        "office": office_id,
        "begin": begin.isoformat() if begin else None,
        "end": end.isoformat() if end else None,
        "timezone": timezone,
        "method": method,
    }

    response = api.get(endpoint, params)
    if rating_table_in_df and method == "EAGER":
        data = Data(response, selector="simple-rating.rating-points")
    elif method == "REFERENCE":
        data = Data(response)
    else:
        data = Data(response, selector="simple-rating")
    return data
