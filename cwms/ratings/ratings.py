from datetime import datetime
from json import loads
from typing import Any, Optional

import pandas as pd

import cwms.api as api
from cwms.cwms_types import JSON, Data
from cwms.ratings.ratings_spec import get_rating_spec


def rating_current_effective_date(rating_id: str, office_id: str) -> Any:
    """Retrieve the most recent effective date for a specific rating id.

    Returns
        datatime
            the datetime of the most recent effective date for a rating id. If max effective date is
            not present for rating_id then None will be returned

    """
    # get all rating effective date information.
    rating_specs = get_rating_spec(office_id=office_id, rating_id=rating_id)
    # find the most recent effective date
    df = rating_specs.df
    if "effective-dates" in df.columns:
        max_effective = pd.to_datetime(df["effective-dates"].iloc[0]).max()
    else:
        max_effective = None
    return max_effective


def get_current_rating(
    rating_id: str,
    office_id: str,
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
        single_rating_df=True,
    )

    return rating


def get_current_rating_xml(
    rating_id: str,
    office_id: str,
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
    single_rating_df: Optional[bool] = False,
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
        single_rating_df: bool, optional = False
            Set to True when using eager and a single rating is returned.  Will place the single rating into the .df function
            used with the get_current_rating or when a only a single rating curve is to be returned.
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
    if (method == "EAGER") and single_rating_df:
        data = Data(response, selector="simple-rating.rating-points")
    elif method == "REFERENCE":
        data = Data(response)
    else:
        data = Data(response, selector="simple-rating")
    return data


def rating_simple_df_to_json(
    data: pd.DataFrame,
    rating_id: str,
    office_id: str,
    units: str,
    effective_date: datetime,
    transition_start_date: Optional[datetime] = None,
    description: Optional[str] = None,
) -> JSON:
    """This function converts a dataframe to a json dictionary in the correct format to be posted using the store_ratings function. Can
    only be used for simple ratings with a indenpendant and 1 dependant variable.

    Parameters
    ----------
        data: pd.Dataframe
            Rating Table to be stored to an exiting rating specification and template.  Can only have 2 columns ind and dep. ind
            contained the indenpendant variable and dep contains the dependent variable.
                        ind	dep
                    0	9.62	0.01
                    1	9.63	0.01
                    2	9.64	0.02
                    3	9.65	0.02
                    4	9.66	0.03
                        ...	...	...
                    2834	37.96	16204.85
                    2835	37.97	16228.6
                    2836	37.98	16252.37
                    2837	37.99	16276.17
                    2838	38.0	16300.0
        rating_id: str
            specify the rating id to post the new rating curve to
        office_id: str
            the owning office of the rating
        units: str
            units for both the independant and dependent variable seperated by ; i.e. ft;cfs or ft;ft.
        effective_date: datetime,
            The effective date of the rating curve to be stored.
        transition_start_date: datetime Optional = None
            The transitional start date of the rating curve to be stored
        description: str Optional = None
            a description to be added to the rating curve

    Returns:
        JSON
    """

    if data.columns.shape[0] != 2:
        raise TypeError(
            f"dataframe has {data.columns.shape[0]} columns. dataframe can only have 2 columns one called ind and the other called dep."
        )
    if not (data.columns == ["ind", "dep"]).all():
        raise TypeError(
            "dataframe column names need to be ['ind','dep'] and in that order."
        )
    if len(units.split(";")) != 2:
        raise TypeError(
            "units needs to contain the units for ind and dep columns in dataframe divided by ;. i.e. ft;cfs"
        )

    rating_header = get_ratings(
        rating_id=rating_id, office_id=office_id, method="REFERENCE"
    )

    points_json = loads(data.to_json(orient="records"))

    simple_rating = {
        "simple-rating": {
            "office-id": office_id,
            "rating-spec-id": rating_id,
            "units-id": units,
            "effective-date": effective_date.isoformat(),
            "transition-start-date": (
                transition_start_date.isoformat() if transition_start_date else None
            ),
            "active": True,
            "description": description,
            "rating-points": {"point": points_json},
        }
    }

    rating_json = rating_header.json
    rating_json.update(simple_rating)

    return rating_json


def update_ratings(
    data: Any, rating_id: str, store_template: Optional[bool] = True
) -> None:
    """Will store a new rating curve to an existing rating specification can be JSON or XML

    Parameters
    ----------
        data: JSON dictionary or XML
            rating data to be stored.
        store_template: Boolean Default = True
            Store updates to the rating template.  Default = True

    Returns
    -------
    response
    """

    endpoint = f"ratings/{rating_id}"
    params = {"store-template": store_template}

    if not isinstance(data, dict) and "<?xml" not in data:
        raise ValueError(
            "Cannot store a timeseries without a JSON data dictionaryor in XML"
        )

    if "<?xml" in data:
        api_version = 102
    else:
        api_version = 2
    return api.patch(endpoint, data, params, api_version=api_version)


def delete_ratings(
    rating_id: str,
    office_id: str,
    begin: datetime,
    end: datetime,
    timezone: Optional[str] = None,
) -> None:
    """Delete ratings for a specific rating-id within the specified time window

    Parameters
    ----------
        rating_id: string
            The rating-id of the effective dates to be retrieved
        office_id: string
            The owning office of the rating specifications. If no office is provided information from all offices will
            be returned
        begin: datetime
            the start of the time window for data to be deleted.  This is based on the effective date of the ratings
        end: datetime
            the end of the time window for data to be deleted. This is based on the effective date of the ratings
        timezone:
            the time zone of the values in the being and end fields if not specified UTC is used

    Returns
    -------
    response
    """
    if rating_id is None:
        raise ValueError("Deleting rating requires an id")
    if office_id is None:
        raise ValueError("Deleting rating requires an office")
    if begin is None:
        raise ValueError("Deleting rating requires a time window")
    if end is None:
        raise ValueError("Deleting rating requires a time window")

    endpoint = f"ratings/{rating_id}"

    params = {
        "office": office_id,
        "begin": begin.isoformat(),
        "end": end.isoformat(),
        "timezone": timezone,
    }

    return api.delete(endpoint, params)
