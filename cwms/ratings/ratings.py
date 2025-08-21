import logging
from datetime import datetime
from json import loads
from typing import Any, Optional, cast

import pandas as pd

import cwms.api as api
from cwms.cwms_types import JSON, Data
from cwms.ratings.ratings_spec import get_rating_spec

xml_heading = "<?xml"


def rating_current_effective_date(rating_id: str, office_id: str) -> Any:
    """Retrieve the most recent effective date for a specific rating id.

    Returns
        Any
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
            define if the independent and dependant variables should be stored as a dataframe
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
    """Retrieves ratings for a specific rating-id

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
            the end of the time window for data to be included int he response. This is based on the effective date of the ratings
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
    active: Optional[bool] = True,
) -> JSON:
    """This function converts a dataframe to a json dictionary in the correct format to be posted using the store_ratings function. Can
    only be used for simple ratings with a independent and 1 dependant variable.

    Parameters
    ----------
        data: pd.Dataframe
            Rating Table to be stored to an exiting rating specification and template.  Can only have 2 columns ind and dep. ind
            contained the independent variable and dep contains the dependent variable.
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
            units for both the independent and dependent variable separated by ; i.e. ft;cfs or ft;ft.
        effective_date: datetime,
            The effective date of the rating curve to be stored.
        transition_start_date: datetime Optional = None
            The transitional start date of the rating curve to be stored
        description: str Optional = None
            a description to be added to the rating curve
        active: Boolean Optional = True
            store the rating as active as True of False

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
            "active": active,
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

    if not isinstance(data, dict) and xml_heading not in data:
        raise ValueError(
            "Cannot store a rating without a JSON data dictionary or in XML"
        )

    if xml_heading in data:
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


def store_rating(data: Any, store_template: Optional[bool] = True) -> None:
    """Will create a new rating-set including template/spec and rating

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

    endpoint = "ratings"
    params = {"store-template": store_template}

    if not isinstance(data, dict) and xml_heading not in data:
        raise ValueError(
            "Cannot store a timeseries without a JSON data dictionary or in XML"
        )

    if xml_heading in data:
        api_version = 102
    else:
        api_version = 2
    return api.post(endpoint, data, params, api_version=api_version)


def _validate_rating_params(
    rating_id: str, office_id: str, units: str, values: list[list[float]]
) -> str:
    if not rating_id:
        raise ValueError("Cannot rate values without a rating identifier")
    parts = rating_id.split(".")
    if len(parts) != 4:
        raise ValueError(f"Invalid rating identifer: {rating_id}")
    try:
        ind_params, _ = parts[1].split(";")
    except Exception:
        raise ValueError(f"Invalid rating template: {parts[1]}")
    if not office_id:
        raise ValueError("Cannot rate values without an office identifier")
    if not units:
        raise ValueError("Cannot rate values without units")
    if not values:
        raise ValueError("No values specified")
    return ind_params


def _get_times(value_count: int, times: Optional[list[int]]) -> list[int]:
    if times:
        time_count = len(times)
        if time_count == 0:
            times = value_count * [int(datetime.now().timestamp())]
        if time_count < value_count:
            times = (value_count - time_count) * [times[-1]]
        elif time_count > value_count:
            times = times[:value_count]
    else:
        times = value_count * [int(datetime.now().timestamp())]
    return times


def _perform_value_rating(
    reverse_rate: bool,
    rating_id: str,
    office_id: str,
    units: str,
    values: list[list[float]],
    times: Optional[list[int]] = None,
    rating_time: Optional[int] = None,
    round: bool = False,
) -> JSON:

    # ------------------------------ #
    # for forward and reverse rating #
    # ------------------------------ #
    ind_params = _validate_rating_params(rating_id, office_id, units, values)
    try:
        ind_units_str, dep_unit = units.split(";")
    except Exception:
        raise ValueError("Invalid units string")
    value_count = len(values[0])
    times = _get_times(value_count, times)
    if not rating_time:
        rating_time = int(datetime.now().timestamp())

    if reverse_rate:
        # ----------------------- #
        # for reverse rating only #
        # ----------------------- #
        if ind_params.find(",") != -1:
            raise ValueError(
                "Cannot reverse-rate with a rating specification with multiple independent parameters"
            )

        endpoint = f"ratings/reverse-rate-values/{office_id}/{rating_id}"

        data = {
            "input-units": [dep_unit],
            "output-unit": ind_units_str,
            "values": values,
            "value-times": times,
            "rating-time": rating_time,
            "round": round,
        }
    else:
        # ----------------------- #
        # for forward rating only #
        # ----------------------- #
        ind_param_count = len(ind_params.split(","))
        ind_units = ind_units_str.split(",")
        if len(ind_units) != ind_param_count:
            raise ValueError(
                f"Expected {ind_param_count} indpendent parameter units, got {len(ind_units)}"
            )
        if len(values) != ind_param_count:
            raise ValueError(
                f"Expected {ind_param_count} lists of independent values, got {len(values)}"
            )
        for i in range(1, ind_param_count):
            if len(values[i]) != value_count:
                raise ValueError(
                    "Independent parameter value lists are not all of same length"
                )

        endpoint = f"ratings/rate-values/{office_id}/{rating_id}"

        data = {
            "input-units": ind_units,
            "output-unit": dep_unit,
            "values": values,
            "value-times": times,
            "rating-time": rating_time,
            "round": round,
        }

    response = api.post_with_returned_data(endpoint=endpoint, data=data, api_version=1)
    return cast(JSON, response)


def rate_values(
    rating_id: str,
    office_id: str,
    units: str,
    values: list[list[float]],
    times: Optional[list[int]] = None,
    rating_time: Optional[int] = None,
    round: bool = False,
) -> JSON:
    """Rates a list of independent parameter values using a specified rating set in
    the database. Returns the rated (dependent) parameter values in the response.

    Parameters
    ----------
    rating_id: string
        The rating-id (rating specification) of the the rating set to use
    office_id: string
        The owning office of the rating set.
    units: string
        The unit of each independent parameter separated by commas concatenated with a semicolon followed by the
        desired dependent_parameter unit (e.g., 'ft;ac-ft' or 'unit,%,ft;cfs')
    values: list[list[float]]
        The independent parameter values to rate (one list for each independent parameter, in parameter order).
        For multiple independent parameters, each list must be of the same length.
    times: list[integer] Optional Default = None
        A list of times (in milliseconds since 1970-01-01T00:00:00Z) for the independent parameter values.
        If None, the current time is used.
        If specified but the constains fewer times than the number of independent parameter values, the last
        time is used for all independent parameter values from that position to the end of the list.
    rating_time: integer Optional Default = None
        A specific date/time to use as the "current time" of the rating. No ratings with a create date later than
        this will be used. Useful for performing historical ratings. If not specified or NULL, the current time is used.
    round: boolean Optional Default = False
        Whether to round the rated values according to the rounding spec contained in the rating specification in the database


    Returns
    -------
    response
    """
    return _perform_value_rating(
        reverse_rate=False,
        rating_id=rating_id,
        office_id=office_id,
        units=units,
        values=values,
        times=times,
        rating_time=rating_time,
        round=round,
    )


def reverse_rate_values(
    rating_id: str,
    office_id: str,
    units: str,
    values: list[float],
    times: Optional[list[int]] = None,
    rating_time: Optional[int] = None,
    round: bool = False,
) -> JSON:
    """Reverse rates a list of dependent parameter values using a specified rating set in
    the database. Returns the rated (independent) parameter values in the response.

    Reverse ratings can only be performed on single-input-parameter ratings.

    Parameters
    ----------
    rating_id: string
        The rating-id (rating specification) of the the rating set to use
    office_id: string
        The owning office of the rating set.
    units: string
        The independent parameter unit and the dependent parameter unit concatenated by a semicolon (e.g., "ft;cfs", "ft;ac-ft")
    values: list[float]
        The dependent parameter values to rate.
    times: list[integer] Optional Default = None
        A list of times (in milliseconds since 1970-01-01T00:00:00Z) for the dependent parameter values.
        If None, the current time is used.
        If specified but the constains fewer times than the number of independent parameter values, the last
        time is used for all independent parameter values from that position to the end of the list.
    rating_time: integer Optional Default = None
        A specific date/time to use as the "current time" of the rating. No ratings with a create date later than
        this will be used. Useful for performing historical ratings. If not specified or NULL, the current time is used.
    round: boolean Optional Default = False
        Whether to round the rated values according to the rounding spec contained in the rating specification in the database


    Returns
    -------
    response
    """
    return _perform_value_rating(
        reverse_rate=True,
        rating_id=rating_id,
        office_id=office_id,
        units=units,
        values=[values],
        times=times,
        rating_time=rating_time,
        round=round,
    )
