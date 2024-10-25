#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

from datetime import datetime
from typing import Optional

import cwms.api as api
from cwms.cwms_types import JSON, Data


def get_all_gate_changes(
    office_id: str,
    project_id: str,
    begin: datetime,
    end: datetime,
    start_time_inclusive: Optional[bool] = True,
    end_time_inclusive: Optional[bool] = False,
    unit_system: Optional[str] = "EN",
    page_size: Optional[int] = 500,
) -> Data:
    """
    Returns all gate changes for a project within a specified time range.

    Parameters
    ----------
    office_id: string
        The owning office of the project
    project_id: string
        The project identifier
    begin: datetime
        The beginning of the time range
    end: datetime
        The end of the time range
    start_time_inclusive: boolean, optional
        Whether the returned gate changes should include data from the specified start timestamp. Default is `True`.
    end_time_inclusive: boolean, optional
        Whether the returned gate changes should include data from the specified end timestamp. Default is `False`.
    unit_system: string, optional
        The unit system to use for the gate changes. Can be SI (International Scientific) or EN (Imperial.)
        Default is `EN`.
    page_size: integer, optional
        The maximum number of gate changes to retrieve, regardless of time window. A positive integer is
        interpreted as the maximum number of changes from the beginning of the time window.
        A negative integer is interpreted as the maximum number from the end of the time window.
        Default 500. A page cursor will not be returned by this DTO. Instead, the next page can be
        determined by querying the next set of changes using the last returned change date
        and using start-time-inclusive=False.

    Returns
    -------
        cwms data type
    """

    endpoint = f"projects/{office_id}/{project_id}/gate-changes"
    params = {
        "begin": begin.isoformat() if begin else None,
        "end": end.isoformat() if end else None,
        "start-time-inclusive": start_time_inclusive,
        "end-time-inclusive": end_time_inclusive,
        "unit-system": unit_system,
        "page-size": page_size,
    }
    response = api.get(endpoint, params=params)
    return Data(response)


def store_gate_change(
    gate_change_data: JSON, fail_if_exists: Optional[bool] = True
) -> None:
    """
    Creates a gate change.

    Parameters
    ----------
    gate_change_data: JSON
        The gate change data to insert into the database.
        The data must be in JSON format as an array.

        Example:
        [{
          "type": "gate-change",
          "project-id": {
            "office-id": "SPK",
            "name": "BIGH"
          },
          "change-date": 1704096000000,
          "pool-elevation": 3.0,
          "protected": true,
          "discharge-computation-type": {
            "office-id": "CWMS",
            "display-value": "A",
            "tooltip": "Adjusted by an automated method",
            "active": true
          },
          "reason-type": {
            "office-id": "CWMS",
            "display-value": "O",
            "tooltip": "Other release",
            "active": true
          },
          "notes": "Test notes",
          "new-total-discharge-override": 1.0,
          "old-total-discharge-override": 2.0,
          "discharge-units": "cfs",
          "tailwater-elevation": 4.0,
          "elevation-units": "ft",
          "settings": [
            {
              "type": "gate-setting",
              "location-id": {
                "office-id": "SPK",
                "name": "BIGH-TG1"
              },
              "opening": 0.0,
              "opening-parameter": "Opening",
              "invert-elevation": 1.0,
              "opening-units": "ft"
            },
            {
              "type": "gate-setting",
              "location-id": {
                "office-id": "SPK",
                "name": "TG2"
              },
              "opening": 0.0,
              "opening-parameter": "Opening",
              "invert-elevation": 1.0,
              "opening-units": "ft"
            }
          ]
        }]

    fail_if_exists: boolean, optional
        Whether to fail if the gate change already exists. Default is `True`.

    Returns
    -------
        cwms data type
    """

    endpoint = "projects/gate-changes"
    params = {
        "fail-if-exists": fail_if_exists,
    }
    return api.post(endpoint, data=gate_change_data, params=params)


def delete_gate_change(
    office_id: str,
    project_id: str,
    begin: datetime,
    end: datetime,
    override_protection: Optional[bool] = False,
) -> None:
    """
    Deletes a gate change.

    Parameters
    ----------
        office_id: string
            The owning office of the gate change.
        project_id: string
            The project identifier.
        begin: datetime
            The beginning of the time range.
        end: datetime
            The end of the time range.
        override_protection: boolean, optional
            Whether to enable override protection for the gate change. Default is `False`.

    Returns
    -------
        None
    """

    endpoint = f"projects/{office_id}/{project_id}/gate-changes"
    params = {
        "begin": begin.isoformat() if begin else None,
        "end": end.isoformat() if end else None,
        "override-protection": override_protection,
    }

    return api.delete(endpoint, params=params)
