#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
import json
from datetime import datetime

import cwms._constants as constants
from cwms.core import CwmsApiSession
from cwms.levels.location_levels import CwmsLevel

session = CwmsApiSession("http://localhost:7000/cwms-data/", "apikey testkey")
level_api = CwmsLevel(session)


def run_spec_level_examples():
    print("------Running through spec level examples-------")
    specified_level = json.loads(
        """
        {
            "office-id": "SPK",
            "id": "Top of Surcharge",
            "description": "TestLevel"
        }
        """
    )
    print(f"Storing a specified level {specified_level['id']}")
    level_api.store_specified_level_json(specified_level, False)

    spec_level_dict = level_api.retrieve_specified_levels_json(
        "Top of Surcharge", "SPK"
    )[0]
    print(spec_level_dict)

    new_id = "Top of Surcharge2"
    print(f"Changing the name of specified level {specified_level['id']} to {new_id}")
    level_api.update_specified_level("Top of Surcharge", new_id, "SPK")
    spec_level_dict = level_api.retrieve_specified_levels_json(new_id, "SPK")[0]
    print(spec_level_dict["id"])
    print(f"Deleting specified level id {new_id}")
    level_api.delete_specified_level("Top of Surcharge2", "SPK")
    spec_levels = level_api.retrieve_specified_levels_json("Top of Surcharge*", "SPK")
    print(f"Confirming delete of spec id")
    print(spec_levels)


def run_loc_level_examples():
    print("------Running through loc level examples-------")
    location = """
        {
          "name": "TEST",
          "latitude": 0,
          "longitude": 0,
          "active": true,
          "public-name": "CWMS TESTING",
          "long-name": "CWMS TESTING",
          "description": "CWMS TESTING",
          "timezone-name": "America/Los_Angeles",
          "location-kind": "PROJECT",
          "nation": "US",
          "state-initial": "CA",
          "county-name": "Yolo",
          "nearest-city": "Davis, CA",
          "horizontal-datum": "NAD83",
          "vertical-datum": "NGVD29",
          "elevation": 320.04,
          "bounding-office-id": "SPK",
          "office-id": "SPK"
        }
        """
    headers = {"Content-Type": constants.HEADER_JSON_V1}
    print("Storing location TEST")
    level_api.get_session().post(
        "locations", params=None, headers=headers, data=location
    )
    level_dict = json.loads(
        """
        {
          "location-level-id": "TEST.Elev.Inst.0.Top of Dam",
          "office-id": "SPK",
          "specified-level-id": "Top of Dam",
          "parameter-type-id": "Inst",
          "parameter-id": "Elev",
          "constant-value": 145.6944,
          "level-units-id": "m",
          "level-date": "1900-01-01T06:00:00Z",
          "duration-id": "0"
        }
        """
    )
    print(f"Storing location level {level_dict['location-level-id']}")
    level_api.store_location_level_json(level_dict)
    date_string = "1900-01-01T06:00:00"
    office_id = "SPK"
    effective_date = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S")
    level_id = level_dict["location-level-id"]
    level = level_api.retrieve_location_level_json(level_id, office_id, effective_date)
    print(level)
    print(f"Retrieving level {level_id} as an hourly timeseries for the past day")
    time_series = level_api.retrieve_level_as_timeseries_json(
        level_id, office_id, "m", interval="1Hour"
    )
    print(time_series)
    print(f"Deleting level {level_id}")
    level_api.delete_location_level(level_id, office_id, effective_date)


if __name__ == "__main__":
    run_spec_level_examples()
    run_loc_level_examples()
