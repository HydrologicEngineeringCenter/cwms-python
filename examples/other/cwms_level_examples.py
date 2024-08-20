#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
import json
from datetime import datetime

import cwms

cwms.api.init_session(
    api_root="http://localhost:7001/swt-data/", api_key="apikey testkey"
)


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
    cwms.store_specified_level(specified_level, False)

    spec_level_dict = cwms.get_specified_levels("Top of Surcharge", "SPK")
    print(spec_level_dict.json)

    new_id = "Top of Surcharge2"
    print(f"Changing the name of specified level {specified_level['id']} to {new_id}")
    cwms.update_specified_level("Top of Surcharge", new_id, "SPK")
    spec_level_dict = cwms.get_specified_levels(new_id, "SPK").json
    print(spec_level_dict["id"])
    print(f"Deleting specified level id {new_id}")
    cwms.delete_specified_level("Top of Surcharge2", "SPK")
    spec_levels = cwms.get_specified_levels("Top of Surcharge*", "SPK")
    print(f"Confirming delete of spec id")
    print(spec_levels.json)


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

    print("Storing location TEST")
    cwms.store_location(data=location)
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
    cwms.store_location_level(level_dict)
    date_string = "1900-01-01T06:00:00"
    office_id = "SPK"
    effective_date = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S")
    level_id = level_dict["location-level-id"]
    level = cwms.get_location_level(level_id, office_id, effective_date)
    print(level)
    print(f"Retrieving level {level_id} as an hourly timeseries for the past day")
    time_series = cwms.get_level_as_timeseries(
        level_id, office_id, "m", interval="1Hour"
    )
    print(time_series)
    print(f"Deleting level {level_id}")
    cwms.delete_location_level(level_id, office_id, effective_date)


if __name__ == "__main__":
    run_spec_level_examples()
    run_loc_level_examples()
