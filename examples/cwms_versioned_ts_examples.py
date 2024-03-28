#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
import json
from datetime import datetime, timedelta

import pytz

import cwms._constants as constants
from cwms.core import CwmsApiSession
from cwms.timeseries.timeseries import CwmsTs

session = CwmsApiSession("http://localhost:7001/swt-data/", "apikey testkey")
ts_api = CwmsTs(session)


def run_versioned_ts_examples():
    print("------Running through versioned ts examples-------")

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
          "bounding-office-id": "SWT",
          "office-id": "SWT"
        }
        """
    headers = {"Content-Type": constants.HEADER_JSON_V2}
    print("Storing location TEST")
    ts_api.get_session().post("locations", params=None, headers=headers, data=location)

    versioned_ts = json.loads(
        """
        {
          "office-id": "SWT",
          "name": "TEST.Flow.Inst.1Hour.0.MockTest",
          "units": "CFS",
          "version-date": "2021-06-20T08:00:00-0700[-07:00]",
          "values": [
            [
              1209679200000,
              4,
              0
            ],
            [
              1209682800000,
              4,
              0
            ],
            [
              1209686400000,
              4,
              0
            ],
            [
              1209690000000,
              3,
              0
            ]
          ]
        }
        """
    )
    print(f"Storing versioned ts {versioned_ts['name']}")
    ts_api.write_ts(data=versioned_ts)

    timezone = pytz.timezone("US/Pacific")
    begin = timezone.localize(datetime(2008, 5, 1, 15, 0, 0))
    print()
    end = timezone.localize(datetime(2008, 5, 1, 18, 0, 0))
    version_date = timezone.localize(datetime(2021, 6, 20, 8, 0, 0))
    print(version_date)

    versioned_ts_dict = ts_api.retrieve_ts_json(
        tsId=versioned_ts["name"],
        office_id="SWT",
        begin=begin,
        end=end,
        version_date=version_date,
    )
    print(versioned_ts_dict)


def run_unversioned_ts_examples():
    print("------Running through versioned ts examples-------")

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
          "bounding-office-id": "SWT",
          "office-id": "SWT"
        }
        """
    headers = {"Content-Type": constants.HEADER_JSON_V2}
    print("Storing location TEST")
    ts_api.get_session().post("locations", params=None, headers=headers, data=location)

    unversioned_ts = json.loads(
        """
        {
          "office-id": "SWT",
          "name": "TEST.Flow.Inst.1Hour.0.MockTestUnversioned",
          "units": "CFS",
          "values": [
            [
              1209679200000,
              4,
              0
            ],
            [
              1209682800000,
              4,
              0
            ],
            [
              1209686400000,
              4,
              0
            ],
            [
              1209690000000,
              3,
              0
            ]
          ]
        }
        """
    )
    print(f"Storing unversioned ts {unversioned_ts['name']}")
    ts_api.write_ts(data=unversioned_ts)

    timezone = pytz.timezone("US/Pacific")
    begin = timezone.localize(datetime(2008, 5, 1, 15, 0, 0))
    end = timezone.localize(datetime(2008, 5, 1, 18, 0, 0))

    unversioned_ts_dict = ts_api.retrieve_ts_json(
        tsId=unversioned_ts["name"], office_id="SWT", begin=begin, end=end
    )
    print(unversioned_ts_dict)


def run_max_agg_ts_examples():
    print("------Running through versioned ts examples-------")

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
              "bounding-office-id": "SWT",
              "office-id": "SWT"
            }
            """
    headers = {"Content-Type": constants.HEADER_JSON_V2}
    print("Storing location TEST")
    ts_api.get_session().post("locations", params=None, headers=headers, data=location)

    versioned_ts = json.loads(
        """
        {
          "office-id": "SWT",
          "name": "TEST.Flow.Inst.1Hour.0.MockTestMaxAgg",
          "units": "CFS",
          "version-date": "2021-06-20T08:00:00-0700[-07:00]",
          "values": [
            [
              1209679200000,
              4,
              0
            ],
            [
              1209682800000,
              3,
              0
            ],
            [
              1209686400000,
              2,
              0
            ]
          ]
        }
        """
    )
    print(f"Storing versioned ts 1 {versioned_ts['name']}")
    ts_api.write_ts(data=versioned_ts)

    versioned_ts2 = json.loads(
        """
        {
          "office-id": "SWT",
          "name": "TEST.Flow.Inst.1Hour.0.MockTestMaxAgg",
          "units": "CFS",
          "version-date": "2021-06-21T08:00:00-0000[-07:00]",
          "values": [
            [
              1209690000000,
              1,
              0
            ]
          ]
        }
        """
    )

    print(f"Storing versioned ts 2 {versioned_ts2['name']}")
    ts_api.write_ts(data=versioned_ts2)

    timezone = pytz.timezone("US/Pacific")
    begin = timezone.localize(datetime(2008, 5, 1, 15, 0, 0))
    end = timezone.localize(datetime(2008, 5, 1, 18, 0, 0))

    max_agg_ts_dict = ts_api.retrieve_ts_json(
        tsId=versioned_ts["name"], office_id="SWT", begin=begin, end=end
    )
    print(max_agg_ts_dict)


if __name__ == "__main__":
    run_versioned_ts_examples()
    run_unversioned_ts_examples()
    run_max_agg_ts_examples()
