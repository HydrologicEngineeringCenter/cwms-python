#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
import json
from datetime import datetime

import pytz

import cwms

cwms.api.init_session(
    api_root="http://localhost:7001/swt-data/", api_key="apikey testkey"
)


def run_bin_ts_examples():
    print("------Running through text ts examples-------")

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
    cwms.store_location(data=location)

    bin_ts = json.loads(
        """
        {
          "office-id": "SPK",
          "name": "TEST.Binary.Inst.1Hour.0.MockTest",
          "interval-offset": 0,
          "time-zone": "America/Los_Angeles",
          "version-date": "2024-02-12T00:00:00Z",
          "binary-values": [
            {
              "date-time": "2024-02-12T00:00:00Z",
              "data-entry-date": "2024-02-12T00:00:00Z",
              "media-type": "image/png",
              "filename": "test.png",
              "quality": 0,
              "binary-value": [
                72,
                101,
                108,
                108,
                111,
                44,
                32,
                87,
                111,
                114,
                108,
                100
              ]
            }
          ]
        }
        """
    )
    print(f"Storing text ts {bin_ts['name']}")
    cwms.store_binary_timeseries(bin_ts, False)

    timezone = pytz.timezone("UTC")
    begin = timezone.localize(datetime(2024, 2, 12, 0, 0, 0))
    end = timezone.localize(datetime(2024, 2, 12, 2, 0, 0))
    bin_ts_dict = cwms.get_binary_timeseries(bin_ts["name"], "SPK", begin, end)
    print(bin_ts_dict.json)

    print(f"Deleting text ts {bin_ts['name']}")
    cwms.delete_binary_timeseries(bin_ts["name"], "SPK", begin, end)
    bin_ts_dict = cwms.get_binary_timeseries(bin_ts["name"], "SPK", begin, end)
    print(f"Confirming delete of text ts {bin_ts['name']}")
    print(bin_ts_dict.json)


if __name__ == "__main__":
    run_bin_ts_examples()
