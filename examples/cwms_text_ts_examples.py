#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
import json
from datetime import datetime

import pytz

import CWMS._constants as constants
from CWMS.core import CwmsApiSession
from CWMS.cwms_text_ts import CwmsTextTs

session = CwmsApiSession("http://localhost:7000/spk-data/", "apikey testkey")
text_ts_api = CwmsTextTs(session)


def run_text_ts_examples():
    print("------Running through text ts examples-------")

    location = '''
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
        '''
    headers = {
        "Content-Type": constants.HEADER_JSON_V1
    }
    print("Storing location TEST")
    text_ts_api.get_session().post("locations", params=None,
                                   headers=headers, data=location)

    text_ts = json.loads(
        '''
        {
          "office-id": "SPK",
          "name": "TEST.Text.Inst.1Hour.0.MockTest",
          "regular-text-values": [
            {
              "date-time": "2024-02-12T00:00:00Z",
              "version-date": "2024-02-12T00:00:00Z",
              "data-entry-date": "2024-02-12T00:00:00Z",
              "attribute": 0,
              "text-value": "Hello, Davis"
            },
            {
              "date-time": "2024-02-12T01:00:00Z",
              "version-date": "2024-02-12T00:00:00Z",
              "data-entry-date": "2024-02-12T00:00:00Z",
              "text-value": "Hello, USA"
            }
          ]
        }
        ''')
    print(f"Storing text ts {text_ts['name']}")
    text_ts_api.store_text_ts_json(text_ts, False)

    timezone = pytz.timezone("UTC")
    begin = timezone.localize(datetime(2024, 2, 12, 0, 0, 0))
    end = timezone.localize(datetime(2024, 2, 12, 2, 0, 0))
    text_ts_dict = text_ts_api.retrieve_text_ts_json(
        text_ts['name'], "SPK", begin, end)
    print(text_ts_dict)

    print(f"Deleting text ts {text_ts['name']}")
    text_ts_api.delete_text_ts(text_ts['name'], "SPK", begin, end)
    text_ts_dict = text_ts_api.retrieve_text_ts_json(
        text_ts['name'], "SPK", begin, end)
    print(f"Confirming delete of text ts {text_ts['name']}")
    print(text_ts_dict)


if __name__ == "__main__":
    run_text_ts_examples()
