#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import pandas as pd
import pytest
import pytz

import cwms.api
import cwms.ratings.ratings as ratings
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"
_RATINGS_JSON = read_resource_file("ratings.json")
_RATINGS_REF_JSON = read_resource_file("ratings_reference.json")


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root=_MOCK_ROOT)


def test_get_ratings_default(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/ratings/TestRating.Stage;Flow.USGS-EXSA-TEST.USGS-NWIS-TEST?office=MVP&"
        "method=EAGER",
        json=_RATINGS_JSON,
    )

    office_id = "MVP"
    rating_id = "TestRating.Stage;Flow.USGS-EXSA-TEST.USGS-NWIS-TEST"

    specs = ratings.get_ratings(
        rating_id=rating_id, office_id=office_id, method="EAGER"
    )

    assert specs.json == _RATINGS_JSON
    assert type(specs.df) is pd.DataFrame
    assert specs.df.shape == (2, 9)


def test_ratings_df_to_json_default(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/ratings/TestRating.Stage;Flow.USGS-EXSA-TEST.USGS-NWIS-TEST?office=MVP&"
        "method=REFERENCE",
        json=_RATINGS_REF_JSON,
    )

    rating_json = {
        "rating-template": {
            "office-id": "MVP",
            "parameters-id": "Stage;Flow",
            "version": "USGS-EXSA-TEST",
            "ind-parameter-specs": {
                "ind-parameter-spec": {
                    "position": "1",
                    "parameter": "Stage",
                    "in-range-method": "LINEAR",
                    "out-range-low-method": "LINEAR",
                    "out-range-high-method": "LINEAR",
                }
            },
            "dep-parameter": "Flow",
            "description": "Expanded, Shift-Adjusted Stream Rating",
        },
        "rating-spec": {
            "office-id": "MVP",
            "rating-spec-id": "TestRating.Stage;Flow.USGS-EXSA-TEST.USGS-NWIS-TEST",
            "template-id": "Stage;Flow.USGS-EXSA-TEST",
            "location-id": "TestRating",
            "version": "USGS-NWIS-TEST",
            "source-agency": "USGS",
            "in-range-method": "LINEAR",
            "out-range-low-method": "NEAREST",
            "out-range-high-method": "NEAREST",
            "active": "true",
            "auto-update": "true",
            "auto-activate": "true",
            "auto-migrate-extension": "true",
            "ind-rounding-specs": {
                "ind-rounding-spec": {"position": "1", "element-value": "2223456782"}
            },
            "dep-rounding-spec": "2222233332",
            "description": "TESTing rating spec",
        },
        "simple-rating": {
            "office-id": "MVP",
            "rating-spec-id": "TestRating.Stage;Flow.USGS-EXSA-TEST.USGS-NWIS-TEST",
            "units-id": "ft;cfs",
            "effective-date": "2024-07-03T09:03:01.540655",
            "transition-start-date": None,
            "active": True,
            "description": "Testing Rating Curves.  Test Rating",
            "rating-points": {
                "point": [
                    {"ind": 1, "dep": 100},
                    {"ind": 2, "dep": 200},
                    {"ind": 3, "dep": 300},
                    {"ind": 4, "dep": 400},
                    {"ind": 5, "dep": 500},
                    {"ind": 6, "dep": 600},
                    {"ind": 7, "dep": 700},
                    {"ind": 8, "dep": 800},
                    {"ind": 9, "dep": 900},
                    {"ind": 10, "dep": 1000},
                ]
            },
        },
    }

    rating_table_df = pd.DataFrame(
        {
            "ind": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "dep": [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
        }
    )

    rating_table_json = ratings.rating_simple_df_to_json(
        data=rating_table_df,
        rating_id="TestRating.Stage;Flow.USGS-EXSA-TEST.USGS-NWIS-TEST",
        office_id="MVP",
        units="ft;cfs",
        effective_date=pd.to_datetime("2024-07-03T09:03:01.540655"),
        transition_start_date=None,
        description="Testing Rating Curves.  Test Rating",
    )

    assert rating_table_json == rating_json
    assert rating_table_df.equals(
        pd.DataFrame(rating_table_json["simple-rating"]["rating-points"]["point"])
    )
