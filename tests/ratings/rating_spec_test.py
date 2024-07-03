#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import pandas as pd
import pytest
import pytz

import cwms.api
import cwms.ratings.ratings_spec as ratings_spec
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"
_RAT_SPEC_JSON = read_resource_file("rating_spec.json")
_RAT_SPECS_JSON = read_resource_file("rating_specs.json")


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root=_MOCK_ROOT)


def test_get_rating_spec_default(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/ratings/spec/TestRating.Stage;Flow.USGS-EXSA-TEST.USGS-NWIS-TEST?office=MVP",
        json=_RAT_SPEC_JSON,
    )

    rating_id = "TestRating.Stage;Flow.USGS-EXSA-TEST.USGS-NWIS-TEST?office=MVP"
    office_id = "MVP"

    spec = ratings_spec.get_rating_spec(rating_id=rating_id, office_id=office_id)

    assert spec.json == _RAT_SPEC_JSON
    assert type(spec.df) is pd.DataFrame


def test_get_rating_specs_default(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}" "/ratings/spec?office=MVP",
        json=_RAT_SPECS_JSON,
    )

    office_id = "MVP"

    specs = ratings_spec.get_rating_specs(office_id=office_id)

    assert specs.json == _RAT_SPECS_JSON
    assert type(specs.df) is pd.DataFrame
    assert specs.df.shape[0] == 4
