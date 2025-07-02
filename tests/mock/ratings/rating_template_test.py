#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import pandas as pd
import pytest
import pytz

import cwms.api
import cwms.ratings.ratings_template as ratings_template
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"
_RAT_TEMP_JSON = read_resource_file("rating_template.json")
_RAT_TEMPS_JSON = read_resource_file("rating_templates.json")


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root=_MOCK_ROOT)


def test_get_rating_template_default(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}" "/ratings/template/Stage;Flow.USGS-EXSA-TEST?office=MVP",
        json=_RAT_TEMP_JSON,
    )

    rating_temp = "Stage;Flow.USGS-EXSA-TEST"
    office_id = "MVP"

    template = ratings_template.get_rating_template(
        template_id=rating_temp, office_id=office_id
    )

    assert template.json == _RAT_TEMP_JSON
    assert type(template.df) is pd.DataFrame


def test_get_rating_templates_default(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}" "/ratings/template?office=MVP",
        json=_RAT_TEMPS_JSON,
    )

    office_id = "MVP"

    templates = ratings_template.get_rating_templates(office_id=office_id)

    assert templates.json == _RAT_TEMPS_JSON
    assert type(templates.df) is pd.DataFrame
    assert templates.df.shape[0] == 5
