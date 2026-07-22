#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

from datetime import datetime

import pytest
import pytz

import cwms.api
import cwms.timeseries.timeseries_txt as timeseries
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"
_TEXT_TS_JSON = read_resource_file("texttimeseries.json")


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root=_MOCK_ROOT)


def test_get_text_timeseries_default(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/timeseries/text?office=SWT&name=TEST.Text.Inst.1Hour.0.MockTest&"
        "begin=2024-02-12T00%3A00%3A00-08%3A00&"
        "end=2020-02-12T02%3A00%3A00-08%3A00",
        json=_TEXT_TS_JSON,
    )

    timeseries_id = "TEST.Text.Inst.1Hour.0.MockTest"
    office_id = "SWT"
    timezone = pytz.timezone("US/Pacific")
    begin = timezone.localize(datetime(2024, 2, 12, 0, 0, 0))
    end = timezone.localize(datetime(2020, 2, 12, 2, 0, 0))

    data = timeseries.get_text_timeseries(timeseries_id, office_id, begin, end)
    assert data.json == _TEXT_TS_JSON


def test_get_text_timeseries(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/timeseries/text?office=SWT&name=TEST.Text.Inst.1Hour.0.MockTest&"
        "begin=2024-02-12T00%3A00%3A00-08%3A00&"
        "end=2020-02-12T02%3A00%3A00-08%3A00",
        json=_TEXT_TS_JSON,
    )

    timeseries_id = "TEST.Text.Inst.1Hour.0.MockTest"
    office_id = "SWT"
    timezone = pytz.timezone("US/Pacific")
    begin = timezone.localize(datetime(2024, 2, 12, 0, 0, 0))
    end = timezone.localize(datetime(2020, 2, 12, 2, 0, 0))

    data = timeseries.get_text_timeseries(timeseries_id, office_id, begin, end)
    assert data.json == _TEXT_TS_JSON


def test_create_text_timeseries(requests_mock):
    requests_mock.post(f"{_MOCK_ROOT}/timeseries/text?replace-all=True")

    data = _TEXT_TS_JSON
    timeseries.store_text_timeseries(data, True)

    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_delete_text_timeseries(requests_mock):
    requests_mock.delete(
        f"{_MOCK_ROOT}"
        "/timeseries/text/TEST.Text.Inst.1Hour.0.MockTest?office=SWT&"
        "begin=2024-02-12T00%3A00%3A00-08%3A00&"
        "end=2020-02-12T02%3A00%3A00-08%3A00&"
        "text-mask=Hello%2C+World",
        json=_TEXT_TS_JSON,
    )

    level_id = "TEST.Text.Inst.1Hour.0.MockTest"
    office_id = "SWT"
    timezone = pytz.timezone("US/Pacific")
    begin = timezone.localize(datetime(2024, 2, 12, 0, 0, 0))
    end = timezone.localize(datetime(2020, 2, 12, 2, 0, 0))

    timeseries.delete_text_timeseries(
        level_id,
        office_id,
        begin,
        end,
        text_mask="Hello, World",
    )

    assert requests_mock.called
    assert requests_mock.call_count == 1
