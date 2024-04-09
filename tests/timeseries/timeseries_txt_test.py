#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

from datetime import datetime

import pytest
import pytz

import cwms.api
import cwms.timeseries.timeseries_txt as timeseries
from cwms.timeseries.timeseries_txt import DeleteMethod, TextTsMode
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"
_TEXT_TS_JSON = read_resource_file("texttimeseries.json")
_STD_TEXT_JSON = read_resource_file("standard_text.json")
_STS_TEXT_CAT_JSON = read_resource_file("standard_text_catalog.json")


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root=_MOCK_ROOT)


def test_get_text_timeseries_json_default(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/timeseries/text?office=SWT&name=TEST.Text.Inst.1Hour.0.MockTest&"
        "begin=2024-02-12T00%3A00%3A00-08%3A00&"
        "end=2020-02-12T02%3A00%3A00-08%3A00&mode=REGULAR",
        json=_TEXT_TS_JSON,
    )

    timeseries_id = "TEST.Text.Inst.1Hour.0.MockTest"
    office_id = "SWT"
    timezone = pytz.timezone("US/Pacific")
    begin = timezone.localize(datetime(2024, 2, 12, 0, 0, 0))
    end = timezone.localize(datetime(2020, 2, 12, 2, 0, 0))

    data = timeseries.get_text_timeseries_json(timeseries_id, office_id, begin, end)
    assert data == _TEXT_TS_JSON


def test_get_text_timeseries_json(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/timeseries/text?office=SWT&name=TEST.Text.Inst.1Hour.0.MockTest&"
        "min-attribute=-1000&max-attribute=1000.0&"
        "begin=2024-02-12T00%3A00%3A00-08%3A00&"
        "end=2020-02-12T02%3A00%3A00-08%3A00&mode=STANDARD",
        json=_TEXT_TS_JSON,
    )

    timeseries_id = "TEST.Text.Inst.1Hour.0.MockTest"
    office_id = "SWT"
    timezone = pytz.timezone("US/Pacific")
    begin = timezone.localize(datetime(2024, 2, 12, 0, 0, 0))
    end = timezone.localize(datetime(2020, 2, 12, 2, 0, 0))

    data = timeseries.get_text_timeseries_json(
        timeseries_id, office_id, begin, end, TextTsMode.STANDARD, -1000, 1000.0
    )
    assert data == _TEXT_TS_JSON


def test_create_text_timeseries_json(requests_mock):
    requests_mock.post(f"{_MOCK_ROOT}/timeseries/text?replace-all=True")

    data = _TEXT_TS_JSON
    timeseries.create_text_timeseries_json(data, True)

    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_delete_text_timeseries(requests_mock):
    requests_mock.delete(
        f"{_MOCK_ROOT}"
        "/timeseries/text/TEST.Text.Inst.1Hour.0.MockTest?office=SWT&"
        "min-attribute=-999.9&max-attribute=999&"
        "begin=2024-02-12T00%3A00%3A00-08%3A00&"
        "end=2020-02-12T02%3A00%3A00-08%3A00&mode=STANDARD&"
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
        TextTsMode.STANDARD,
        "Hello, World",
        -999.9,
        999,
    )

    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_get_standard_text_json(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}" "/timeseries/text/standard-text-id/HW?office=SPK",
        json=_TEXT_TS_JSON,
    )

    text_id = "HW"
    office_id = "SPK"

    data = timeseries.get_standard_text_json(text_id, office_id)
    assert data == _TEXT_TS_JSON


def test_get_standard_text_catalog_json_default(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}" "/timeseries/text/standard-text-id",
        json=_TEXT_TS_JSON,
    )

    data = timeseries.get_standard_text_catalog_json()
    assert data == _TEXT_TS_JSON


def test_get_standard_text_catalog_json(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/timeseries/text/standard-text-id?text-id-mask=HW&office-id-mask=SPK",
        json=_TEXT_TS_JSON,
    )

    text_id = "HW"
    office_id = "SPK"

    data = timeseries.get_standard_text_catalog_json(text_id, office_id)
    assert data == _TEXT_TS_JSON


def test_create_standard_text_json(requests_mock):
    requests_mock.post(
        f"{_MOCK_ROOT}" "/timeseries/text/standard-text-id?fail-if-exists=True"
    )

    timeseries.create_standard_text_json(_STD_TEXT_JSON, fail_if_exists=True)

    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_delete_standard_text(requests_mock):
    requests_mock.delete(
        f"{_MOCK_ROOT}"
        "/timeseries/text/standard-text-id/HW?office=SPK&method=DELETE_ALL"
    )

    text_id = "HW"
    office_id = "SPK"

    timeseries.delete_standard_text(text_id, DeleteMethod.DELETE_ALL, office_id)

    assert requests_mock.called
    assert requests_mock.call_count == 1
