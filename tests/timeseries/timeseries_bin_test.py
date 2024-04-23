#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

from datetime import datetime

import pytest
import pytz

import cwms.api
import cwms.timeseries.timeseries_bin as timeseries
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"
_BIN_TS_JSON = read_resource_file("binarytimeseries.json")


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root=_MOCK_ROOT)


def test_get_binary_timeseries_default(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/timeseries/binary?office=SPK&name=TEST.Binary.Inst.1Hour.0.MockTest&"
        "begin=2024-02-12T00%3A00%3A00-08%3A00&"
        "end=2020-02-12T02%3A00%3A00-08%3A00",
        json=_BIN_TS_JSON,
    )

    timeseries_id = "TEST.Binary.Inst.1Hour.0.MockTest"
    office_id = "SPK"
    timezone = pytz.timezone("US/Pacific")
    begin = timezone.localize(datetime(2024, 2, 12, 0, 0, 0))
    end = timezone.localize(datetime(2020, 2, 12, 2, 0, 0))

    data = timeseries.get_binary_timeseries(timeseries_id, office_id, begin, end)
    assert data.json == _BIN_TS_JSON


def test_get_binary_timeseries(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/timeseries/binary?office=SPK&name=TEST.Binary.Inst.1Hour.0.MockTest&"
        "begin=2024-02-12T00%3A00%3A00-08%3A00&"
        "end=2020-02-12T02%3A00%3A00-08%3A00&"
        "binary-type-mask=text%2Fplain",
        json=_BIN_TS_JSON,
    )

    timeseries_id = "TEST.Binary.Inst.1Hour.0.MockTest"
    office_id = "SPK"
    timezone = pytz.timezone("US/Pacific")
    begin = timezone.localize(datetime(2024, 2, 12, 0, 0, 0))
    end = timezone.localize(datetime(2020, 2, 12, 2, 0, 0))

    data = timeseries.get_binary_timeseries(
        timeseries_id, office_id, begin, end, bin_type_mask="text/plain"
    )
    assert data.json == _BIN_TS_JSON


"""
def tests_retrieve_large_blob(m):
    url = "https://example.com/large_blob"
    m.get(
        url,
        text="Example byte data but short",
        headers={"content-type": "application/octet-stream"},
    )

    blob_data = timeseries.get_large_blob(url)

    assert isinstance(blob_data, bytes)
    assert blob_data == b"Example byte data but short"
"""


def test_create_binary_timeseries(requests_mock):
    requests_mock.post(f"{_MOCK_ROOT}/timeseries/binary?replace-all=True")

    data = _BIN_TS_JSON
    timeseries.store_binary_timeseries(data, True)

    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_delete_binary_timeseries(requests_mock):
    requests_mock.delete(
        f"{_MOCK_ROOT}"
        "/timeseries/binary/TEST.Binary.Inst.1Hour.0.MockTest?office=SPK&"
        "begin=2024-02-12T00%3A00%3A00-08%3A00&"
        "end=2020-02-12T02%3A00%3A00-08%3A00&"
        "binary-type-mask=text%2Fplain",
        json=_BIN_TS_JSON,
    )

    level_id = "TEST.Binary.Inst.1Hour.0.MockTest"
    office_id = "SPK"
    timezone = pytz.timezone("US/Pacific")
    begin = timezone.localize(datetime(2024, 2, 12, 0, 0, 0))
    end = timezone.localize(datetime(2020, 2, 12, 2, 0, 0))

    timeseries.delete_binary_timeseries(
        level_id, office_id, begin, end, bin_type_mask="text/plain"
    )

    assert requests_mock.called
    assert requests_mock.call_count == 1
