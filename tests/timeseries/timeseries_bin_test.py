#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import unittest
from datetime import datetime

import pytz
import requests_mock

from cwms.core import CwmsApiSession
from cwms.timeseries.timeseries_bin import CwmsBinTs
from tests._test_utils import read_resource_file

_BIN_TS_JSON = read_resource_file("binarytimeseries.json")


class TestBinTs(unittest.TestCase):
    _MOCK_ROOT = "https://mockwebserver.cwms.gov"

    @requests_mock.Mocker()
    def test_retrieve_bin_ts_json_default(self, m):
        m.get(
            f"{TestBinTs._MOCK_ROOT}"
            "/timeseries/binary?office=SPK&name=TEST.Binary.Inst.1Hour.0.MockTest&"
            "begin=2024-02-12T00%3A00%3A00-08%3A00&"
            "end=2020-02-12T02%3A00%3A00-08%3A00",
            json=_BIN_TS_JSON)
        cwms_bin_ts = CwmsBinTs(CwmsApiSession(TestBinTs._MOCK_ROOT))
        timeseries_id = "TEST.Binary.Inst.1Hour.0.MockTest"
        office_id = "SPK"
        timezone = pytz.timezone("US/Pacific")
        begin = timezone.localize(datetime(2024, 2, 12, 0, 0, 0))
        end = timezone.localize(datetime(2020, 2, 12, 2, 0, 0))
        timeseries = cwms_bin_ts.retrieve_bin_ts_json(timeseries_id,
                                                      office_id,
                                                      begin, end)
        self.assertEqual(_BIN_TS_JSON, timeseries)

    @requests_mock.Mocker()
    def test_retrieve_bin_ts_json(self, m):
        m.get(
            f"{TestBinTs._MOCK_ROOT}"
            "/timeseries/binary?office=SPK&name=TEST.Binary.Inst.1Hour.0.MockTest&"
            "min-attribute=-1000&max-attribute=1000.0&"
            "begin=2024-02-12T00%3A00%3A00-08%3A00&"
            "end=2020-02-12T02%3A00%3A00-08%3A00&"
            "binary-type-mask=text%2Fplain",
            json=_BIN_TS_JSON)
        cwms_bin_ts = CwmsBinTs(CwmsApiSession(TestBinTs._MOCK_ROOT))
        timeseries_id = "TEST.Binary.Inst.1Hour.0.MockTest"
        office_id = "SPK"
        timezone = pytz.timezone("US/Pacific")
        begin = timezone.localize(datetime(2024, 2, 12, 0, 0, 0))
        end = timezone.localize(datetime(2020, 2, 12, 2, 0, 0))
        timeseries = cwms_bin_ts.retrieve_bin_ts_json(timeseries_id,
                                                      office_id,
                                                      begin, end, "text/plain",
                                                      -1000, 1000.0)
        self.assertEqual(_BIN_TS_JSON, timeseries)

    @requests_mock.Mocker()
    def test_store_bin_ts_json(self, m):
        m.post(f"{TestBinTs._MOCK_ROOT}/timeseries/binary?replace-all=True")
        cwms_bin_ts = CwmsBinTs(CwmsApiSession(TestBinTs._MOCK_ROOT))
        data = _BIN_TS_JSON
        cwms_bin_ts.store_bin_ts_json(data, True)
        assert m.called
        assert m.call_count == 1

    @requests_mock.Mocker()
    def tests_retrieve_large_blob(self, m):
        url = 'https://example.com/large_blob'
        m.get(url, text='Example byte data but short', headers={'content-type': 'application/octet-stream'})

        cwms_bin_ts = CwmsBinTs(CwmsApiSession(TestBinTs._MOCK_ROOT))
        blob_data = cwms_bin_ts.retrieve_large_blob(url)

        self.assertEqual(type(blob_data), bytes)
        self.assertEqual(blob_data, b'Example byte data but short')

    @requests_mock.Mocker()
    def test_delete_bin_ts(self, m):
        m.delete(
            f"{TestBinTs._MOCK_ROOT}"
            "/timeseries/binary/TEST.Binary.Inst.1Hour.0.MockTest?office=SPK&"
            "min-attribute=-999.9&max-attribute=999&"
            "begin=2024-02-12T00%3A00%3A00-08%3A00&"
            "end=2020-02-12T02%3A00%3A00-08%3A00&"
            "binary-type-mask=text%2Fplain",
            json=_BIN_TS_JSON)
        cwms_bin_ts = CwmsBinTs(CwmsApiSession(TestBinTs._MOCK_ROOT))
        level_id = "TEST.Binary.Inst.1Hour.0.MockTest"
        office_id = "SPK"
        timezone = pytz.timezone("US/Pacific")
        begin = timezone.localize(datetime(2024, 2, 12, 0, 0, 0))
        end = timezone.localize(datetime(2020, 2, 12, 2, 0, 0))
        cwms_bin_ts.delete_bin_ts(
            level_id, office_id, begin, end,
            "text/plain", -999.9, 999)
        assert m.called
        assert m.call_count == 1


if __name__ == "__main__":
    unittest.main()
