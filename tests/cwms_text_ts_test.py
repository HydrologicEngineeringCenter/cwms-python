#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import unittest
from datetime import datetime

import pytz
import requests_mock

from CWMS.core import CwmsApiSession
from CWMS.cwms_text_ts import CwmsTextTs, TextTsMode
from ._test_utils import read_resource_file

_TEXT_TS_JSON = read_resource_file("texttimeseries.json")


class TestTextTs(unittest.TestCase):
    _MOCK_ROOT = "https://mockwebserver.cwms.gov"

    @requests_mock.Mocker()
    def test_retrieve_text_ts_json_default(self, m):
        m.get(
            f"{TestTextTs._MOCK_ROOT}"
            "/timeseries/text?office=SWT&name=TEST.Text.Inst.1Hour.0.MockTest&"
            "begin=2024-02-12T00%3A00%3A00-08%3A00&"
            "end=2020-02-12T02%3A00%3A00-08%3A00&mode=REGULAR",
            json=_TEXT_TS_JSON)
        cwms_text_ts = CwmsTextTs(CwmsApiSession(TestTextTs._MOCK_ROOT))
        timeseries_id = "TEST.Text.Inst.1Hour.0.MockTest"
        office_id = "SWT"
        timezone = pytz.timezone("US/Pacific")
        begin = timezone.localize(datetime(2024, 2, 12, 0, 0, 0))
        end = timezone.localize(datetime(2020, 2, 12, 2, 0, 0))
        timeseries = cwms_text_ts.retrieve_text_ts_json(timeseries_id,
                                                        office_id,
                                                        begin, end)
        self.assertEqual(_TEXT_TS_JSON, timeseries)

    @requests_mock.Mocker()
    def test_retrieve_text_ts_json(self, m):
        m.get(
            f"{TestTextTs._MOCK_ROOT}"
            "/timeseries/text?office=SWT&name=TEST.Text.Inst.1Hour.0.MockTest&"
            "min-attribute=-1000&max-attribute=1000.0&"
            "begin=2024-02-12T00%3A00%3A00-08%3A00&"
            "end=2020-02-12T02%3A00%3A00-08%3A00&mode=STANDARD",
            json=_TEXT_TS_JSON)
        cwms_text_ts = CwmsTextTs(CwmsApiSession(TestTextTs._MOCK_ROOT))
        timeseries_id = "TEST.Text.Inst.1Hour.0.MockTest"
        office_id = "SWT"
        timezone = pytz.timezone("US/Pacific")
        begin = timezone.localize(datetime(2024, 2, 12, 0, 0, 0))
        end = timezone.localize(datetime(2020, 2, 12, 2, 0, 0))
        timeseries = cwms_text_ts.retrieve_text_ts_json(timeseries_id,
                                                        office_id,
                                                        begin, end,
                                                        TextTsMode.STANDARD,
                                                        -1000, 1000.0)
        self.assertEqual(_TEXT_TS_JSON, timeseries)

    @requests_mock.Mocker()
    def test_store_text_ts_json(self, m):
        m.post(f"{TestTextTs._MOCK_ROOT}/timeseries/text?replace-all=True")
        cwms_text_ts = CwmsTextTs(CwmsApiSession(TestTextTs._MOCK_ROOT))
        data = _TEXT_TS_JSON
        cwms_text_ts.store_text_ts_json(data, True)
        assert m.called
        assert m.call_count == 1

    @requests_mock.Mocker()
    def test_delete_text_ts(self, m):
        m.delete(
            f"{TestTextTs._MOCK_ROOT}"
            "/timeseries/text/TEST.Text.Inst.1Hour.0.MockTest?office=SWT&"
            "min-attribute=-999.9&max-attribute=999&"
            "begin=2024-02-12T00%3A00%3A00-08%3A00&"
            "end=2020-02-12T02%3A00%3A00-08%3A00&mode=STANDARD&"
            "text-mask=Hello%2C+World",
            json=_TEXT_TS_JSON)
        cwms_text_ts = CwmsTextTs(CwmsApiSession(TestTextTs._MOCK_ROOT))
        level_id = "TEST.Text.Inst.1Hour.0.MockTest"
        office_id = "SWT"
        timezone = pytz.timezone("US/Pacific")
        begin = timezone.localize(datetime(2024, 2, 12, 0, 0, 0))
        end = timezone.localize(datetime(2020, 2, 12, 2, 0, 0))
        cwms_text_ts.delete_text_ts(
            level_id, office_id, begin, end, TextTsMode.STANDARD, "Hello, World", -999.9, 999)
        assert m.called
        assert m.call_count == 1


if __name__ == "__main__":
    unittest.main()
