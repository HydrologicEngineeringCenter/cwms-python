#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import unittest
from datetime import datetime

import pytz
import requests_mock

from cwms.core import CwmsApiSession
from cwms.timeseries.timeseries_txt import CwmsTextTs, DeleteMethod
from tests._test_utils import read_resource_file

_TEXT_TS_JSON = read_resource_file("texttimeseries.json")
_STD_TEXT_JSON = read_resource_file("standard_text.json")
_STS_TEXT_CAT_JSON = read_resource_file("standard_text_catalog.json")


class TestTextTs(unittest.TestCase):
    _MOCK_ROOT = "https://mockwebserver.cwms.gov"

    @requests_mock.Mocker()
    def test_retrieve_text_ts_json_default(self, m):
        m.get(
            f"{TestTextTs._MOCK_ROOT}"
            "/timeseries/text?office=SWT&name=TEST.Text.Inst.1Hour.0.MockTest&"
            "begin=2024-02-12T00%3A00%3A00-08%3A00&"
            "end=2020-02-12T02%3A00%3A00-08%3A00",
            json=_TEXT_TS_JSON,
        )
        cwms_text_ts = CwmsTextTs(CwmsApiSession(TestTextTs._MOCK_ROOT))
        timeseries_id = "TEST.Text.Inst.1Hour.0.MockTest"
        office_id = "SWT"
        timezone = pytz.timezone("US/Pacific")
        begin = timezone.localize(datetime(2024, 2, 12, 0, 0, 0))
        end = timezone.localize(datetime(2020, 2, 12, 2, 0, 0))
        timeseries = cwms_text_ts.retrieve_text_ts_json(
            timeseries_id, office_id, begin, end
        )
        self.assertEqual(_TEXT_TS_JSON, timeseries)

    @requests_mock.Mocker()
    def test_retrieve_text_ts_json(self, m):
        m.get(
            f"{TestTextTs._MOCK_ROOT}"
            "/timeseries/text?office=SWT&name=TEST.Text.Inst.1Hour.0.MockTest&"
            "begin=2024-02-12T00%3A00%3A00-08%3A00&"
            "end=2020-02-12T02%3A00%3A00-08%3A00",
            json=_TEXT_TS_JSON,
        )
        cwms_text_ts = CwmsTextTs(CwmsApiSession(TestTextTs._MOCK_ROOT))
        timeseries_id = "TEST.Text.Inst.1Hour.0.MockTest"
        office_id = "SWT"
        timezone = pytz.timezone("US/Pacific")
        begin = timezone.localize(datetime(2024, 2, 12, 0, 0, 0))
        end = timezone.localize(datetime(2020, 2, 12, 2, 0, 0))
        timeseries = cwms_text_ts.retrieve_text_ts_json(
            timeseries_id, office_id, begin, end
        )
        self.assertEqual(_TEXT_TS_JSON, timeseries)

    @requests_mock.Mocker()
    def tests_retrieve_large_clob(self, m):
        url = "https://example.com/large_clob"
        m.get(
            url,
            text="Example text data but short",
            headers={"content-type": "text/plain"},
        )

        cwms_text_ts = CwmsTextTs(CwmsApiSession(TestTextTs._MOCK_ROOT))
        clob_data = cwms_text_ts.retrieve_large_clob(url)

        self.assertEqual(type(clob_data), str)
        self.assertEqual(clob_data, "Example text data but short")

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
            "begin=2024-02-12T00%3A00%3A00-08%3A00&"
            "end=2020-02-12T02%3A00%3A00-08%3A00&"
            "text-mask=Hello%2C+World",
            json=_TEXT_TS_JSON,
        )
        cwms_text_ts = CwmsTextTs(CwmsApiSession(TestTextTs._MOCK_ROOT))
        level_id = "TEST.Text.Inst.1Hour.0.MockTest"
        office_id = "SWT"
        timezone = pytz.timezone("US/Pacific")
        begin = timezone.localize(datetime(2024, 2, 12, 0, 0, 0))
        end = timezone.localize(datetime(2020, 2, 12, 2, 0, 0))
        cwms_text_ts.delete_text_ts(
            level_id, office_id, begin, end, text_mask="Hello, World"
        )
        assert m.called
        assert m.call_count == 1

    @requests_mock.Mocker()
    def test_retrieve_std_text_json(self, m):
        m.get(
            f"{TestTextTs._MOCK_ROOT}"
            "/timeseries/text/standard-text-id/HW?office=SPK",
            json=_TEXT_TS_JSON,
        )
        cwms_text_ts = CwmsTextTs(CwmsApiSession(TestTextTs._MOCK_ROOT))
        text_id = "HW"
        office_id = "SPK"
        standard_txt = cwms_text_ts.retrieve_std_txt_json(text_id, office_id)
        self.assertEqual(_TEXT_TS_JSON, standard_txt)

    @requests_mock.Mocker()
    def test_retrieve_std_text_cat_json_default(self, m):
        m.get(
            f"{TestTextTs._MOCK_ROOT}" "/timeseries/text/standard-text-id",
            json=_TEXT_TS_JSON,
        )
        cwms_text_ts = CwmsTextTs(CwmsApiSession(TestTextTs._MOCK_ROOT))
        standard_txt = cwms_text_ts.retrieve_std_txt_cat_json()
        self.assertEqual(_TEXT_TS_JSON, standard_txt)

    @requests_mock.Mocker()
    def test_retrieve_std_text_cat_json(self, m):
        m.get(
            f"{TestTextTs._MOCK_ROOT}"
            "/timeseries/text/standard-text-id?text-id-mask=HW&office-id-mask=SPK",
            json=_TEXT_TS_JSON,
        )
        cwms_text_ts = CwmsTextTs(CwmsApiSession(TestTextTs._MOCK_ROOT))
        text_id = "HW"
        office_id = "SPK"
        catalog = cwms_text_ts.retrieve_std_txt_cat_json(text_id, office_id)
        self.assertEqual(_TEXT_TS_JSON, catalog)

    @requests_mock.Mocker()
    def test_store_std_text_json(self, m):
        m.post(
            f"{TestTextTs._MOCK_ROOT}"
            "/timeseries/text/standard-text-id?fail-if-exists=True"
        )
        cwms_text_ts = CwmsTextTs(CwmsApiSession(TestTextTs._MOCK_ROOT))
        cwms_text_ts.store_std_txt_json(_STD_TEXT_JSON, fail_if_exists=True)
        assert m.called
        assert m.call_count == 1

    @requests_mock.Mocker()
    def test_delete_std_text_json(self, m):
        m.delete(
            f"{TestTextTs._MOCK_ROOT}"
            "/timeseries/text/standard-text-id/HW?office=SPK&method=DELETE_ALL"
        )
        cwms_text_ts = CwmsTextTs(CwmsApiSession(TestTextTs._MOCK_ROOT))
        text_id = "HW"
        office_id = "SPK"
        cwms_text_ts.delete_std_txt(text_id, DeleteMethod.DELETE_ALL, office_id)
        assert m.called
        assert m.call_count == 1


if __name__ == "__main__":
    unittest.main()
