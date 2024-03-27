#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import unittest
from datetime import datetime

import pytz
import requests_mock

from cwms.core import CwmsApiSession
from cwms.timeseries.timeseries import CwmsTs
from tests._test_utils import read_resource_file

_VERS_TS_JSON = read_resource_file("versioned_num_ts.json")
_UNVERS_TS_JSON = read_resource_file("unversioned_num_ts.json")


class TestTs(unittest.TestCase):
    _MOCK_ROOT = "https://mockwebserver.cwms.gov"

    @requests_mock.Mocker()
    def test_retrieve_unversioned_ts_json_default(self, m):
        m.get(
            f"{TestTs._MOCK_ROOT}"
            "/timeseries?office=SWT&"
            "name=TEST.Text.Inst.1Hour.0.MockTest&"
            "unit=EN&"
            "begin=2008-05-01T15%3A00%3A00%2B00%3A00&"
            "end=2008-05-01T17%3A00%3A00%2B00%3A00&"
            "page-size=500000",
            json=_UNVERS_TS_JSON,
        )
        cwms_ts = CwmsTs(CwmsApiSession(TestTs._MOCK_ROOT))
        timeseries_id = "TEST.Text.Inst.1Hour.0.MockTest"
        office_id = "SWT"

        # explicitly format begin and end dates with default timezone as an example
        timezone = pytz.timezone("UTC")
        begin = timezone.localize(datetime(2008, 5, 1, 15, 0, 0))
        end = timezone.localize(datetime(2008, 5, 1, 17, 0, 0))

        timeseries = cwms_ts.retrieve_ts_json(
            tsId=timeseries_id, office_id=office_id, begin=begin, end=end
        )
        self.assertEqual(_UNVERS_TS_JSON, timeseries)

    @requests_mock.Mocker()
    def test_store_unversioned_ts_json_default(self, m):
        m.post(
            f"{TestTs._MOCK_ROOT}/timeseries?"
            f"create-as-lrts=False&"
            f"override-protection=False"
        )
        cwms_ts = CwmsTs(CwmsApiSession(TestTs._MOCK_ROOT))
        data = _UNVERS_TS_JSON
        cwms_ts.write_ts(data=data)
        assert m.called
        assert m.call_count == 1

    @requests_mock.Mocker()
    def test_retrieve_versioned_ts_json_default(self, m):
        m.get(
            f"{TestTs._MOCK_ROOT}"
            "/timeseries?office=SWT&"
            "name=TEST.Text.Inst.1Hour.0.MockTest&"
            "unit=EN&"
            "begin=2008-05-01T15%3A00%3A00%2B00%3A00&"
            "end=2008-05-01T17%3A00%3A00%2B00%3A00&"
            "page-size=500000&"
            "version-date=2021-06-20T08%3A00%3A00%2B00%3A00",
            json=_VERS_TS_JSON,
        )
        cwms_ts = CwmsTs(CwmsApiSession(TestTs._MOCK_ROOT))
        timeseries_id = "TEST.Text.Inst.1Hour.0.MockTest"
        office_id = "SWT"

        # explicitly format begin and end dates with default timezone as an example
        timezone = pytz.timezone("UTC")
        begin = timezone.localize(datetime(2008, 5, 1, 15, 0, 0))
        end = timezone.localize(datetime(2008, 5, 1, 17, 0, 0))
        version_date = timezone.localize(datetime(2021, 6, 20, 8, 0, 0))

        timeseries = cwms_ts.retrieve_ts_json(
            tsId=timeseries_id,
            office_id=office_id,
            begin=begin,
            end=end,
            version_date=version_date,
        )
        self.assertEqual(_VERS_TS_JSON, timeseries)

    @requests_mock.Mocker()
    def test_store_versioned_ts_json_default(self, m):
        m.post(
            f"{TestTs._MOCK_ROOT}/timeseries?"
            f"create-as-lrts=False&"
            f"override-protection=False"
        )
        cwms_ts = CwmsTs(CwmsApiSession(TestTs._MOCK_ROOT))
        data = _VERS_TS_JSON
        cwms_ts.write_ts(data=data)
        assert m.called
        assert m.call_count == 1
