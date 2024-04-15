#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import unittest
from datetime import datetime

import requests_mock

from cwms.core import CwmsApiSession
from cwms.forecast.forecast_instance import CwmsForecastInstance
from tests._test_utils import read_resource_file

_FORECAST_INSTANCES_JSON = read_resource_file("forecast_instances.json")
_FORECAST_INSTANCE_JSON = read_resource_file("forecast_instance.json")


class TestForecastInstance(unittest.TestCase):
    _MOCK_ROOT = "https://mockwebserver.cwms.gov"

    @requests_mock.Mocker()
    def test_retrieve_forecast_instances_json(self, m):
        m.get(
            f"{TestForecastInstance._MOCK_ROOT}/forecast-instance?office=SWT"
            f"&name=test-spec&designator=designator",
            json=_FORECAST_INSTANCES_JSON,
        )
        session = CwmsApiSession(TestForecastInstance._MOCK_ROOT)
        cwms_forecast = CwmsForecastInstance(session)
        levels = cwms_forecast.retrieve_forecast_instances_json(
            "test-spec", "SWT", "designator"
        )
        self.assertEqual(_FORECAST_INSTANCES_JSON, levels)

    @requests_mock.Mocker()
    def test_retrieve_forecast_instance_json(self, m):
        m.get(
            f"{TestForecastInstance._MOCK_ROOT}/forecast-instance/test-spec?"
            f"office=SWT&designator=designator"
            f"&forecast-date=2021-06-21T14%3A00%3A10"
            f"&issue-date=2022-05-22T12%3A03%3A40",
            json=_FORECAST_INSTANCES_JSON,
        )
        session = CwmsApiSession(TestForecastInstance._MOCK_ROOT)
        cwms_forecast = CwmsForecastInstance(session)
        forecast_date = datetime.utcfromtimestamp(1624284010000 / 1000)
        issue_date = datetime.utcfromtimestamp(1653221020000 / 1000)
        levels = cwms_forecast.retrieve_forecast_instance_json(
            "test-spec", "SWT", "designator", forecast_date, issue_date
        )
        self.assertEqual(_FORECAST_INSTANCES_JSON, levels)

    @requests_mock.Mocker()
    def test_store_forecast_instance_json(self, m):
        m.post(
            f"{TestForecastInstance._MOCK_ROOT}/forecast-instance",
            status_code=200,
            json=_FORECAST_INSTANCE_JSON,
        )
        session = CwmsApiSession(TestForecastInstance._MOCK_ROOT)
        cwms_forecast = CwmsForecastInstance(session)
        cwms_forecast.store_forecast_instance_json(_FORECAST_INSTANCE_JSON)
        assert m.called
        assert m.call_count == 1

    @requests_mock.Mocker()
    def test_delete_forecast_instance_json(self, m):
        m.delete(
            f"{TestForecastInstance._MOCK_ROOT}/forecast-instance/test-spec?"
            f"office=SWT&designator=designator"
            f"&forecast-date=2021-06-21T14%3A00%3A10"
            f"&issue-date=2022-05-22T12%3A03%3A40",
            status_code=200,
        )
        session = CwmsApiSession(TestForecastInstance._MOCK_ROOT)
        cwms_forecast = CwmsForecastInstance(session)
        forecast_date = datetime.utcfromtimestamp(1624284010000 / 1000)
        issue_date = datetime.utcfromtimestamp(1653221020000 / 1000)
        cwms_forecast.delete_forecast_instance(
            "test-spec", "SWT", "designator", forecast_date, issue_date
        )
        assert m.called
        assert m.call_count == 1


if __name__ == "__main__":
    unittest.main()
