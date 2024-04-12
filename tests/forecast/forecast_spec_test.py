#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import unittest

import requests_mock

from cwms.core import CwmsApiSession
from cwms.forecast.forecast_spec import CwmsForecastSpec
from tests._test_utils import read_resource_file

_FORECAST_SPECS_JSON = read_resource_file("forecast_specs.json")
_FORECAST_SPEC_JSON = read_resource_file("forecast_spec.json")


class TestForecastSpec(unittest.TestCase):
    _MOCK_ROOT = "https://mockwebserver.cwms.gov"

    @requests_mock.Mocker()
    def test_retrieve_forecast_specs_json(self, m):
        m.get(
            f"{TestForecastSpec._MOCK_ROOT}/forecast-spec?office=office_mask"
            f"&id_mask=id_mask&designator-mask=designator_mask"
            f"&location-mask=location_mask"
            f"&source-entity=source_entity",
            json=_FORECAST_SPECS_JSON,
        )
        session = CwmsApiSession(TestForecastSpec._MOCK_ROOT)
        cwms_forecast = CwmsForecastSpec(session)
        levels = cwms_forecast.retrieve_forecast_specs_json(
            "id_mask",
            "office_mask",
            "designator_mask",
            "location_mask",
            "source_entity",
        )
        self.assertEqual(_FORECAST_SPECS_JSON, levels)

    @requests_mock.Mocker()
    def test_retrieve_forecast_spec_json(self, m):
        m.get(
            f"{TestForecastSpec._MOCK_ROOT}/forecast-spec/test-spec?"
            f"office=SWT&designator=designator",
            json=_FORECAST_SPECS_JSON,
        )
        session = CwmsApiSession(TestForecastSpec._MOCK_ROOT)
        cwms_forecast = CwmsForecastSpec(session)
        levels = cwms_forecast.retrieve_forecast_spec_json(
            "test-spec", "SWT", "designator"
        )
        self.assertEqual(_FORECAST_SPECS_JSON, levels)

    @requests_mock.Mocker()
    def test_store_forecast_spec_json(self, m):
        m.post(
            f"{TestForecastSpec._MOCK_ROOT}/forecast-spec",
            status_code=200,
            json=_FORECAST_SPEC_JSON,
        )
        session = CwmsApiSession(TestForecastSpec._MOCK_ROOT)
        cwms_forecast = CwmsForecastSpec(session)
        cwms_forecast.store_forecast_spec_json(_FORECAST_SPEC_JSON)
        assert m.called
        assert m.call_count == 1

    @requests_mock.Mocker()
    def test_delete_forecast_spec_json(self, m):
        m.delete(
            f"{TestForecastSpec._MOCK_ROOT}/forecast-spec/test-spec?"
            f"office=SWT&designator=designator",
            status_code=200,
        )
        session = CwmsApiSession(TestForecastSpec._MOCK_ROOT)
        cwms_forecast = CwmsForecastSpec(session)
        cwms_forecast.delete_forecast_spec("test-spec", "SWT", "designator")
        assert m.called
        assert m.call_count == 1


if __name__ == "__main__":
    unittest.main()
