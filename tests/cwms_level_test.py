#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import unittest
from datetime import datetime

import pytest
import pytz
import requests_mock
from requests.exceptions import HTTPError

from ._test_utils import read_resource_file
from CWMS.core import CwmsApiSession
from CWMS.cwms_level import CwmsLevel


_SPEC_LEVELS_JSON = read_resource_file("specified_levels.json")
_SPEC_LEVEL_JSON = read_resource_file("specified_level.json")
_LOC_LEVELS_JSON = read_resource_file("location_levels.json")
_LOC_LEVEL_JSON = read_resource_file("location_level.json")
_LOC_LEVEL_TS_JSON = read_resource_file("level_timeseries.json")
_ERROR_CODE_500_JSON = read_resource_file("error_code_500.json")


class TestSpecLevels(unittest.TestCase):
    _MOCK_ROOT = "https://mockwebserver.cwms.gov"

    @requests_mock.Mocker()
    def test_retrieve_specified_levels_json_default(self, m):
        m.get(f"{TestSpecLevels._MOCK_ROOT}/specified-levels?office=%2A&template-id-mask=%2A",
              json=_SPEC_LEVELS_JSON)
        cwms_levels = CwmsLevel(CwmsApiSession(TestSpecLevels._MOCK_ROOT))
        levels = cwms_levels.retrieve_specified_levels_json()
        self.assertEqual(_SPEC_LEVELS_JSON, levels)

    @requests_mock.Mocker()
    def test_retrieve_specified_levels_json(self, m):
        m.get(f"{TestSpecLevels._MOCK_ROOT}/specified-levels?office=SWT&template-id-mask=%2A",
              json=_SPEC_LEVELS_JSON)
        cwms_levels = CwmsLevel(CwmsApiSession(TestSpecLevels._MOCK_ROOT))
        levels = cwms_levels.retrieve_specified_levels_json("*", "SWT")
        self.assertEqual(_SPEC_LEVELS_JSON, levels)

    @requests_mock.Mocker()
    def test_store_specified_level_json(self, m):
        m.post(f"{TestSpecLevels._MOCK_ROOT}/specified-levels?fail-if-exists=True",
               status_code=200, json=_SPEC_LEVEL_JSON)
        cwms_levels = CwmsLevel(CwmsApiSession(TestSpecLevels._MOCK_ROOT))
        cwms_levels.store_specified_level_json(_SPEC_LEVEL_JSON)
        assert m.called
        assert m.call_count == 1

    @requests_mock.Mocker()
    def test_store_specified_level_error_code_json(self, m):
        with pytest.raises(HTTPError):
            m.post(f"{TestSpecLevels._MOCK_ROOT}/specified-levels",
                   status_code=500, json=_ERROR_CODE_500_JSON)
            cwms_levels = CwmsLevel(CwmsApiSession(TestSpecLevels._MOCK_ROOT))
            cwms_levels.store_specified_level_json(_SPEC_LEVEL_JSON)

    @requests_mock.Mocker()
    def test_delete_specified_level_json(self, m):
        m.delete(f"{TestSpecLevels._MOCK_ROOT}/specified-levels/Test?office=SWT",
                 status_code=200)
        cwms_levels = CwmsLevel(CwmsApiSession(TestSpecLevels._MOCK_ROOT))
        cwms_levels.delete_specified_level("Test", "SWT")
        assert m.called
        assert m.call_count == 1

    @requests_mock.Mocker()
    def test_update_specified_level_json(self, m):
        m.patch(f"{TestSpecLevels._MOCK_ROOT}/specified-levels/Test?specified-level-id=TEst2&office=SWT",
                status_code=200)
        cwms_levels = CwmsLevel(CwmsApiSession(TestSpecLevels._MOCK_ROOT))
        cwms_levels.update_specified_level("Test", "Test2", "SWT")
        assert m.called
        assert m.call_count == 1


class TestLocLevels(unittest.TestCase):
    _MOCK_ROOT = "https://mockwebserver.cwms.gov"

    @requests_mock.Mocker()
    def test_retrieve_loc_levels_json_default(self, m):
        m.get(f"{TestLocLevels._MOCK_ROOT}/levels?level-id-mask=%2A",
              json=_LOC_LEVELS_JSON)
        cwms_levels = CwmsLevel(CwmsApiSession(TestSpecLevels._MOCK_ROOT))
        levels = cwms_levels.retrieve_location_levels_json()
        self.assertEqual(_LOC_LEVELS_JSON, levels)

    @requests_mock.Mocker()
    def test_retrieve_loc_levels_json(self, m):
        m.get(f"{TestLocLevels._MOCK_ROOT}/levels?office=SWT&level-id-mask=AARK.Elev.Inst.0.Bottom+of+Inlet&"
              "unit=m&datum=NAV88&begin=2020-02-14T10%3A30%3A00-08%3A00&end=2020-03-30T10%3A30%3A00-07%3A00&"
              "page=MHx8bnVsbHx8MTAw&page-size=100",
              json=_LOC_LEVELS_JSON)
        cwms_levels = CwmsLevel(CwmsApiSession(TestSpecLevels._MOCK_ROOT))
        level_id = "AARK.Elev.Inst.0.Bottom of Inlet"
        office_id = "SWT"
        unit = "m"
        datum = "NAV88"
        timezone = pytz.timezone("US/Pacific")
        begin = timezone.localize(datetime(2020, 2, 14, 10, 30, 0))
        end = timezone.localize(datetime(2020, 3, 30, 10, 30, 0))
        page = "MHx8bnVsbHx8MTAw"
        page_size = 100
        levels = cwms_levels.retrieve_location_levels_json(level_id, office_id, unit, datum,
                                                           begin, end, page, page_size)
        self.assertEqual(_LOC_LEVELS_JSON, levels)

    @requests_mock.Mocker()
    def test_retrieve_loc_level_json(self, m):
        m.get(f"{TestLocLevels._MOCK_ROOT}/levels/AARK.Elev.Inst.0.Bottom%20of%20Inlet?office=SWT&"
              "unit=m&effective-date=2020-02-14T10%3A30%3A00-08%3A00",
              json=_LOC_LEVEL_JSON)
        cwms_levels = CwmsLevel(CwmsApiSession(TestSpecLevels._MOCK_ROOT))
        level_id = "AARK.Elev.Inst.0.Bottom of Inlet"
        office_id = "SWT"
        unit = "m"
        timezone = pytz.timezone("US/Pacific")
        effective_date = timezone.localize(datetime(2020, 2, 14, 10, 30, 0))
        levels = cwms_levels.retrieve_location_level_json(
            level_id, office_id, effective_date, unit)
        self.assertEqual(_LOC_LEVEL_JSON, levels)

    @requests_mock.Mocker()
    def test_store_loc_level_json(self, m):
        m.post(f"{TestLocLevels._MOCK_ROOT}/levels")
        cwms_levels = CwmsLevel(CwmsApiSession(TestSpecLevels._MOCK_ROOT))
        data = _LOC_LEVEL_JSON
        cwms_levels.store_location_level_json(data)
        assert m.called
        assert m.call_count == 1

    @requests_mock.Mocker()
    def test_delete_loc_level(self, m):
        m.delete(f"{TestLocLevels._MOCK_ROOT}/levels/AARK.Elev.Inst.0.Bottom%20of%20Inlet?office=SWT&"
                 "effective-date=2020-02-14T10%3A30%3A00-08%3A00&cascade-delete=True",
                 json=_LOC_LEVEL_JSON)
        cwms_levels = CwmsLevel(CwmsApiSession(TestSpecLevels._MOCK_ROOT))
        level_id = "AARK.Elev.Inst.0.Bottom of Inlet"
        office_id = "SWT"
        timezone = pytz.timezone("US/Pacific")
        effective_date = timezone.localize(datetime(2020, 2, 14, 10, 30, 0))
        cwms_levels.delete_location_level(
            level_id, office_id, effective_date, True)
        assert m.called
        assert m.call_count == 1

    @requests_mock.Mocker()
    def test_retrieve_loc_level_ts_json(self, m):
        m.get(f"{TestLocLevels._MOCK_ROOT}/levels/AARK.Elev.Inst.0.Bottom%20of%20Inlet/timeseries?office=SWT&unit=m&"
              "begin=2020-02-14T10%3A30%3A00-08%3A00&end=2020-03-14T10%3A30%3A00-07%3A00&interval=1Day",
              json=_LOC_LEVEL_TS_JSON)
        cwms_levels = CwmsLevel(CwmsApiSession(TestSpecLevels._MOCK_ROOT))
        level_id = "AARK.Elev.Inst.0.Bottom of Inlet"
        office_id = "SWT"
        interval = "1Day"
        timezone = pytz.timezone("US/Pacific")
        begin = timezone.localize(datetime(2020, 2, 14, 10, 30, 0))
        end = timezone.localize(datetime(2020, 3, 14, 10, 30, 0))
        levels = cwms_levels.retrieve_level_as_timeseries_json(
            level_id, office_id, "m", begin, end, interval)
        self.assertEqual(_LOC_LEVEL_TS_JSON, levels)


if __name__ == "__main__":
    unittest.main()
