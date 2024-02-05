#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import json
import unittest
import pytest
from pathlib import Path
from requests.exceptions import HTTPError

import requests_mock

from CWMS.core import CwmsApiSession
from CWMS.cwms_level import CwmsLevel


def read_resource_file(file_name):
    current_path = Path(__file__).resolve().parent
    resource_path = current_path / "resources" / file_name

    with open(resource_path, "r") as file:
        data = json.load(file)

    return data


_SPEC_LEVELS_RETURN = read_resource_file("specified_levels.json")
_SPEC_LEVEL_RETURN = read_resource_file("specified_level.json")
_ERROR_CODE_500 = read_resource_file("error_code_500.json")


class TestRetrieveSpecifiedLevels(unittest.TestCase):

    @requests_mock.Mocker()
    def test_retrieve_specified_levels_json_default(self, m):
        m.get("https://mockwebserver.cwms.gov/specified-levels?office=%2A&template-id-mask=%2A",
              json=_SPEC_LEVELS_RETURN)
        cwms_levels = CwmsLevel(CwmsApiSession("https://mockwebserver.cwms.gov"))
        levels = cwms_levels.retrieve_specified_levels_json()
        self.assertEqual(_SPEC_LEVELS_RETURN, levels)

    @requests_mock.Mocker()
    def test_retrieve_specified_levels_json(self, m):
        m.get("https://mockwebserver.cwms.gov/specified-levels?office=SWT&template-id-mask=%2A",
              json=_SPEC_LEVELS_RETURN)
        cwms_levels = CwmsLevel(CwmsApiSession("https://mockwebserver.cwms.gov"))
        levels = cwms_levels.retrieve_specified_levels_json("*", "SWT")
        self.assertEqual(_SPEC_LEVELS_RETURN, levels)

    @requests_mock.Mocker()
    def test_retrieve_specified_level_json(self, m):
        m.get(f"https://mockwebserver.cwms.gov/specified-levels/Bottom%20of%20Exclusive%20Flood%20Control?office=CWMS",
              json=_SPEC_LEVEL_RETURN)
        cwms_levels = CwmsLevel(CwmsApiSession("https://mockwebserver.cwms.gov"))
        levels = cwms_levels.retrieve_specified_level_json("Bottom of Exclusive Flood Control", "CWMS")
        self.assertEqual(_SPEC_LEVEL_RETURN, levels)

    @requests_mock.Mocker()
    def test_store_specified_level_json(self, m):
        m.post(f"https://mockwebserver.cwms.gov/specified-levels?fail-if-exists=True",
              status_code=200, json=_SPEC_LEVEL_RETURN)
        cwms_levels = CwmsLevel(CwmsApiSession("https://mockwebserver.cwms.gov"))
        cwms_levels.store_specified_level_json(_SPEC_LEVEL_RETURN)
        assert m.called
        assert m.call_count == 1

    @requests_mock.Mocker()
    def test_store_specified_level_error_code_json(self, m):
        with pytest.raises(HTTPError):
            m.post(f"https://mockwebserver.cwms.gov/specified-levels",
                   status_code=500, json=_ERROR_CODE_500)
            cwms_levels = CwmsLevel(CwmsApiSession("https://mockwebserver.cwms.gov"))
            cwms_levels.store_specified_level_json(_SPEC_LEVEL_RETURN)

    @requests_mock.Mocker()
    def test_delete_specified_level_json(self, m):
        m.delete(f"https://mockwebserver.cwms.gov/specified-levels/Test?office=SWT",
               status_code=200)
        cwms_levels = CwmsLevel(CwmsApiSession("https://mockwebserver.cwms.gov"))
        cwms_levels.delete_specified_level("Test", "SWT")
        assert m.called
        assert m.call_count == 1

    @requests_mock.Mocker()
    def test_update_specified_level_json(self, m):
        m.patch(f"https://mockwebserver.cwms.gov/specified-levels/Test?specified-level-id=TEst2&office=SWT",
                 status_code=200)
        cwms_levels = CwmsLevel(CwmsApiSession("https://mockwebserver.cwms.gov"))
        cwms_levels.update_specified_level("Test", "Test2", "SWT")
        assert m.called
        assert m.call_count == 1


if __name__ == "__main__":
    unittest.main()
