#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

from datetime import datetime

import pytest
import pytz

import cwms.api
import cwms.standard_text.standard_text as standard_text
from cwms.types import DeleteMethod
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"
_STD_TEXT_JSON = read_resource_file("standard_text.json")
_STS_TEXT_CAT_JSON = read_resource_file("standard_text_catalog.json")


def test_get_standard_text(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}" "/standard-text-id/HW?office=SPK",
        json=_STD_TEXT_JSON,
    )

    text_id = "HW"
    office_id = "SPK"

    data = standard_text.get_standard_text(text_id, office_id)
    assert data.json == _STD_TEXT_JSON


def test_get_standard_text_catalog_default(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}" "/standard-text-id",
        json=_STS_TEXT_CAT_JSON,
    )

    data = standard_text.get_standard_text_catalog()
    assert data.json == _STS_TEXT_CAT_JSON


def test_get_standard_text_catalog(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}" "/standard-text-id?text-id-mask=HW&office-id-mask=SPK",
        json=_STS_TEXT_CAT_JSON,
    )

    text_id = "HW"
    office_id = "SPK"

    data = standard_text.get_standard_text_catalog(text_id, office_id)
    assert data.json == _STS_TEXT_CAT_JSON


def test_create_standard_text(requests_mock):
    requests_mock.post(f"{_MOCK_ROOT}" "/standard-text-id?fail-if-exists=True")

    standard_text.store_standard_text(_STD_TEXT_JSON, fail_if_exists=True)

    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_delete_standard_text(requests_mock):
    requests_mock.delete(
        f"{_MOCK_ROOT}" "/standard-text-id/HW?office=SPK&method=DELETE_ALL"
    )

    text_id = "HW"
    office_id = "SPK"

    standard_text.delete_standard_text(text_id, DeleteMethod.DELETE_ALL, office_id)

    assert requests_mock.called
    assert requests_mock.call_count == 1
