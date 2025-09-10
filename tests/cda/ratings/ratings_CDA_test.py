import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

import cwms
import cwms.ratings.ratings_spec as ratings_spec
import cwms.ratings.ratings_template as ratings_template
from cwms.api import ApiError

RESOURCES = Path(__file__).parent.parent / "resources"

TEST_OFFICE = "MVP"
TEST_LOCATION_ID = "pytest_template_group"
TEST_TEMPLATE_ID = "pytest_template.Linear"


@pytest.fixture(scope="module", autouse=True)
def setup_data():
    location = {
        "office-id": TEST_OFFICE,
        "location-id": TEST_LOCATION_ID,
        "location-name": "Pytest Template Location",
        "location-type": "OTHER",
        "latitude": 40.0,
        "longitude": -90.0,
        "elevation": 100.0,
        "horizontal-datum": "NAD83",
        "vertical-datum": "NAVD88",
    }
    cwms.store_location(location)
    yield
    cwms.delete_location(TEST_LOCATION_ID, TEST_OFFICE, cascade_delete=True)


@pytest.fixture(autouse=True)
def init_session():
    print("Initializing CWMS API session for template tests...")


def test_store_template():
    template_json = json.load(open(RESOURCES / "template.json"))
    ratings_template.store_rating_template(template_json)
    fetched = ratings_template.get_rating_template(TEST_TEMPLATE_ID, TEST_OFFICE)
    assert fetched["template-id"] == TEST_TEMPLATE_ID
    assert fetched["office-id"] == TEST_OFFICE


def test_get_template():
    fetched = ratings_template.get_rating_template(TEST_TEMPLATE_ID, TEST_OFFICE)
    assert fetched["template-id"] == TEST_TEMPLATE_ID
    assert fetched["office-id"] == TEST_OFFICE


def test_update_template():
    template_json = json.load(open(RESOURCES / "template.json"))
    template_json["description"] = template_json.get("description", "") + " - updated"
    ratings_template.store_rating_template(template_json)
    fetched = ratings_template.get_rating_template(TEST_TEMPLATE_ID, TEST_OFFICE)
    assert fetched["description"].endswith(" - updated")


def test_delete_template():
    ratings_template.delete_rating_template(TEST_TEMPLATE_ID, TEST_OFFICE)
    with pytest.raises(ApiError):
        ratings_template.get_rating_template(TEST_TEMPLATE_ID, TEST_OFFICE)


# Rating specs
def test_store_rating_spec():
    spec_json = json.load(open(RESOURCES / "spec.json"))
    ratings_spec.store_rating_spec(spec_json)
    fetched = ratings_spec.get_rating_spec(spec_json["rating-spec-id"], TEST_OFFICE)
    assert fetched["rating-spec-id"] == spec_json["rating-spec-id"]
    assert fetched["office-id"] == TEST_OFFICE


def test_get_rating_spec():
    spec_json = json.load(open(RESOURCES / "spec.json"))
    fetched = ratings_spec.get_rating_spec(spec_json["rating-spec-id"], TEST_OFFICE)
    assert fetched["rating-spec-id"] == spec_json["rating-spec-id"]
    assert fetched["office-id"] == TEST_OFFICE


def test_update_rating_spec():
    spec_json = json.load(open(RESOURCES / "spec.json"))
    spec_json["description"] = spec_json.get("description", "") + " - updated"
    ratings_spec.store_rating_spec(spec_json)
    fetched = ratings_spec.get_rating_spec(spec_json["rating-spec-id"], TEST_OFFICE)
    assert fetched["description"].endswith(" - updated")


def test_delete_rating_spec():
    spec_json = json.load(open(RESOURCES / "spec.json"))
    ratings_spec.delete_rating_spec(spec_json["rating-spec-id"], TEST_OFFICE)
    with pytest.raises(ApiError):
        ratings_spec.get_rating_spec(spec_json["rating-spec-id"], TEST_OFFICE)
