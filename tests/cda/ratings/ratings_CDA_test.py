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
    location = json.load(open(RESOURCES / "location.json"))
    cwms.store_location(location)
    yield
    try:
        cwms.delete_location(TEST_LOCATION_ID, TEST_OFFICE, cascade_delete=True)
    except ApiError:
        pass


@pytest.fixture(autouse=True)
def init_session():
    print("Initializing CWMS API session for template tests...")


def test_store_template():
    template_xml = (RESOURCES / "template.xml").read_text()
    ratings_template.store_rating_template(template_xml)
    fetched = ratings_template.get_rating_template(TEST_TEMPLATE_ID, TEST_OFFICE)
    assert fetched.json["template-id"] == TEST_TEMPLATE_ID
    assert fetched.json["office-id"] == TEST_OFFICE


def test_get_template():
    fetched = ratings_template.get_rating_template(TEST_TEMPLATE_ID, TEST_OFFICE)
    assert fetched.json["template-id"] == TEST_TEMPLATE_ID
    assert fetched.json["office-id"] == TEST_OFFICE


def test_update_template():
    fetched = ratings_template.get_rating_template(TEST_TEMPLATE_ID, TEST_OFFICE)
    fetched_json = fetched.json
    fetched_json["description"] = (fetched_json.get("description") or "") + " - updated"
    ratings_template.store_rating_template(fetched_json)
    updated = ratings_template.get_rating_template(TEST_TEMPLATE_ID, TEST_OFFICE)
    assert updated.json["description"].endswith(" - updated")


def test_delete_template():
    ratings_template.delete_rating_template(TEST_TEMPLATE_ID, TEST_OFFICE, "DELETE_ALL")
    with pytest.raises(ApiError):
        ratings_template.get_rating_template(TEST_TEMPLATE_ID, TEST_OFFICE)


# Rating specs
def test_store_rating_spec():
    spec_xml = (RESOURCES / "spec.xml").read_text()
    ratings_spec.store_rating_spec(spec_xml)
    # Parse spec_xml to get rating-spec-id for fetching
    # If spec_xml contains <rating-spec-id> element, parse it:
    import xml.etree.ElementTree as ET

    root = ET.fromstring(spec_xml)
    rating_spec_id = root.findtext("rating-spec-id")
    fetched = ratings_spec.get_rating_spec(rating_spec_id, TEST_OFFICE)
    assert fetched.json["rating-spec-id"] == rating_spec_id
    assert fetched.json["office-id"] == TEST_OFFICE


def test_get_rating_spec():
    spec_xml = (RESOURCES / "spec.xml").read_text()
    import xml.etree.ElementTree as ET

    root = ET.fromstring(spec_xml)
    rating_spec_id = root.findtext("rating-spec-id")
    fetched = ratings_spec.get_rating_spec(rating_spec_id, TEST_OFFICE)
    assert fetched.json["rating-spec-id"] == rating_spec_id
    assert fetched.json["office-id"] == TEST_OFFICE


def test_update_rating_spec():
    spec_xml = (RESOURCES / "spec.xml").read_text()
    import xml.etree.ElementTree as ET

    root = ET.fromstring(spec_xml)
    rating_spec_id = root.findtext("rating-spec-id")
    fetched = ratings_spec.get_rating_spec(rating_spec_id, TEST_OFFICE)
    fetched_json = fetched.json
    fetched_json["description"] = (fetched_json.get("description") or "") + " - updated"
    ratings_spec.store_rating_spec(fetched_json)
    updated = ratings_spec.get_rating_spec(rating_spec_id, TEST_OFFICE)
    assert updated.json["description"].endswith(" - updated")


def test_delete_rating_spec():
    spec_xml = (RESOURCES / "spec.xml").read_text()
    import xml.etree.ElementTree as ET

    root = ET.fromstring(spec_xml)
    rating_spec_id = root.findtext("rating-spec-id")
    ratings_spec.delete_rating_spec(rating_spec_id, TEST_OFFICE, "DELETE_ALL")
    with pytest.raises(ApiError):
        ratings_spec.get_rating_spec(rating_spec_id, TEST_OFFICE)
