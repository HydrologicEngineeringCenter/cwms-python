import json
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd
import pytest

import cwms
import cwms.ratings.ratings_spec as ratings_spec
import cwms.ratings.ratings_template as ratings_template
from cwms.api import ApiError


def get_xml_text(root, tag):
    elem = root.find(tag)
    if elem is None:
        raise AssertionError(f"Missing <{tag}> in XML response")
    return elem.text


RESOURCES = Path(__file__).parent.parent / "resources"

TEST_OFFICE = "MVP"
TEST_LOCATION_ID = "TestRating"

# Parse spec.xml to get rating-spec-id
SPEC_XML = (RESOURCES / "spec.xml").read_text()
spec_root = ET.fromstring(SPEC_XML)

TEST_RATING_SPEC_ID = spec_root.findtext(
    "rating-spec-id"
)  # TestRating.Stage;Flow.TEST.Spec-test


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
    print("Initializing CWMS API session for ratings tests...")


def test_store_template():
    TEMPLATE_XML = (RESOURCES / "template.xml").read_text()
    TEST_TEMPLATE_ID = "Stage;Flow.TEST"

    ratings_template.store_rating_template(TEMPLATE_XML)
    fetched = ratings_template.get_rating_template(TEST_TEMPLATE_ID, TEST_OFFICE)
    assert fetched.json["id"] == TEST_TEMPLATE_ID
    assert fetched.json["office-id"] == TEST_OFFICE
    assert TEST_TEMPLATE_ID in fetched.df["id"].values
    assert TEST_OFFICE in fetched.df["office-id"].values


def test_get_template():
    TEMPLATE_XML = (RESOURCES / "template.xml").read_text()
    TEST_TEMPLATE_ID = "Stage;Flow.TEST"

    fetched = ratings_template.get_rating_template(TEST_TEMPLATE_ID, TEST_OFFICE)
    assert fetched.json["id"] == TEST_TEMPLATE_ID
    assert fetched.json["office-id"] == TEST_OFFICE
    assert TEST_TEMPLATE_ID in fetched.df["id"].values
    assert TEST_OFFICE in fetched.df["office-id"].values


def test_update_template():
    TEMPLATE_XML = (RESOURCES / "template.xml").read_text()
    TEST_TEMPLATE_ID = "Stage;Flow.TEST"

    root = ET.fromstring(TEMPLATE_XML)
    desc = root.find("description")
    if desc is None:
        desc = ET.SubElement(root, "description")
    desc.text = (desc.text or "") + " - updated"

    updated_xml = ET.tostring(root, encoding="unicode", xml_declaration=True)
    ratings_template.store_rating_template(updated_xml, fail_if_exists=False)
    updated = ratings_template.get_rating_template(TEST_TEMPLATE_ID, TEST_OFFICE)
    assert updated.json["description"].endswith(" - updated")
    assert updated.df["description"].iloc[0].endswith(" - updated")


def test_store_rating_spec():
    ratings_spec.store_rating_spec(SPEC_XML)
    fetched = ratings_spec.get_rating_spec(TEST_RATING_SPEC_ID, TEST_OFFICE)
    data_json = fetched.json
    data_df = fetched.df
    assert data_json["rating-id"] == TEST_RATING_SPEC_ID
    assert data_json["office-id"] == TEST_OFFICE
    assert TEST_RATING_SPEC_ID in data_df["rating-id"].values
    assert TEST_OFFICE in data_df["office-id"].values


def test_get_rating_spec():
    fetched = ratings_spec.get_rating_spec(TEST_RATING_SPEC_ID, TEST_OFFICE)
    data_json = fetched.json
    data_df = fetched.df
    assert data_json["rating-id"] == TEST_RATING_SPEC_ID
    assert data_json["office-id"] == TEST_OFFICE
    assert TEST_RATING_SPEC_ID in data_df["rating-id"].values
    assert TEST_OFFICE in data_df["office-id"].values


def test_update_rating_spec():
    desc = spec_root.find("description")
    if desc is None:
        desc = ET.SubElement(spec_root, "description")
    desc.text = (desc.text or "") + " - updated"
    updated_xml = ET.tostring(spec_root, encoding="unicode", xml_declaration=True)

    ratings_spec.store_rating_spec(updated_xml, fail_if_exists=False)
    fetched = ratings_spec.get_rating_spec(TEST_RATING_SPEC_ID, TEST_OFFICE)
    data_json = fetched.json
    data_df = fetched.df
    assert data_json["description"].endswith(" - updated")
    assert data_df["description"].iloc[0].endswith(" - updated")


def test_delete_rating_spec():
    ratings_spec.delete_rating_spec(TEST_RATING_SPEC_ID, TEST_OFFICE, "DELETE_ALL")
    with pytest.raises(ApiError):
        ratings_spec.get_rating_spec(TEST_RATING_SPEC_ID, TEST_OFFICE)


def test_delete_template():
    TEST_TEMPLATE_ID = "Stage;Flow.TEST"

    ratings_template.delete_rating_template(TEST_TEMPLATE_ID, TEST_OFFICE, "DELETE_ALL")
    with pytest.raises(ApiError):
        ratings_template.get_rating_template(TEST_TEMPLATE_ID, TEST_OFFICE)
