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

# Parse template.xml to construct template ID
template_xml = (RESOURCES / "template.xml").read_text()
template_root = ET.fromstring(template_xml)
parameters_id = template_root.findtext("parameters-id")
template_version = template_root.findtext("version")

TEST_TEMPLATE_ID = f"{parameters_id}.{template_version}"  # Stage;Flow.TEST

# Parse spec.xml to get rating-spec-id
spec_xml = (RESOURCES / "spec.xml").read_text()
spec_root = ET.fromstring(spec_xml)

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
    ratings_template.store_rating_template(template_xml)
    fetched = ratings_template.get_rating_template(TEST_TEMPLATE_ID, TEST_OFFICE)
    assert fetched.json["id"] == TEST_TEMPLATE_ID
    assert fetched.json["office-id"] == TEST_OFFICE
    assert TEST_TEMPLATE_ID in fetched.df["id"].values
    assert TEST_OFFICE in fetched.df["office-id"].values


def test_get_template():
    fetched = ratings_template.get_rating_template(TEST_TEMPLATE_ID, TEST_OFFICE)
    assert fetched.json["id"] == TEST_TEMPLATE_ID
    assert fetched.json["office-id"] == TEST_OFFICE
    assert TEST_TEMPLATE_ID in fetched.df["id"].values
    assert TEST_OFFICE in fetched.df["office-id"].values


def test_update_template():
    root = ET.fromstring(template_xml)
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
    ratings_spec.store_rating_spec(spec_xml)
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
    ratings_template.delete_rating_template(TEST_TEMPLATE_ID, TEST_OFFICE, "DELETE_ALL")
    with pytest.raises(ApiError):
        ratings_template.get_rating_template(TEST_TEMPLATE_ID, TEST_OFFICE)
