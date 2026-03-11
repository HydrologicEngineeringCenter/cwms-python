from __future__ import annotations

from datetime import datetime, timezone

import pytest

import cwms.catalog.blobs as blobs
import cwms.catalog.clobs as clobs

from catalog_resources import CatalogResource, find_row


def _blob_content(x) -> str:
    # blobs.get_blob returns a string in your current test
    return x


def _clob_content(x) -> str:
    # clobs.get_clob returns Data with json["value"]
    return x.json["value"]


RESOURCES = [
    CatalogResource(
        name="blob",
        id_key="blob_id",
        id_col_fallback="blob-id",
        list_fn=blobs.get_blobs,
        store_fn=blobs.store_blobs,
        get_fn=blobs.get_blob,
        update_fn=blobs.update_blob,
        delete_fn=blobs.delete_blob,
        extract_content=_blob_content,
    ),
    CatalogResource(
        name="clob",
        id_key="clob_id",
        id_col_fallback="clob-id",
        list_fn=clobs.get_clobs,
        store_fn=clobs.store_clobs,
        get_fn=clobs.get_clob,
        update_fn=clobs.update_clob,
        delete_fn=clobs.delete_clob,
        extract_content=_clob_content,
    ),
]


@pytest.fixture(params=RESOURCES, ids=[r.name for r in RESOURCES])
def resource(request) -> CatalogResource:
    return request.param


@pytest.fixture
def test_constants(resource: CatalogResource):
    office = "MVP"
    item_id = f"/PYTEST/{resource.name.upper()}/ALPHA"
    desc = f"pytest {resource.name} ? initial"
    desc_updated = f"pytest {resource.name} ? updated"
    text = "Hello from pytest @ " + datetime.now(timezone.utc).isoformat(
        timespec="seconds"
    )
    text_updated = text + " (edited)"
    return office, item_id, desc, desc_updated, text, text_updated


@pytest.fixture(autouse=True)
def ensure_clean_slate(resource: CatalogResource, test_constants):
    office, item_id, *_ = test_constants
    try:
        resource.delete_fn(office_id=office, **{resource.id_key: item_id})
    except Exception:
        pass
    yield
    try:
        resource.delete_fn(office_id=office, **{resource.id_key: item_id})
    except Exception:
        pass


def test_store(resource: CatalogResource, test_constants):
    office, item_id, desc, _, text, _ = test_constants

    payload = {"office-id": office, "id": item_id, "description": desc, "value": text}
    # blob needs media-type-id; clob does not
    if resource.name == "blob":
        payload["media-type-id"] = "text/plain"

    resource.store_fn(payload, fail_if_exists=True)

    row = find_row(resource, office, item_id)
    assert row is not None, f"Stored {resource.name} not found in listing"
    assert str(row["id"]).upper() == item_id.upper()
    if "description" in row.index:
        assert desc in str(row["description"])

    got = resource.get_fn(office_id=office, **{resource.id_key: item_id})
    content = resource.extract_content(got)
    assert isinstance(content, str) and content
    assert text in content


def test_get(resource: CatalogResource, test_constants):
    office, item_id, desc, _, text, _ = test_constants

    # Ensure it exists (keeps tests order-independent)
    payload = {"office-id": office, "id": item_id, "description": desc, "value": text}
    if resource.name == "blob":
        payload["media-type-id"] = "text/plain"
    resource.store_fn(payload, fail_if_exists=False)

    got = resource.get_fn(office_id=office, **{resource.id_key: item_id})
    content = resource.extract_content(got)
    assert text in content
    assert len(content) >= len(text)


def test_update(resource: CatalogResource, test_constants):
    office, item_id, desc, desc_updated, text, text_updated = test_constants

    # Ensure it exists
    payload = {"office-id": office, "id": item_id, "description": desc, "value": text}
    if resource.name == "blob":
        payload["media-type-id"] = "text/plain"
    resource.store_fn(payload, fail_if_exists=False)

    update = {
        "office-id": office,
        "id": item_id,
        "description": desc_updated,
        "value": text_updated,
    }

    # your current APIs differ slightly here
    if resource.name == "blob":
        update["media-type-id"] = "text/plain"
        resource.update_fn(update, fail_if_not_exists=True)
    else:
        resource.update_fn(update, ignore_nulls=True)

    row = find_row(resource, office, item_id)
    assert row is not None, f"Updated {resource.name} not found"
    if "description" in row.index:
        assert desc_updated in str(row["description"])

    got = resource.get_fn(office_id=office, **{resource.id_key: item_id})
    content = resource.extract_content(got)
    assert text_updated in content
