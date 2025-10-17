# tests/test_blob.py
from __future__ import annotations

import base64
import mimetypes
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import pytest

import cwms.catalog.blobs as blobs

TEST_OFFICE = "MVP"
TEST_BLOB_ID = "PYTEST_BLOB_ALPHA"
TEST_BLOB_UPDATED_ID = TEST_BLOB_ID  # keeping same id; update modifies fields
TEST_MEDIA_TYPE = "text/plain"
TEST_DESC = "pytest blob ? initial"
TEST_DESC_UPDATED = "pytest blob ? updated"
TEST_TEXT = "Hello from pytest @ " + datetime.now(timezone.utc).isoformat(
    timespec="seconds"
)
TEST_TEXT_UPDATED = TEST_TEXT + " (edited)"


@pytest.fixture(scope="module", autouse=True)
def ensure_clean_slate():
    """Delete the test blob (if it exists) before/after running this module."""
    try:
        blobs.delete_blob(office_id=TEST_OFFICE, blob_id=TEST_BLOB_ID)
    except Exception:
        pass
    yield
    try:
        blobs.delete_blob(office_id=TEST_OFFICE, blob_id=TEST_BLOB_ID)
    except Exception:
        pass


@pytest.fixture(autouse=True)
def init_session(request):
    print("Initializing CWMS API session for blob tests...")


def _find_blob_row(office: str, blob_id: str) -> Optional[pd.Series]:
    """
    Helper: return the row for blob_id from cwms.get_blobs(...).df if present.
    """
    res = blobs.get_blobs(office_id=office, blob_id_like=blob_id)
    df = res if isinstance(res, pd.DataFrame) else getattr(res, "df", None)
    if df is None or df.empty:
        return None
    # normalize id column name if needed id or blob-id
    if "id" not in df.columns and "blob-id" in df.columns:
        df = df.rename(columns={"blob-id": "id"})
    match = df[df["id"].str.upper() == blob_id.upper()]
    return match.iloc[0] if not match.empty else None


def test_store_blob_xcel():
    excel_file_path = Path(__file__).parent.parent / "resources" / "blob_test.xlsx"
    with open(excel_file_path, "rb") as f:
        file_data = f.read()
    mime_type, _ = mimetypes.guess_type(excel_file_path)
    xcel_blob_id = "TEST_BLOB_XCEL"
    payload = {
        "office-id": TEST_OFFICE,
        "id": xcel_blob_id,
        "description": "testing excel file",
        "media-type-id": mime_type,
        "value": base64.b64encode(file_data).decode("utf-8"),
    }
    blobs.store_blobs(data=payload)
    try:
        row = _find_blob_row(TEST_OFFICE, xcel_blob_id)
        assert row is not None, "Stored blob not found in listing"
    finally:
        # Cleanup second specified level
        blobs.delete_blob(blob_id=xcel_blob_id, office_id=TEST_OFFICE)


def test_store_blob():
    # Build request JSON for store_blobs
    payload = {
        "office-id": TEST_OFFICE,
        "id": TEST_BLOB_ID,
        "description": TEST_DESC,
        "media-type-id": TEST_MEDIA_TYPE,
        "value": TEST_TEXT,
    }
    blobs.store_blobs(payload, fail_if_exists=True)

    # Verify via listing metadata
    row = _find_blob_row(TEST_OFFICE, TEST_BLOB_ID)
    assert row is not None, "Stored blob not found in listing"
    assert str(row["id"]).upper() == TEST_BLOB_ID
    if "media-type-id" in row.index:
        assert row["media-type-id"] == TEST_MEDIA_TYPE
    if "description" in row.index:
        assert TEST_DESC in str(row["description"])

    # Verify content by downloading
    content = blobs.get_blob(office_id=TEST_OFFICE, blob_id=TEST_BLOB_ID)
    assert isinstance(content, str) and content, "Empty blob content"
    assert TEST_TEXT in content


def test_get_blob():
    # Do a simple read of the blob created in test_store_blob
    content = blobs.get_blob(office_id=TEST_OFFICE, blob_id=TEST_BLOB_ID)
    assert TEST_TEXT in content
    assert len(content) >= len(TEST_TEXT)


def test_update_blob():
    # Test updating all fields
    update = {
        "office-id": TEST_OFFICE,
        "id": TEST_BLOB_UPDATED_ID,
        "description": TEST_DESC_UPDATED,
        "media-type-id": TEST_MEDIA_TYPE,
        "value": TEST_TEXT_UPDATED,
    }
    blobs.update_blob(update, fail_if_not_exists=True)

    # Confirm updated metadata
    row = _find_blob_row(TEST_OFFICE, TEST_BLOB_UPDATED_ID)
    assert row is not None, "Updated blob not found"
    if "description" in row.index:
        assert TEST_DESC_UPDATED in str(row["description"])

    # Verify new content
    content = blobs.get_blob(office_id=TEST_OFFICE, blob_id=TEST_BLOB_UPDATED_ID)
    assert TEST_TEXT_UPDATED in content
