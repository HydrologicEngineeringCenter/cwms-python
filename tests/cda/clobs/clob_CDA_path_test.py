# tests/test_clob.py
from __future__ import annotations

import base64
import mimetypes
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import pytest

import cwms.catalog.clobs as clobs
from cwms.cwms_types import Data

TEST_OFFICE = "MVP"
TEST_CLOB_ID = "/PYTEST/CLOB/ALPHA"
TEST_CLOB_UPDATED_ID = TEST_CLOB_ID  # keeping same id; update modifies fields
TEST_DESC = "pytest clob ? initial"
TEST_DESC_UPDATED = "pytest clob ? updated"
TEST_TEXT = "Hello from pytest @ " + datetime.now(timezone.utc).isoformat(
    timespec="seconds"
)
TEST_TEXT_UPDATED = TEST_TEXT + " (edited)"


@pytest.fixture(scope="module", autouse=True)
def ensure_clean_slate():
    """Delete the test clob (if it exists) before/after running this module."""
    try:
        clobs.delete_clob(office_id=TEST_OFFICE, clob_id=TEST_CLOB_ID)
    except Exception:
        pass
    yield
    try:
        clobs.delete_clob(office_id=TEST_OFFICE, clob_id=TEST_CLOB_ID)
    except Exception:
        pass


@pytest.fixture(autouse=True)
def init_session(request):
    print("Initializing CWMS API session for clob tests...")


def _find_clob_row(office: str, clob_id: str) -> Optional[pd.Series]:
    """
    Helper: return the row for clob_id from cwms.get_clobs(...).df if present.
    """
    res = clobs.get_clobs(office_id=office, clob_id_like=clob_id)
    df = res if isinstance(res, pd.DataFrame) else getattr(res, "df", None)
    if df is None or df.empty:
        return None
    # normalize id column name if needed id or clob-id
    if "id" not in df.columns and "clob-id" in df.columns:
        df = df.rename(columns={"clob-id": "id"})
    match = df[df["id"].str.upper() == clob_id.upper()]
    return match.iloc[0] if not match.empty else None


def test_store_clob():
    # Build request JSON for store_clobs
    payload = {
        "office-id": TEST_OFFICE,
        "id": TEST_CLOB_ID,
        "description": TEST_DESC,
        "value": TEST_TEXT,
    }
    clobs.store_clobs(payload, fail_if_exists=True)

    # Verify via listing metadata
    row = _find_clob_row(TEST_OFFICE, TEST_CLOB_ID)
    assert row is not None, "Stored clob not found in listing"
    assert str(row["id"]).upper() == TEST_CLOB_ID
    if "description" in row.index:
        assert TEST_DESC in str(row["description"])

    # Verify content by downloading
    data = clobs.get_clob(office_id=TEST_OFFICE, clob_id=TEST_CLOB_ID)
    assert isinstance(data, Data) and isinstance(
        data.json["value"], str
    ), "Empty clob content"
    content = data.json["value"]
    assert TEST_TEXT in content


def test_get_clob():
    # Do a simple read of the clob created in test_store_clob
    data = clobs.get_clob(office_id=TEST_OFFICE, clob_id=TEST_CLOB_ID)
    assert data.json["value"] is not None
    content = data.json["value"]
    assert TEST_TEXT in content
    assert len(content) >= len(TEST_TEXT)


def test_update_clob():
    # Test updating all fields
    update = {
        "office-id": TEST_OFFICE,
        "id": TEST_CLOB_UPDATED_ID,
        "description": TEST_DESC_UPDATED,
        "value": TEST_TEXT_UPDATED,
    }
    clobs.update_clob(update, ignore_nulls=True)

    # Confirm updated metadata
    row = _find_clob_row(TEST_OFFICE, TEST_CLOB_UPDATED_ID)
    assert row is not None, "Updated clob not found"
    if "description" in row.index:
        assert TEST_DESC_UPDATED in str(row["description"])

    # Verify new content
    data = clobs.get_clob(office_id=TEST_OFFICE, clob_id=TEST_CLOB_UPDATED_ID)
    content = data.json["value"]
    assert TEST_TEXT_UPDATED in content
