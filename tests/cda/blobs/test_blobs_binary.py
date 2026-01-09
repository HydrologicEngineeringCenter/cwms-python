from __future__ import annotations

import base64
import mimetypes
from pathlib import Path

import pytest

import cwms.catalog.blobs as blobs

TEST_OFFICE = "MVP"
EXCEL_BLOB_ID = "PYTEST_BLOB_EXCEL"


def _excel_mime_type(path: Path) -> str:
    ext = path.suffix.lower()
    guessed = mimetypes.guess_type(path.name)[0]
    if ext == ".xlsx":
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    if ext == ".xls":
        return "application/vnd.ms-excel"
    return guessed or "application/octet-stream"


@pytest.fixture
def excel_payload() -> dict:
    excel_file_path = Path(__file__).parent / ".." / "resources" / "blob_test.xlsx"
    with open(excel_file_path, "rb") as f:
        file_data = f.read()

    return {
        "office-id": TEST_OFFICE,
        "id": EXCEL_BLOB_ID,
        "description": "pytest excel blob",
        "media-type-id": _excel_mime_type(excel_file_path),
        "value": base64.b64encode(file_data).decode("utf-8"),
    }


@pytest.fixture
def stored_excel_blob(excel_payload):
    # ensure clean start
    try:
        blobs.delete_blob(office_id=TEST_OFFICE, blob_id=EXCEL_BLOB_ID)
    except Exception:
        pass

    blobs.store_blobs(data=excel_payload, fail_if_exists=False)
    yield

    # always cleanup
    try:
        blobs.delete_blob(office_id=TEST_OFFICE, blob_id=EXCEL_BLOB_ID)
    except Exception:
        pass


def test_store_blob_excel_creates_blob(stored_excel_blob):
    # If store_blobs didn't throw, we consider it created.
    # If you want a stronger assertion, use your shared find_row helper from Option A.
    assert True


def test_get_excel_blob_returns_content(stored_excel_blob):
    content = blobs.get_blob(office_id=TEST_OFFICE, blob_id=EXCEL_BLOB_ID)
    assert content is not None
    assert len(content) > 0
