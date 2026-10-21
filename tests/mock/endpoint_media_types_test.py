"""Check wire headers against the production Swagger contract, not mock defaults.

Source: https://cwms-data.usace.army.mil/cwms-data/swagger-docs
Specification version: 2026.05.12-i (retrieved 2026-09-10).
"""

from datetime import datetime, timezone
from importlib import import_module

import pytest

import cwms.api as api

JSON = "application/json"
JSON_V2 = "application/json;version=2"
DATA = {"id": "TEST", "office-id": "SPK"}
DATE = datetime(2026, 1, 1, tzinfo=timezone.utc)

# module, function, arguments, HTTP method, expected request/response media type
CASES = [
    ("catalog.blobs", "store_blobs", (DATA,), "POST", JSON_V2),
    ("catalog.clobs", "store_clobs", (DATA,), "POST", JSON_V2),
    ("catalog.clobs", "update_clob", (DATA,), "PATCH", JSON_V2),
    ("levels.location_levels", "store_location_level", (DATA,), "POST", JSON),
    ("levels.location_levels", "update_location_level", (DATA, "TEST"), "PATCH", JSON),
    ("locations.physical_locations", "store_location", (DATA,), "POST", JSON),
    ("locations.physical_locations", "update_location", ("TEST", DATA), "PATCH", JSON),
    ("outlets.outlets", "get_outlets", ("SPK", "TEST"), "GET", JSON),
    (
        "outlets.virtual_outlets",
        "get_virtual_outlet",
        ("SPK", "TEST", "OUTLET"),
        "GET",
        JSON,
    ),
    ("outlets.virtual_outlets", "get_virtual_outlets", ("SPK", "TEST"), "GET", JSON),
    ("outlets.virtual_outlets", "store_virtual_outlet", (DATA,), "POST", JSON),
    ("projects.project_lock_rights", "get_project_lock_rights", ("SPK",), "GET", JSON),
    (
        "projects.project_locks",
        "get_project_lock",
        ("SPK", "TEST", "TEST_APP"),
        "GET",
        JSON,
    ),
    ("projects.project_locks", "get_project_locks", ("SPK",), "GET", JSON),
    ("projects.project_locks", "request_project_lock", (DATA,), "POST", JSON),
    (
        "projects.water_supply.accounting",
        "store_pump_accounting",
        ("SPK", "TEST", "TEST_USER", "TEST_CONTRACT", DATA),
        "POST",
        JSON,
    ),
    (
        "timeseries.timeseries_profile",
        "get_timeseries_profile",
        ("SPK", "TEST", "Elev"),
        "GET",
        JSON,
    ),
    (
        "timeseries.timeseries_profile",
        "get_timeseries_profiles",
        ("SPK", "TEST", "Elev"),
        "GET",
        JSON,
    ),
    (
        "timeseries.timeseries_profile",
        "store_timeseries_profile",
        ("{}",),
        "POST",
        JSON,
    ),
    (
        "timeseries.timeseries_profile_instance",
        "get_timeseries_profile_instance",
        ("SPK", "TEST", "Elev", "TEST", "ft", None, None, None),
        "GET",
        JSON,
    ),
    (
        "timeseries.timeseries_profile_instance",
        "get_timeseries_profile_instances",
        ("SPK", "TEST", "Elev", "TEST"),
        "GET",
        JSON,
    ),
    (
        "timeseries.timeseries_profile_instance",
        "store_timeseries_profile_instance",
        ("{}", "TEST", DATE),
        "POST",
        JSON,
    ),
    (
        "timeseries.timeseries_profile_parser",
        "get_timeseries_profile_parser",
        ("SPK", "TEST", "Elev"),
        "GET",
        JSON,
    ),
    (
        "timeseries.timeseries_profile_parser",
        "get_timeseries_profile_parsers",
        ("SPK", "TEST", "Elev"),
        "GET",
        JSON,
    ),
    (
        "timeseries.timeseries_profile_parser",
        "store_timeseries_profile_parser",
        ("{}",),
        "POST",
        JSON,
    ),
    ("users.users", "store_user", ("test_user", "SPK", ["CWMS User"]), "POST", JSON),
    ("users.users", "update_user", ("test_user", "SPK", ["CWMS User"]), "POST", JSON),
    (
        "users.users",
        "delete_user_roles",
        ("test_user", "SPK", ["CWMS User"]),
        "DELETE",
        JSON,
    ),
    # Keep mixed-format resources covered on both sides of their CRUD contract.
    (
        "levels.location_levels",
        "get_location_level",
        ("TEST", "SPK", DATE),
        "GET",
        JSON_V2,
    ),
    ("locations.physical_locations", "get_location", ("TEST", "SPK"), "GET", JSON_V2),
    ("catalog.clobs", "get_clob", ("TEST", "SPK"), "GET", JSON_V2),
    ("catalog.blobs", "get_blobs", (), "GET", JSON_V2),
    ("catalog.blobs", "update_blob", (DATA,), "PATCH", JSON),
    ("projects.projects", "get_project", ("SPK", "TEST"), "GET", JSON),
]


class RequestCaptured(Exception):
    """Stop after requests prepares the wire request, before any network I/O."""


@pytest.mark.parametrize(
    "module_name,function_name,args,method,media_type",
    CASES,
    ids=[case[1] for case in CASES],
)
def test_endpoint_media_type(
    monkeypatch, module_name, function_name, args, method, media_type
):
    module = import_module(f"cwms.{module_name}")
    # update_user reads the existing roles before posting additional roles.
    if function_name == "update_user":
        monkeypatch.setattr(module, "get_user", lambda _: {"roles": {"SPK": []}})

    def capture(request, **kwargs):
        assert request.method == method
        header = "Accept" if method == "GET" else "Content-Type"
        assert request.headers[header] == media_type
        raise RequestCaptured

    monkeypatch.setattr(api.SESSION, "send", capture)
    endpoint_function = getattr(module, function_name)
    with pytest.raises(RequestCaptured):
        endpoint_function(*args)
