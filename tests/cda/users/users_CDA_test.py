import os

import pytest

import cwms
import cwms.api

TEST_USER_NAME = os.getenv("CWMS_TEST_USER_NAME", "q0hectest")
TEST_OFFICE_ID = os.getenv("CWMS_TEST_OFFICE_ID", "HEC")


@pytest.fixture(autouse=True)
def init_session(request):
    print("Initializing CWMS API session for user management CDA tests...")


@pytest.mark.skipif(
    not TEST_USER_NAME,
    reason="Set CWMS_TEST_USER_NAME to run user management CDA tests.",
)
def test_store_and_update_user_roles_roundtrip():
    user_name = str(TEST_USER_NAME)
    office_id = TEST_OFFICE_ID

    existing_user = cwms.api.get(f"users/{user_name}")
    original_roles = list(existing_user.get("roles", {}).get(office_id, []))

    all_roles = cwms.api.get("roles")
    role_to_add = next((role for role in all_roles if role not in original_roles), None)
    if role_to_add is None:
        pytest.skip(
            f"No unassigned roles available for {user_name} in office {office_id}."
        )

    try:
        cwms.store_user(user_name=user_name, office_id=office_id, roles=[role_to_add])
        updated_user = cwms.api.get(f"users/{user_name}")
        updated_roles = updated_user.get("roles", {}).get(office_id, [])
        assert role_to_add in updated_roles
    finally:
        cwms.update_user(user_name=user_name, office_id=office_id, roles=original_roles)

    restored_user = cwms.api.get(f"users/{user_name}")
    restored_roles = restored_user.get("roles", {}).get(office_id, [])
    assert sorted(set(restored_roles)) == sorted(set(original_roles))
