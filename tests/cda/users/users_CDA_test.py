import os

import pytest

import cwms

TEST_USER_NAME = os.getenv("CWMS_TEST_USER_NAME", "l2hectest")
TEST_OFFICE_ID = os.getenv("CWMS_TEST_OFFICE_ID", "SPK")


@pytest.fixture(autouse=True)
def init_session(request):
    print("Initializing CWMS API session for user management CDA tests...")


def _roles_for_office(user_json, office_id):
    # CDA has returned roles both as an office-keyed mapping and as a flat list.
    # Normalize both shapes so the role assertions below are about behavior, not payload format.
    roles = user_json.get("roles", {})
    if isinstance(roles, dict):
        office_roles = roles.get(office_id, [])
        return office_roles if isinstance(office_roles, list) else []
    if isinstance(roles, list):
        return roles
    return []


def _get_test_user(user_name):
    return cwms.get_user(user_name)


def test_get_roles():
    # /roles
    # Covers the baseline read case where the caller can enumerate the available role catalog
    # and CDA returns at least one assignable role.
    roles = cwms.get_roles()
    assert isinstance(roles, list)
    assert len(roles) > 0


def test_get_user_profile():
    # /users/profile
    # Covers fetching the currently authenticated user's own profile rather than an arbitrary user
    # record proving CDA returns identity data for the active session.
    profile = cwms.get_user_profile()
    assert isinstance(profile, dict)
    assert "user-name" in profile


def test_get_user():
    # /users/{user-id}
    # Covers looking up one known test account by username and confirms the response resolves to
    # that exact user ignoring case differences in the identifier.
    user_name = str(TEST_USER_NAME)
    user = _get_test_user(user_name)
    assert user["user-name"].lower() == user_name.lower()


def test_get_users():
    # /users
    # Covers the paginated user-list endpoint and verifies the known test account appears in the
    # returned page when requesting a large enough page size.
    user_name = str(TEST_USER_NAME)
    users = cwms.get_users(page_size=200)
    users_list = users.json.get("users", [])
    assert isinstance(users_list, list)
    assert any(
        str(u.get("user-name", "")).lower() == user_name.lower() for u in users_list
    )


def test_store_update_delete_user_roles_roundtrip():
    # Round Trip: Covers the role-management lifecycle for an existing user in one office:
    # 1. add a role the user does not currently have
    # 2. verify it was stored
    # 3. remove that same role
    # 4. restore the exact original role set
    # Resetting the state back to original
    user_name = str(TEST_USER_NAME)
    office_id = TEST_OFFICE_ID

    existing_user = _get_test_user(user_name)
    original_roles = list(_roles_for_office(existing_user, office_id))

    all_roles = cwms.get_roles()
    role_to_add = next((role for role in all_roles if role not in original_roles), None)
    assert (
        role_to_add is not None
    ), f"No unassigned roles available for {user_name} in office {office_id}."

    try:
        cwms.store_user(user_name=user_name, office_id=office_id, roles=[role_to_add])
        updated_user = cwms.get_user(user_name)
        updated_roles = _roles_for_office(updated_user, office_id)
        assert role_to_add in updated_roles

        cwms.delete_user_roles(
            user_name=user_name,
            office_id=office_id,
            roles=[role_to_add],
        )
        deleted_user = cwms.get_user(user_name)
        deleted_roles = _roles_for_office(deleted_user, office_id)
        assert role_to_add not in deleted_roles
    finally:
        cwms.update_user(user_name=user_name, office_id=office_id, roles=original_roles)

    restored_user = cwms.get_user(user_name)
    restored_roles = _roles_for_office(restored_user, office_id)
    assert sorted(set(restored_roles)) == sorted(set(original_roles))
