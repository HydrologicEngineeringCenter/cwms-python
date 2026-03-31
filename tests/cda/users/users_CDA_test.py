import os

import pytest

import cwms

TEST_USER_NAME = os.getenv("CWMS_TEST_USER_NAME", "l2hectest")
TEST_OFFICE_ID = os.getenv("CWMS_TEST_OFFICE_ID", "SPK")
FORBIDDEN_LOOKUP_USER_NAME = os.getenv("CWMS_FORBIDDEN_LOOKUP_USER_NAME", "q0hectest")


@pytest.fixture(autouse=True)
def init_session(request):
    api_root = request.config.getoption("api_root")
    api_key = request.config.getoption("user_admin_api_key")
    cwms.api.init_session(api_root=api_root, api_key=api_key)
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


def _replace_roles_for_office(user_name, office_id, desired_roles):
    # Replace the roles for the given office with the desired set.
    # This is used to restore state after tests that modify roles.
    current_roles = _roles_for_office(_get_test_user(user_name), office_id)
    if desired_roles:
        cwms.update_user(user_name=user_name, office_id=office_id, roles=desired_roles)
        current_roles = [role for role in current_roles if role not in desired_roles]
    if current_roles:
        cwms.delete_user_roles(
            user_name=user_name,
            office_id=office_id,
            roles=current_roles,
        )


def test_get_roles():
    # /roles
    # Covers the baseline read case where the caller can enumerate the available role catalog
    # and CDA returns at least one assignable role.
    roles = cwms.get_roles()
    assert isinstance(roles, list)
    assert len(roles) > 0
    assert "CWMS User Admins" in roles


def test_get_user_profile():
    # /users/profile
    # Covers fetching the currently authenticated user's own profile rather than an arbitrary user
    # record proving CDA returns identity data for the active session.
    profile = cwms.get_user_profile()
    assert isinstance(profile, dict)
    assert "user-name" in profile
    assert profile["user-name"].lower() == "q0hectest"


def test_get_user():
    # /users/{user-id}
    # Covers looking up one known test account by username and confirms the response resolves to
    # that exact user ignoring case differences in the identifier.
    user_name = str(TEST_USER_NAME)
    user = cwms.get_user(user_name)
    assert user["user-name"].lower() == user_name.lower()


def test_get_user_403_has_friendly_message_for_missing_roles(request):
    api_root = request.config.getoption("api_root")
    non_admin_api_key = request.config.getoption("user_non_admin_api_key")
    user_name = str(FORBIDDEN_LOOKUP_USER_NAME)

    cwms.api.init_session(api_root=api_root, api_key=non_admin_api_key)

    with pytest.raises(cwms.PermissionError) as error:
        cwms.get_user(user_name)

    assert str(error.value) == (
        f"User '{user_name}' retrieval could not be completed because the current credentials "
        "are not authorized for user-management access or are missing the required "
        "role assignment. CDA responded with 403 Forbidden."
    )


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


def test_get_users_by_office():
    # /users?office={office_id}
    # Covers filtering the user list by office and verifies the known test account appears in the
    # returned page when filtering by the test user's office.
    user_name = str(TEST_USER_NAME)
    office_id = TEST_OFFICE_ID
    users = cwms.get_users(office_id=office_id, page_size=200).json
    users_list = users.get("users", [])
    assert isinstance(users_list, list)
    assert any(
        str(u.get("user-name", "")).lower() == user_name.lower() for u in users_list
    )
    for user in users["users"]:
        roles = user["roles"]
        assert set(roles.keys()) == {
            office_id
        }, f"{user['user-name']} has unexpected office keys: {set(roles.keys())}"


def test_get_users_by_office_not_present_in_other_office():
    # /users?office={office_id}
    # Covers filtering the user list by office and verifies the known test account does not appear in the
    # returned page when filtering by a different office.
    user_name = str(TEST_USER_NAME)
    office_id = "MVP" if TEST_OFFICE_ID != "MVP" else "SPK"
    users = cwms.get_users(office_id=office_id, page_size=200)
    users_list = users.json.get("users", [])
    assert isinstance(users_list, list)
    assert not any(
        str(u.get("user-name", "")).lower() == user_name.lower() for u in users_list
    )


def test_get_users_with_paging():
    # /users with paging parameters
    # Covers requesting a specific page and page size and verifies the response contains the expected pagination metadata.
    page_size = 2
    users_with_page = cwms.get_users(page_size=page_size)
    users_list = users_with_page.json.get("users", [])
    users_without_page = cwms.get_users()
    all_users_list = users_without_page.json.get("users", [])
    assert isinstance(users_list, list)
    assert isinstance(all_users_list, list)
    assert len(users_list) >= page_size
    assert len(all_users_list) == len(users_list)


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
        _replace_roles_for_office(user_name, office_id, original_roles)

    restored_user = cwms.get_user(user_name)
    restored_roles = _roles_for_office(restored_user, office_id)
    assert sorted(set(restored_roles)) == sorted(set(original_roles))


def test_update_user_sets_only_the_specific_roles_requested():
    # Ensure the roles being set, set them and then confirm there are no extras
    user_name = str(TEST_USER_NAME)
    office_id = TEST_OFFICE_ID

    existing_user = _get_test_user(user_name)
    original_roles = list(_roles_for_office(existing_user, office_id))
    all_roles = cwms.get_roles()
    desired_role = next(
        (role for role in original_roles if role in all_roles),
        next(iter(all_roles), None),
    )
    assert (
        desired_role is not None
    ), "No roles available to build a selective update test."

    desired_roles = [desired_role]

    try:
        cwms.update_user(user_name=user_name, office_id=office_id, roles=desired_roles)

        updated_user = cwms.get_user(user_name)
        updated_roles = _roles_for_office(updated_user, office_id)

        assert sorted(set(updated_roles)) == sorted(set(desired_roles))
    finally:
        _replace_roles_for_office(user_name, office_id, original_roles)

    restored_user = cwms.get_user(user_name)
    restored_roles = _roles_for_office(restored_user, office_id)
    assert sorted(set(restored_roles)) == sorted(set(original_roles))
