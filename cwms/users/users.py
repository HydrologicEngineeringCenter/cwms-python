import json
from typing import Any, List, Optional

import cwms.api as api
from cwms.cwms_types import Data


def get_roles() -> List[str]:
    """Retrieve all available user-management roles."""

    response = api.get("roles", api_version=1)
    return list(response)


def get_user_profile() -> dict[str, Any]:
    """Retrieve the profile for the currently authenticated user."""

    return api.get("user/profile", api_version=1)


def get_users(
    office_id: Optional[str] = None,
    page: Optional[str] = None,
    page_size: Optional[int] = None,
) -> Data:
    """Retrieve users with optional office and paging filters."""

    params = {"office": office_id, "page": page, "page-size": page_size}
    response = api.get("users", params=params, api_version=1)
    return Data(response, selector="users")


def get_user(user_name: str) -> dict[str, Any]:
    """Retrieve a single user by user name."""

    if not user_name:
        raise ValueError("Get user requires a user name")
    return api.get(f"users/{user_name}", api_version=1)


def store_user(user_name: str, office_id: str, roles: List[str]) -> None:
    """Create a user role assignment for an office.

    Notes
    -----
    The CDA User Management API creates/manages user access through role assignment
    at `/user/{user-name}/roles/{office-id}`.
    """

    if not user_name:
        raise ValueError("Store user requires a user name")
    if not office_id:
        raise ValueError("Store user requires an office id")
    if not roles:
        raise ValueError("Store user requires a roles list")

    endpoint = f"user/{user_name}/roles/{office_id}"
    api.post(endpoint, roles)


def delete_user_roles(user_name: str, office_id: str, roles: List[str]) -> None:
    """Delete user role assignments for an office."""

    if not user_name:
        raise ValueError("Delete user roles requires a user name")
    if not office_id:
        raise ValueError("Delete user roles requires an office id")
    if roles is None:
        raise ValueError("Delete user roles requires a roles list")

    endpoint = f"user/{user_name}/roles/{office_id}"
    headers = {"accept": "*/*", "Content-Type": api.api_version_text(api.API_VERSION)}
    # TODO: Delete does not currently support a body in the api module. Use SESSION directly
    with api.SESSION.delete(
        endpoint, headers=headers, data=json.dumps(roles)
    ) as response:
        if not response.ok:
            raise api.ApiError(response)


def update_user(user_name: str, office_id: str, roles: List[str]) -> None:
    """Update a user's roles for an office by replacing the current role set."""

    if not user_name:
        raise ValueError("Update user requires a user name")
    if not office_id:
        raise ValueError("Update user requires an office id")
    if not roles:
        raise ValueError("Update user requires a roles list")

    endpoint = f"user/{user_name}/roles/{office_id}"
    user = get_user(user_name)

    roles_by_office = user.get("roles")
    if isinstance(roles_by_office, dict):
        existing_roles = roles_by_office.get(office_id, [])
    elif isinstance(roles_by_office, list):
        existing_roles = roles_by_office
    else:
        existing_roles = []

    if not isinstance(existing_roles, list):
        existing_roles = []

    desired_roles = sorted(set(roles))
    current_roles = sorted(set(existing_roles))
    # Determine roles to add and remove
    roles_to_remove = [role for role in current_roles if role not in desired_roles]
    roles_to_add = [role for role in desired_roles if role not in current_roles]

    if roles_to_remove:
        delete_user_roles(user_name, office_id, roles_to_remove)
    if roles_to_add:
        api.post(endpoint, roles_to_add)
