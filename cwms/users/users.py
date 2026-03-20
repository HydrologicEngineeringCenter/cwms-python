import json
from typing import Any, List, Optional

import cwms.api as api
from cwms.cwms_types import Data


def _raise_user_management_error(error: api.ApiError, action: str) -> None:
    status_code = getattr(error.response, "status_code", None)
    if status_code == 403:
        response_hint = getattr(error.response, "reason", None) or "Forbidden"
        message = (
            f"{action} could not be completed because the current credentials "
            "are not authorized for user-management access or are missing the "
            f"required role assignment. CDA responded with 403 {response_hint}."
        )
        raise api.PermissionError(error.response, message) from None
    raise error


def get_roles() -> List[str]:
    """Retrieve all available user-management roles."""

    try:
        response = api.get("roles", api_version=1)
    except api.ApiError as error:
        _raise_user_management_error(error, "User role lookup")
    return list(response)


def get_user_profile() -> dict[str, Any]:
    """Retrieve the profile for the currently authenticated user."""

    response = api.get("user/profile", api_version=1)
    return dict(response)


def get_users(
    office_id: Optional[str] = None,
    username_like: Optional[str] = None,
    include_roles: Optional[bool] = None,
    page: Optional[str] = None,
    page_size: Optional[int] = 5000,
) -> Data:
    """Retrieve users with optional office and paging filters."""

    endpoint = "users"
    params = {
        "office": office_id,
        "username-like": username_like,
        "include-roles": include_roles,
        "page": page,
        "page-size": page_size,
    }
    try:
        response = api.get_with_paging(
            endpoint=endpoint, selector="users", params=params, api_version=1
        )
    except api.ApiError as error:
        _raise_user_management_error(error, "User list lookup")

    # filter by office if office_id is provided since the API does not
    # currently support filtering by office on the backend. This is a
    # temporary workaround until the API supports office filtering.
    if office_id:
        data = response
        filtered_users = [
            user for user in data["users"] if office_id in user.get("roles", {})
        ]
        response = {**data, "users": filtered_users, "total": len(filtered_users)}
    return Data(response, selector="users")


def get_user(user_name: str) -> dict[str, Any]:
    """Retrieve a single user by user name."""

    if not user_name:
        raise ValueError("Get user requires a user name")
    try:
        response = api.get(f"users/{user_name}", api_version=1)
    except api.ApiError as error:
        status_code = getattr(error.response, "status_code", None)
        if status_code == 404:
            raise api.NotFoundError(
                error.response, f"User '{user_name}' was not found."
            ) from None
        if status_code == 403:
            _raise_user_management_error(error, f"User '{user_name}' retrieval")
        raise
    return dict(response)


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
    try:
        api.post(endpoint, roles)
    except api.ApiError as error:
        _raise_user_management_error(
            error, f"User '{user_name}' role assignment update"
        )


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
            _raise_user_management_error(
                api.ApiError(response), f"User '{user_name}' role deletion"
            )


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
        try:
            api.post(endpoint, roles_to_add)
        except api.ApiError as error:
            _raise_user_management_error(error, f"User '{user_name}' role replacement")
