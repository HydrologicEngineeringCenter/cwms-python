import json
from typing import List

import cwms.api as api


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


def _delete_user_roles(user_name: str, office_id: str, roles: List[str]) -> None:
    """Delete user roles for an office with a DELETE request body."""

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
    user = api.get(f"users/{user_name}", api_version=1)

    existing_roles = user.get("roles", {}).get(office_id, [])
    desired_roles = sorted(set(roles))
    current_roles = sorted(set(existing_roles))
    # Determine roles to add and remove
    roles_to_remove = [role for role in current_roles if role not in desired_roles]
    roles_to_add = [role for role in desired_roles if role not in current_roles]

    if roles_to_remove:
        _delete_user_roles(user_name, office_id, roles_to_remove)
    if roles_to_add:
        api.post(endpoint, roles_to_add)
