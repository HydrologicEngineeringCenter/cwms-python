import argparse
import json
import os
from getpass import getpass

import cwms
from cwms.api import ApiError


def parse_args():
    parser = argparse.ArgumentParser(
        description="Check CWMS token auth by fetching the authenticated user profile."
    )
    parser.add_argument(
        "--api-root",
        default=os.getenv("CDA_API_ROOT"),
        help="CWMS Data API root URL.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if not args.api_root:
        print(
            "Error: API root URL is required. Provide it with --api-root or set CDA_API_ROOT environment variable."
        )
        raise SystemExit(1)
    token = getpass("Paste Keycloak access token: ").strip()
    if not token:
        raise SystemExit("No token provided.")

    cwms.init_session(api_root=args.api_root, token=token)

    try:
        profile = cwms.api.get("auth/profile", api_version=1)
        endpoint = "auth/profile"
    except ApiError as error:
        if getattr(error.response, "status_code", None) != 404:
            raise
        profile = cwms.get_user_profile()
        endpoint = "user/profile"

    print(f"Fetched profile from {endpoint}:")
    print(json.dumps(profile, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
