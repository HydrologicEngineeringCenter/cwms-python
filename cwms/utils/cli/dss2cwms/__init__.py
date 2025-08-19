# cwms/utils/cli/dss2cwms/main.py
import importlib
import sys

from . import dss2cwms  # OK to import this if it does not import `hec` at top-level
from .utils.args import get_args


def require_hec_or_exit():
    # TODO: Check versions of python (Requires 3.9 atm)
    # TODO: Check min version of hecdss
    try:
        importlib.import_module("hec")
    except ModuleNotFoundError:
        msg = (
            "hec-python-library (package:hec-python-library) is required for this command.\n"
            "Install with:\n"
            "\tpip install hec-python-library\n"
            "\t\tor\n"
            "\tpython -m pip install hec-python-library\n"
        )
        print(msg, file=sys.stderr)
        sys.exit(2)


def main():
    args = get_args()  # this prints usage/errors like: --dss and command required
    # Ensure only commands that use DSS require the hec package
    if args.command in {"fixed", "lookback", "monitor"}:
        require_hec_or_exit()

    # Confirm the required fields are met even if they are not in the env vars
    if not args.cda_api_key:
        print("CDA_API_KEY is required. Set this via env vars or command line args.")
        sys.exit(1)
    if not args.cda_api_root:
        print("CDA_API_ROOT is required. Set this via env vars or command line args.")
        sys.exit(1)

    dss2cwms.run(
        dss_file_name=args.dss,
        dss_start_time=args.begin,
        dss_end_time=args.end,
        dss_time_series_pattern=args.time_series_pattern,
        cda_api_root=args.cda_api_root,
        cda_api_key=args.cda_api_key,
        cda_office_name=args.office,
        verify=args.dt,
    )
