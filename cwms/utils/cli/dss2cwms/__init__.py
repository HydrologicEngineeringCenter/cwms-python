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

    dss2cwms.run(args)
