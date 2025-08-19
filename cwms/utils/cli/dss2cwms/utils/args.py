import argparse
import os


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load data from a HEC-DSS file into a CWMS database."
    )

    # Read environment options for defaults
    CDA_API_KEY = os.getenv("CDA_API_KEY")
    if CDA_API_KEY:
        print(f"Using CDA API key from environment variable.")
    CDA_API_ROOT = os.getenv("CDA_API_ROOT")
    if CDA_API_ROOT:
        print(f"Using CDA API root from environment variable.")
    # Global options
    parser.add_argument(
        "--dss",
        required=True,
        metavar="dss_file_name",
        help="The pathname of the HEC-DSS file to use.",
    )
    parser.add_argument(
        "--inst",
        metavar="instance_name",
        help="Instance name to differentiate logs/shutdown files.",
    )
    parser.add_argument(
        "--tz",
        metavar="time_zone",
        help="Time zone for interpreting times if not in records.",
    )
    parser.add_argument(
        "--id",
        metavar="identifier",
        help="Identifier for shadow HEC-DSS file. Defaults to process ID.",
    )
    parser.add_argument(
        "--mf",
        metavar="mapping_file_name",
        help="Mapping file: mutually exclusive with --ff.",
    )
    parser.add_argument(
        "--ff",
        metavar="filter_file_name",
        help="Filter file: mutually exclusive with --mf.",
    )
    parser.add_argument(
        "--dbf",
        metavar="db_file_name",
        help="HEC Password File with CWMS DB credentials.",
    )
    # ofc should be accepted but the help should display --office, for backwards compatibility
    parser.add_argument(
        "--office",
        "--ofc",
        metavar="office",
        help="Office for storing series in CWMS DB.",
        required=True,
    )
    parser.add_argument(
        "--cda_api_key",
        metavar="cda_api_key",
        help="API key for writing to CDA.",
        default=CDA_API_KEY,
    )
    parser.add_argument(
        "--cda_api_root",
        metavar="cda_api_root",
        help="API root URL for CDA.",
        default=CDA_API_ROOT,
    )
    parser.add_argument(
        "--logd",
        metavar="log_dir_name",
        help="Directory for log files (default: execution directory).",
    )
    parser.add_argument(
        "--mlad",
        type=int,
        metavar="max_log_age_days",
        help="Max age (days) for logs before deletion.",
    )
    parser.add_argument(
        "--vlvl",
        type=int,
        metavar="verbosity_level",
        choices=[0, 1, 2],
        default=1,
        help="Verbosity: 0=almost none, 1=normal, 2=lots (default: 1).",
    )

    parser.add_argument(
        "--time_series_pattern",
        metavar="time_series_pattern",
        help="pattern (Optional[str], must be passed by name): Wildcard pattern (using `*` and `?`) to use for matching pathnames. `regex` takes precedence if both are specified. Defaults to None.",
        default="*",
    )

    # Subcommands
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Fixed retrieval subcommand
    fixed_parser = subparsers.add_parser(
        "fixed", help="Run a fixed retrieval period from HEC-DSS file."
    )
    fixed_parser.add_argument(
        "--begin",
        "--tws",
        metavar="begin",
        help="Start time (ISO 8601 format: YYYY-MM-DDTHH:MM:SS).",
    )
    fixed_parser.add_argument(
        "--end",
        "--twe",
        metavar="end",
        help="End time (ISO 8601 format: YYYY-MM-DDTHH:MM:SS).",
    )
    fixed_parser.add_argument(
        "--dt",
        action="store_true",
        help="Interpret fixed retrieval as data times (default: update times).",
    )

    # Look-back subcommand
    lookback_parser = subparsers.add_parser(
        "lookback", help="Retrieve data from look-back period until now."
    )
    lookback_parser.add_argument(
        "--lbh",
        type=int,
        required=True,
        metavar="look_back_hours",
        help="Hours before current time to start retrieval.",
    )
    lookback_parser.add_argument(
        "--dt", action="store_true", help="Interpret fixed retrieval as data times."
    )

    # Monitor subcommand
    monitor_parser = subparsers.add_parser(
        "monitor", help="Monitor HEC-DSS file for updates."
    )
    monitor_parser.add_argument(
        "--mon", action="store_true", default=True, help="Enable monitoring mode."
    )

    args = parser.parse_args()

    # Global validation
    if args.mf and args.ff:
        parser.error("Arguments --mf and --ff are mutually exclusive.")

    return args
