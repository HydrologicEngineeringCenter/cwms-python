import argparse
import base64
import json
import logging
import mimetypes
import os
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler

import requests

import cwms.api as api
import cwms.catalog.blobs as blobs
import cwms.timeseries.timeseries as timeseries

# Windows SSL Error?
# Run: pip install pip_system_certs

# Setup logging
home_dir = os.path.expanduser("~")
LOG_DIR = os.path.join(home_dir, "logs")
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

log_path = os.path.join(LOG_DIR, "cwms_blob_uploader.log")


def get_media_type(file_path: str) -> str:
    mime_type, _ = mimetypes.guess_type(file_path)
    return mime_type or "application/octet-stream"


def main(args: list[str] | None = None):
    if args is None:
        args = sys.argv[1:]

    # Skip the subcommand if it's passed in
    if len(args) > 0 and args[0] == "store_file":
        args = args[1:]
    parser = argparse.ArgumentParser(description="Upload a file as blob to CWMS.")
    # TODO: Should this also take a web url for input files?
    parser.add_argument("input_file", help="Path to the input file")
    parser.add_argument("output_id", help="ID to use for the stored blob")
    parser.add_argument(
        "--description", help="Optional description of the file", default=None
    )
    parser.add_argument(
        "--media-type", help="Optional media type of the file", default=None
    )
    parser.add_argument(
        "--office-id",
        help="Optional office ID",
        default=os.getenv("OFFICE")
        or os.getenv("OFFICE_ID")
        or os.getenv("CDA_OFFICE"),
    )
    parser.add_argument(
        "--api-root",
        help="API root URL",
        dest="api_root",
        required=False,
        default=os.getenv("APIROOT")
        or os.getenv("API_ROOT")
        or os.getenv("CDA_HOST")
        or os.getenv("CDA_API_ROOT"),
    )
    parser.add_argument(
        "--api-key",
        help="API key",
        dest="api_key",
        required=False,
        default=os.getenv("APIKEY") or os.getenv("API_KEY") or os.getenv("CDA_API_KEY"),
    )
    parser.add_argument(
        "--log-level",
        help="Sets the log output level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        required=False,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )

    args = parser.parse_args(args)

    # Verify required arguments
    if not args.input_file or not os.path.isfile(args.input_file):
        logging.error("Input file is required and must be a valid file path.")
        sys.exit(1)
    if not args.api_root:
        logging.error(
            "API root is required. Set the CDA_API_ROOT environment variable or use --api-root."
        )
        sys.exit(1)
    if not args.api_key:
        logging.error(
            "API key is required. Set the CDA_API_KEY environment variable or use --api-key."
        )
        sys.exit(1)
    if not args.office_id:
        logging.error(
            "Office ID is required. Set the OFFICE environment variable or use --office-id."
        )
        sys.exit(1)
    # Setup root logger
    logger = logging.getLogger()
    logger.setLevel(args.log_level)

    # File handler with rotation: 5 MB per file, keep 5 backups
    file_handler = RotatingFileHandler(
        log_path, maxBytes=5 * 1024 * 1024, backupCount=5
    )
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(args.log_level)
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(console_handler)

    try:
        file_size = os.path.getsize(args.input_file)
        with open(args.input_file, "rb") as f:
            file_data = f.read()
        logging.info(f"Read file: {args.input_file} ({file_size} bytes)")
    except Exception as e:
        logging.error(f"Failed to read file: {e}")
        sys.exit(1)

    logging.debug("CDA_API_ROOT: " + args.api_root)
    logging.debug("CDA_API_KEY: ***...***" + args.api_key[-6:])
    if not args.api_root.endswith("/"):
        args.api_root += "/"

    # Establish CDA session with provided API root and key
    api.init_session(api_root=args.api_root, api_key=args.api_key)

    blob: dict[str, str] = {
        "office-id": "SWT",
        "id": args.output_id,
        "media-type-id": get_media_type(args.input_file),
        "value": base64.b64encode(file_data).decode("utf-8"),
    }
    # Only store description if one is provided
    if args.description:
        blob["description"] = args.description
    try:
        blobs.store_blobs(blob, fail_if_exists=False)
        logging.info(f"Successfully stored blob with ID: {args.output_id.upper()}")
        logging.info(
            f"View: {args.api_root}blobs/{args.output_id.upper()}?office={args.office_id}"
        )
    except Exception as e:
        logging.error(f"Failed to store blob: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Run when this file is executed directly
    main()
