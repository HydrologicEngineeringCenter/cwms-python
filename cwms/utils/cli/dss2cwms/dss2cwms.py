# -*- coding: utf-8 -*-
"""
Command-line utility to transfer time series data from a HEC-DSS file to a CWMS
database.
Requires the hec-dssvue package to read HEC-DSS files and the cwms-python package
to interact with the CWMS database.

Copied from Michael M Perryman, Hydrologic Engineering Center examples:
https://github.com/HydrologicEngineeringCenter/hec-python-library/blob/main/examples/dss_to_cwms_db.py

"""
import os
import sys
from datetime import datetime
from typing import cast

import numpy as np


def run(
    dss_file_name: str,
    dss_start_time: datetime,
    dss_end_time: datetime,
    dss_time_series_pattern: str,
    cda_api_root: str,
    cda_api_key: str,
    cda_office_name: str,
    verify: bool,
):
    # Confirm the dss file exists for us to read (so we don't create one if it doesn't exist)
    if not os.path.exists(dss_file_name):
        print(
            f"HEC-DSS file not found: {dss_file_name}. You must provide a DSS file path."
        )
        sys.exit(1)

    from hec import CwmsDataStore, DssDataStore, HecTime, TimeSeries

    with DssDataStore.open(dss_file_name) as dss:
        # -------------------------------------------------------------------------------- #
        # If you don't set a time window for the DSS data store, the hecdss module will    #
        # retrieve all times for a specified time series, regardless of whether the D part #
        # of the pathname is specified or not.                                             #
        # -------------------------------------------------------------------------------- #
        if dss_start_time and dss_end_time:
            dss.time_window = (
                f"{dss_start_time.isoformat()}, {dss_end_time.isoformat()}"
            )
        data_set_names = dss.catalog(
            "timeseries", pattern=dss_time_series_pattern, condensed=True
        )  # one entry per time series
        with CwmsDataStore.open(
            api_root=cda_api_root,
            api_key=cda_api_key,
            office=cda_office_name,
            read_only=False,
        ) as db:
            if verify:
                db.time_zone = "UTC"
            for data_set_name in data_set_names:
                location_name = data_set_name.split("/")[B]
                if location_name.upper() not in cwms_location_names:
                    # location not in local set
                    catalog = db.catalog("location", pattern=location_name)
                    if catalog:
                        # location is in CWMS db, so add to local set
                        cwms_location_names.add(catalog[0].upper())
                    else:
                        # location is not in CWMS db
                        if location_name.upper() in new_locations_by_name:
                            # store the location to the CWMS db and add to local set
                            print(
                                f"==> Storing location {new_locations_by_name[location_name.upper()].name}"
                            )
                            # db.store(new_locations_by_name[location_name.upper()])
                            cwms_location_names.add(location_name.upper())
                        else:
                            # skip time series with unknown location
                            print(
                                f"==> Can't store time series {data_set_name} with unknown location: {location_name}"
                            )
                            continue
                # ------------------------------------- #
                # retrieve the time series from HEC-DSS #
                # ------------------------------------- #
                dss_retrieve_count += 1
                try:
                    ts = cast(TimeSeries, dss.retrieve(data_set_name))
                except Exception as e:
                    print(f"==> Error retrieving {data_set_name}\n\t{str(e)}")
                    dss_retrieve_errors += 1
                    continue
                print(f"Retrieved {ts}")
                if len(ts) == 0:
                    continue
                ts.context = "CWMS"
                if ts.time_zone is None:
                    ts.ilabel_as_time_zone(dss_time_zone, on_already_set=0)
                # ---------------------------------------------- #
                # Perform any renaming or other conversions here #
                # ---------------------------------------------- #
                ts.version = (
                    "FromArchive" if not ts.version else ts.version + "-FromArchive"
                )
                # -------------------------------- #
                # store the time series to CWMS db #
                # -------------------------------- #
                sys.stdout.write(f"\tStoring {ts}...")
                sys.stdout.flush()
                db_store_count += 1
                try:
                    # db.store(ts)
                    print("stored ts: ", ts)
                except Exception as e:
                    print(f"Error: {e}")
                    db_store_errors += 1
                    continue
                else:
                    print("done")
                if verify:
                    # ------------------------------------- #
                    # Retrieve the time series fom the CWMS #
                    # db and compare with what was stored   #
                    # ------------------------------------- #
                    sys.stdout.write("\tRetrieving...")
                    sys.stdout.flush()
                    db.time_zone = ts.time_zone  # type: ignore
                    db.start_time = HecTime(ts.times[0])
                    db.end_time = HecTime(ts.times[-1])
                    ts2 = cast(TimeSeries, db.retrieve(ts.name))
                    if len(ts2) != len(ts):
                        print(f"\tError: expected {len(ts)} values, got {len(ts2)}")
                        verify_errors += 1
                    elif ts2.times != ts.times:
                        print(f"\tError: times are different")
                        verify_errors += 1
                    elif not np.allclose(ts2.values, ts.values, equal_nan=True):
                        print(f"\tError: values are different")
                        verify_errors += 1
                    else:
                        print("\tVerified")

    print("\nRun Statistics:")
    print(f"{dss_retrieve_count:5d} time series to retrieve from {dss_file_name}")
    print(f"{dss_retrieve_errors:5d} errors retrieving time series")
    print(f"{db_store_count:5d} time series to store to {cda_api_root}")
    print(f"{db_store_errors:5d} errors storing time series")
    if verify:
        print(f"{verify_errors:5d} errors verifying stored time series")
