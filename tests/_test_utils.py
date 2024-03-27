#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC


import json
from pathlib import Path


def read_resource_file(file_name):
    current_path = Path(__file__).resolve().parent
    resource_path = current_path / "resources" / file_name

    with open(resource_path, "r") as file:
        data = json.load(file)

    return data
