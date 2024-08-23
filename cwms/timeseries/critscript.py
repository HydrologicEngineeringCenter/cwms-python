import json
import re
from typing import Optional

import pandas as pd


def crit_script(file_path, office_id, group_id):
    def update_timeseries_groups(
        group_id: str, office_id: str, replace_assigned_ts: Optional[bool] = False
    ) -> None:
        """
        Updates the timeseries groups with the provided group ID and office ID.

        Parameters
        ----------
        group_id : str
            The new specified timeseries ID that will replace the old ID.
        office_id : str
            The ID of the office associated with the specified level.
        replace_assigned_ts : bool, optional
            Specifies whether to unassign all existing time series before assigning new time series specified in the content body. Default is False.

        Returns
        -------
        None
        """
        if not group_id:
            raise ValueError("Cannot update a specified level without an id")
        if not office_id:
            raise ValueError("Cannot update a specified level without an office id")

        endpoint = f"timeseries/group/{group_id}"
        params = {
            "replace-assigned-ts": replace_assigned_ts,
            "office": office_id,
        }

        # Assuming api.patch is a valid function available in your environment
        api.patch(endpoint=endpoint, params=params)

    def timeseries_group_df_to_json(data: pd.DataFrame, group_id: str) -> json:
        """
        Converts a dataframe to a json dictionary in the correct format.

        Parameters
        ----------
        data: pd.DataFrame
            Dataframe containing timeseries information.
        group_id: str
            The group ID for the timeseries.

        Returns
        -------
        json
            JSON dictionary of the timeseries data.
        """
        required_columns = ["office-id", "ts-id", "alias", "ts-code", "attribute"]
        for column in required_columns:
            if column not in data.columns:
                raise TypeError(
                    f"{column} is a required column in data when posting as a dataframe"
                )

        if data.isnull().values.any():
            raise ValueError("Null/NaN data must be removed from the dataframe")

        json_dict = {
            "office-id": office_id,
            "id": group_id,
            "time-series-category": {"office-id": office_id, "id": "Data Acquisition"},
            "time-series": [],
        }

        for _, row in data.iterrows():
            ts_dict = {
                "office-id": row["office-id"],
                "id": row["ts-id"],
                "alias": row["alias"],
                "ts-code": row["ts-code"],
                "attribute": row["attribute"],
            }
            json_dict["time-series"].append(ts_dict)

        return json_dict

    def parse_crit_file(file_path):
        """
        Parses a .crit file into a dictionary. Parses for the timeseries ID and Alias

        :param file_path: Path to the .crit file
        :return: A dictionary containing the parsed key-value pairs
        """
        parsed_data = []
        with open(file_path, "r") as file:
            for line in file:
                # Ignore comment lines and empty lines
                if line.startswith("#") or not line.strip():
                    continue

                # Extract alias, timeseries ID, and TZ
                match = re.match(r"([^=]+)=([^;]+);(.+)", line.strip())
                if match:
                    alias = match.group(1).strip()
                    timeseries_id = match.group(2).strip()
                    alias2 = match.group(3).strip()

                    parsed_data.append(
                        {
                            "Alias": alias + " " + alias2,
                            "Timeseries ID": timeseries_id,
                        }
                    )

        return parsed_data

    def append_df(df: pd.DataFrame, office_id: str, tsId: str, alias: str):
        """
        Appends a row to the DataFrame

        :param df: The DataFrame to append to
        :param office_id: The office ID
        :param tsId: The timeseries ID
        :param alias: The alias
        :return: The updated DataFrame
        """
        data = {
            "office-id": [office_id],
            "ts-id": [tsId],
            "alias": [alias],
            "ts-code": ["none"],  # Default value for ts-code
            "attribute": [0],  # Default value for attribute
        }
        df = pd.concat([df, pd.DataFrame(data)])
        return df

    # Parse the file and get the parsed data
    parsed_data = parse_crit_file(file_path)

    df = pd.DataFrame()

    for data in parsed_data:
        # Create DataFrame for the current row
        df = append_df(df, office_id, data["Timeseries ID"], data["Alias"])

    # Generate JSON dictionary
    json_dict = timeseries_group_df_to_json(df, group_id)

    # Print DataFrame for verification
    pd.set_option("display.max_columns", None)
    print(df)
    print(json_dict)

    update_timeseries_groups(group_id, office_id, json_dict)


json_dict = crit_script("CEMVP_GOES.crit", "CWMS", "Data Acquisition")