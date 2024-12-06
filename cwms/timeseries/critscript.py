import re

import pandas as pd

import cwms


def crit_script(
    file_path,
    office_id,
    group_id="SHEF Data Acquisition",
    category_id="Data Aquisition",
    group_office_id="CWMS",
):
    def parse_crit_file(file_path):
        """
        Parses a .crit file into a dictionary containing timeseries ID and Alias.

        Parameters
        ----------
            file_path : str
                   Path to the .crit file.
            office_id : str
                The ID of the office associated with the specified timeseries.
            group_id : str
                The specified group associated with the timeseries data. Defaults to "SHEF Data Acquisition".
            category_id: string
                The category id that contains the timeseries group. Defaults to "Data Acquisition".
            group_office_id : str
                The specified office group associated with the timeseries data. Defaults to "CWMS".
        Returns
        -------
        list of dict
            A list of dictionaries containing the parsed key-value pairs, each with Alias and Timeseries ID.
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
                        "Alias": alias + ":" + alias2,
                        "Timeseries ID": timeseries_id,
                    }
                )

        return parsed_data

    def append_df(df: pd.DataFrame, office_id: str, tsId: str, alias: str):
        """
        Appends a row to the DataFrame.

        Parameters
        ----------
            df : pandas.DataFrame
                The DataFrame to append to.
            office_id : str
                The ID of the office associated with the specified timeseries.
            tsId : str
                The timeseries ID from the file.
            alias : str
                The alias from the file.
        Returns
        -------
        pandas.DataFrame
            The updated DataFrame.
        """
        data = {
            "officeId": [office_id],
            "timeseriesId": [tsId],
            "aliasId": [alias],
            "tsCode": ["none"],  # Default value for ts-code
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
    json_dict = cwms.timeseries_group_df_to_json(
        df, group_id, group_office_id, category_id
    )

    # Print DataFrame for verification
    pd.set_option("display.max_columns", None)

    cwms.update_timeseries_groups(
        group_id=group_id,
        office_id=office_id,
        category_id=category_id,
        replace_assigned_ts=None,
        data=json_dict,
    )
