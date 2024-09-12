import re

import pandas as pd

import cwms


#
def crit_script(file_path, office_id, group_id):
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
    json_dict = cwms.timeseries_group_df_to_json(
        df, "USGS CHEF Data Acquisition", "CWMS", "Data Acquisition"
    )

    # Print DataFrame for verification
    pd.set_option("display.max_columns", None)
    print(df)
    print(json_dict)

    cwms.update_timeseries_groups(group_id, office_id, json_dict)


crit_script("CEMVP_GOES.crit", "CWMS", "Data Acquisition")
