from cwms.utils import queryCDA, return_df
import cwms._constants as constants
from cwms.core import CwmsApiSession
from cwms.core import _CwmsBase
import pandas as pd
import json
import datetime


class CwmsTs(_CwmsBase):
    _TIMESERIES_ENDPOINT = "timeseries"
    _TIMESERIES_GROUP_ENDPOINT = "timeseries/group"

    def __init__(self, cwms_api_session: CwmsApiSession):
        super().__init__(cwms_api_session)

    def retrieve_ts_group_df(self, group_id: str, category_id: str, office_id: str) -> pd.DataFrame:
        """Retreives time series stored in the requested time series groupas a dataframe

       Parameters
        ----------
            group_id: string
                Timeseries group whose data is to be included in the response.
            category_id: string
                The category id that contains the timeseries group.
            office_id: string 
                The owning office of the timeseries group.

        Returns
        -------
        df : pandas.dataframe
            The pandas dataframe containing the time series group information.
        """

        responce = CwmsTs.retrieve_ts_group_json(
            self, group_id, category_id, office_id)

        df = return_df(responce, dict_key=['assigned-time-series'])

        return df

    def retrieve_ts_group_json(self, group_id: str, category_id: str, office_id: str) -> dict:
        """Retreives time series stored in the requested time series group as a dictionary

       Parameters
        ----------
            group_id: string
                Timeseries group whose data is to be included in the response.
            category_id: string
                The category id that contains the timeseries group.
            office_id: string 
                The owning office of the timeseries group.

        Returns
        -------
        response : dict
            The JSON response containing the time series group information.
        """

        end_point = f"{CwmsTs._TIMESERIES_GROUP_ENDPOINT}/{group_id}"

        params = {constants.OFFICE_PARAM: office_id,
                  "category-id": category_id}

        headerList = {"Accept": constants.HEADER_JSON_V1}

        responce = queryCDA(self, end_point, params, headerList)

        return responce

    def retrieve_ts_df(
        self,
        tsId: str,
        office_id: str,
        unit: str = 'EN',
        datum: str = None,
        begin: datetime = None,
        end: datetime = None,
        timezone: str = None,
        page_size: int = 500000,
    ) -> pd.DataFrame:
        """Retrieves time series data from a specified time series and time window.  
        Value date-times obtained are always in UTC.

        Parameters
        ----------
            tsId: string
                Name(s) of the time series whose data is to be included in the response.
            office_id: string
                The owning office of the time series(s).
            unit: string, optional, default is EN
                The unit or unit system of the response. Defaults to EN. Valid values 
                for the unit field are: 
                    1. EN. English unit system. 
                    2. SI. SI unit system. 
                    3. Other. 
            datum: string, optional, default is None
                The elevation datum of the response. This field affects only elevation location 
                levels. Valid values for this field are:  
                    1. NAVD88.  
                    2. NGVD29.  
            begin: datetime, optional, default is None 
                Start of the time window for data to be included in the response. If this field is 
                not specified, any required time window begins 24 hours prior to the specified 
                or default end time.
            end: datetime, optional, default is None
                End of the time window for data to be included in the response. If this field is 
                not specified, any required time window ends at the current time.
            timezone: str, optional, default is None: 
                Specifies the time zone of the values of the begin and end fields. UTC is the default. 
                This value does not impact the values in response.  response is always in UTC.
            page_size: int, optional, default is 5000000: Sepcifies the number of records to obtain in 
                a single call.

        Returns
        -------
        df : pandas.DataFrame
            The pandas dataframe containing the time series dates, values, and quality codes. Values are always in UTC. 
        """

        responce = CwmsTs.retrieve_ts_json(
            self, tsId, office_id, unit, datum, begin, end, timezone, page_size
        )

        df = return_df(responce, dict_key=['values'])

        return df

    def retrieve_ts_json(
        self,
        tsId: str,
        office_id: str,
        unit: str = 'EN',
        datum: str = None,
        begin: datetime = None,
        end: datetime = None,
        timezone: str = None,
        page_size: int = 500000,
    ) -> dict:
        """Retrieves time series data from a specified time series and time window.  Value date-times 
        obtained are always in UTC.

        Parameters
        ----------
            tsId: string
                Name(s) of the time series whose data is to be included in the response.
            office_id: string
                The owning office of the time series(s).
            unit: string, optional, default is EN
                The unit or unit system of the response. Defaults to EN. Valid values 
                for the unit field are: 
                    1. EN. English unit system. 
                    2. SI. SI unit system. 
                    3. Other. 
            datum: string, optional, default is None
                The elevation datum of the response. This field affects only elevation location 
                levels. Valid values for this field are:  
                    1. NAVD88.  
                    2. NGVD29.  
            begin: datetime, optional, default is None 
                Start of the time window for data to be included in the response. If this field is 
                not specified, any required time window begins 24 hours prior to the specified 
                or default end time.
            end: datetime, optional, default is None
                End of the time window for data to be included in the response. If this field is 
                not specified, any required time window ends at the current time.
            timezone: str, optional, default is None: 
                Specifies the time zone of the values of the begin and end fields. UTC is the default. 
                This value does not impact the values in response.  response is always in UTC.
            page_size: int, optional, default is 5000000: Sepcifies the number of records to obtain in 
                a single call.

        Returns
        -------
        response : dict
            The JSON response containing the time series information.  Values are always in UTC. 
        """

        # creates the dataframe from the timeseries data
        end_point = CwmsTs._TIMESERIES_ENDPOINT

        if begin is not None:
            begin = begin.strftime('%Y-%m-%dT%H:%M:%S')

        if end is not None:
            end = end.strftime('%Y-%m-%dT%H:%M:%S')

        params = {
            constants.OFFICE_PARAM: office_id,
            constants.NAME: tsId,
            constants.UNIT: unit,
            constants.DATUM: datum,
            constants.BEGIN: begin,
            constants.END: end,
            constants.TIMEZONE: timezone,
            constants.PAGE_SIZE: page_size,
        }

        headerList = {"Accept": constants.HEADER_JSON_V2}
        responce = queryCDA(self, end_point, params, headerList)

        return responce

    def write_ts(
        self,
        data,
        version_date: str = None,
        timezone: str = None,
        create_as_ltrs: bool = False,
        store_rule: str = None,
        override_protection: bool = False,
    ):
        """Will Create new TimeSeries if not already present.  Will store any data provided

        Parameters
        ----------
            data: pd.Dataframe, or Dictionary 
                Time Series data to be stored.  If dataframe data must be provided in the following format
                    df.tsId = timeseried id:specified name of the time series to be posted to
                    df.office = the owning office of the time series
                    df.units = units of values to be stored (ie. ft, in, m, cfs....)
                    dataframe should have three columns date-time, value, quality-code. date-time values 
                    can be a string in ISO8601 formate or a datetime field. if quality-code column is not 
                    present is will be set to 0.
                                            date-time value  quality-code
                    0   2023-12-20T14:45:00.000-05:00  93.1           0
                    1   2023-12-20T15:00:00.000-05:00  99.8           0
                    2   2023-12-20T15:15:00.000-05:00  98.5           0
                    3   2023-12-20T15:30:00.000-05:00  98.5           0
            version_date: str, optional, default is None
                The version date for the timeseries to create. The format for this field is ISO 8601 extended.
            timezone: str, optional, default is None)
                Specifies the time zone of the version-date field (unless otherwise specified). If this field is 
                not specified, the default time zone of UTC shall be used.  Ignored if version-date was specified 
                with offset and timezone.
            create_as_ltrs: bool, optional, defualt is False
                Flag indicating if timeseries should be created as Local Regular Time Series.
            store_rule: str, optional, default is None: 
                The business rule to use when merging the incoming with existing data. Available values : 
                    REPLACE_ALL, 
                    DO_NOT_REPLACE, 
                    REPLACE_MISSING_VALUES_ONLY, 
                    REPLACE_WITH_NON_MISSING, 
                    DELETE_INSERT.
            override_protection: str, optional, default is False
                A flag to ignore the protected data quality when storing data.

        Returns
        -------
        response
        """

        end_point = CwmsTs._TIMESERIES_ENDPOINT
        params = {
            'version-date': version_date,
            constants.TIMEZONE: timezone,
            'create-as-lrts': create_as_ltrs,
            'store-rule': store_rule,
            'override-protection': override_protection,
        }
        headerList = {
            'accept': '*/*',
            'Content-Type': constants.HEADER_JSON_V2,
        }
        if isinstance(data, pd.DataFrame):
            # grab time series information
            tsId = data.tsId
            office = data.office
            units = data.units

            # check dataframe columns
            if 'quality-code' not in data:
                data['quality-code'] = 0
            if 'date-time' not in data:
                raise TypeError(
                    "date-time is a required column in data when posting as a dateframe"
                )
            if 'value' not in data:
                raise TypeError(
                    "value is a required column when posting data when posting as a dataframe"
                )

            # make sure that dataTime column is in iso8601 formate.
            data['date-time'] = pd.to_datetime(data['date-time']).apply(
                pd.Timestamp.isoformat
            )
            data = data.reindex(columns=['date-time', 'value', 'quality-code'])
            if data.isnull().values.any():
                raise ValueError(
                    "Null/NaN data must be removed from the dataframe")

            ts_dict = {
                "name": tsId,
                "office-id": office,
                "units": units,
                "values": data.values.tolist(),
            }

        elif isinstance(data, dict):
            ts_dict = data

        else:
            raise TypeError("data is not of type dataframe or dictionary")

        # print(ts_dict)
        response = self.get_session().post(
            end_point, params=params, headers=headerList, data=json.dumps(
                ts_dict)
        )
        return response
