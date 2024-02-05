from .utils import queryCDA
from ._constants import *
import pandas as pd
import json


class CwmsTsMixin:

    def retreive_ts_group(self,p_group_id,p_category_id,p_office_id, return_type='df'):
        """Retreives time series stored in the requested time series group
        
        Parameters
        -------
        p_group_id : str
            Specifies the timeseries group whose data is to be included in the response (required)
        p_category_id : str
            Specifies the category containing the timeseries group whose data is to be included in the response. (required)
        p_office_id : str
            Specifies the owning office of the timeseries group whose data is to be included in the response. (required)
        return_type : str
            output type to return values as. 1. 'df' will return a pandas dataframe. 2. 'dict' will return a json decoded dictionay. 3. all other values will return Responce object from request package.
        
        Returns
        -------
        pandas df, json decoded dictionay, or Responce object from request package

        Examples
        -------
        """
        endPoint = f'timeseries/group/{p_group_id}'

        params = {
            OFFICE_PARAM: p_office_id,
            "category-id": p_category_id
        }

        headerList={
            "Accept": HEADER_JSON_V1
        }

        responce = queryCDA(self, endPoint, params, headerList, return_type, dict_key = ['assigned-time-series'])

        #if dataframe:
        #    responce = pd.DataFrame(responce['assigned-time-series'])

        return responce

    def retrieve_ts(self, p_tsId, p_office_id, p_unit='EN', p_datum=None, p_start_date=None, p_end_date=None, p_timezone=None, p_page_size=500000, return_type='df'):
        """Retrieves time series data from a specified time series and time window.  Value date-times obtained are always in UTC.

        Parameters
        -----------
        p_tsId : str (required)
            Specifies the name(s) of the time series whose data is to be included in the response. A case insensitive comparison is used to match names.
        p_office_id : str
            Specifies the owning office of the time series(s) whose data is to be included in the response. If this field is not specified, matching location level information from all offices shall be returned.
        p_unit : str
            Specifies the unit or unit system of the response. Valid values for the unit field are: 1. EN. (default) Specifies English unit system. 2. SI.   Specifies the SI unit system. 3. Other. Any unit returned in the response to the units URI request that is appropriate for the requested parameters.
        p_datum: str
            Specifies the elevation datum of the response. This field affects only elevation location levels. Valid values for this field are:  1. NAVD88.  The elevation values will in the specified or default units above the NAVD-88 datum.  2. NGVD29.  The elevation values will be in the specified or default units above the NGVD-29 datum.
        p_start_date : datetime
            Specifies the start of the time window for data to be included in the response. If this field is not specified, any required time window begins 24 hours prior to the specified or default end time.
        p_end_date : datetime
            Specifies the end of the time window for data to be included in the response. If this field is not specified, any required time window ends at the current time.
        p_timezone : string
            Specifies the time zone of the values of the begin and end fields. UTC is the default.  This value does not impact the values in response.  response is always in UTC.  
        return_type : str
            output type to return values as. 1. 'df' will return a pandas dataframe. 2. 'dict' will return a json decoded dictionay. 3. all other values will return Responce object from request package.
        Returns
        --------
        pandas df, json decoded dictionay, or Responce object from request package.  Dates in responce are in UTC.  

        Examples
        -------
        """
        #creates the dataframe from the timeseries data
        endPoint = 'timeseries'
        if p_start_date is not None: p_start_date = p_start_date.strftime('%Y-%m-%dT%H:%M:%S')

        if p_end_date is not None: p_end_date = p_end_date.strftime('%Y-%m-%dT%H:%M:%S') 

        params = {
            OFFICE_PARAM: p_office_id,
            "name": p_tsId,
            "unit": p_unit,
            "datum": p_datum,
            "begin": p_start_date,
            "end": p_end_date,
            "timezone" : p_timezone,
            "page-size" : p_page_size
        }

        headerList={
            "Accept": HEADER_JSON_V2
        }
        responce = queryCDA(self,endPoint,params,headerList,return_type, dict_key = ['values'])

        return responce

    def write_ts(self, data, version_date = None, timezone = None, create_as_ltrs = False, store_rule = None, override_protection = None):
        """Will Create new TimeSeries if not already present.  Will store any data provided.

        Parameters
        -------
        data : pd.Dataframe, or Dictionay
            Time Series data to be stored.  If dataframe data must be provided in the following format
            df.tsId = timeseried id:specified name of the time series to be posted to
            df.office = the owning office of the time series
            df.units = units of values to be stored (ie. ft, in, m, cfs....)
            dataframe should have three columns date-time, value, quality-code. date-time values can be a string in ISO8601 formate or a datetime field. 
            if quality-code column is not present is will be set to 0.
                                         date-time value  quality-code
               0   2023-12-20T14:45:00.000-05:00  93.1           0
               1   2023-12-20T15:00:00.000-05:00  99.8           0
               2   2023-12-20T15:15:00.000-05:00  98.5           0
               3   2023-12-20T15:30:00.000-05:00  98.5           0

        version_date : str
            Specifies the version date for the timeseries to create. If this field is not specified, a null version date will be used.  The format for this field is ISO 8601 extended, with optional timezone, i.e., 'format', e.g., '2021-06-10T13:00:00-0700[PST8PDT]'.
        timezone : str
            Specifies the time zone of the version-date field (unless otherwise specified). If this field is not specified, the default time zone of UTC shall be used.  Ignored if version-date was specified with offset and timezone.
        create_as_lrts : bool
            Flag indicating if timeseries should be created as Local Regular Time Series. 'True' or 'False', default is 'False'
        store_rule : str
            The business rule to use when merging the incoming with existing data. Available values : REPLACE_ALL, DO_NOT_REPLACE, REPLACE_MISSING_VALUES_ONLY, REPLACE_WITH_NON_MISSING, DELETE_INSERT
        override_protection : bool
            A flag to ignore the protected data quality when storing data. 'True' or 'False'

        Returns
        -------
        Responce object from request package

        Examples
        -------
        
        """
        endPoint = 'timeseries'
        params = {
                'version-date': version_date,
                'timezone': timezone,
                'create-as-lrts' : create_as_ltrs, 
                'store-rule' : store_rule,
                'override-protection' : override_protection
            }
        headerList={
                'accept': '*/*',
                'Content-Type': HEADER_JSON_V2,
            }
        if isinstance(data, pd.DataFrame):
            #grab time series information
            tsId = data.tsId
            office = data.office
            units = data.units

            #check dataframe columns
            if 'quality-code' not in data:
                values['quality-code'] = 0
            if 'date-time' not in data:
                raise TypeError("date-time is a required column in data when posting as a dateframe")
            if 'value' not in data:
                raise TypeError("value is a required column when posting data when posting as a dataframe")
                
            #make sure that dataTime column is in iso8601 formate.      
            data['date-time'] = pd.to_datetime(data['date-time']).apply(pd.Timestamp.isoformat)
            data = data.reindex(columns=['date-time','value','quality-code'])
            if data.isnull().values.any():
                raise ValueError("Null/NaN data must be removed from the dataframe")
                
            ts_dict = {"name": tsId,
                       "office-id": office,
                       "units": units,
                       "values": data.values.tolist()
                       }  
            
        elif isinstance(data, dict): 
            ts_dict = data

        else: raise TypeError("data is not of type dataframe or dictionary")
            
        #print(ts_dict)
        response = self.get_session().post(endPoint, headers = headerList, data = json.dumps(ts_dict))
        return response

