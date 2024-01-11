import requests
import pandas as pd
import json

def queryCDA(self, endpoint, payload, headerList, return_type, dict_key):
    """Send a query.

    Wrapper for requests.get that handles errors and returns response.

    Parameters
    ----------
    endpoint: string
        URL to query
    payload: dict
        query parameters passed to ``requests.get``
    headerList: dict
        headers
    return_type : str
        output type to return values as. 1. 'df' will return a pandas dataframe. 2. 'dict' will return a json decoded dictionay. 3. all other values will return Responce object from request package.
    dict_key : str
        key needed to grab correct values from json decoded dictionary.
        

    Returns
    -------
    string: query response
        The response from the API query ``requests.get`` function call.
    """


    response = self.s.get(endpoint, params=payload, headers=headerList)

    if response.status_code == 400:
        raise ValueError(
            f'Bad Request, check that your parameters are correct. URL: {response.url}'
        )
    elif response.status_code == 404:
        raise ValueError(
            'Page Not Found Error. May be the result of an empty query. '
            + f'URL: {response.url}'
        )

    return output_type(response, return_type, dict_key)

def output_type(response, return_type, dict_key):
    """Convert output to correct format requested by user 
    Parameters
    ----------
    response : Request object
        response from get request
    return_type : str
        output type to return values as. 1. 'df' will return a pandas dataframe. 2. 'dict' will return a json decoded dictionay. 3. all other values will return Responce object from request package.
    dict_key : str
        key needed to grab correct values from json decoded dictionary.

    Returns
    -------
    pandas df, json decoded dictionay, or Responce object from request package
    """
    if return_type in ['df','dict']:
        response = response.json()

    if return_type == 'df':
        temp = response
        for key in dict_key:
            temp = temp[key]
        temp_df = pd.DataFrame(temp)
        if dict_key[-1] == 'values':
            temp_df.columns = [sub['name'] for sub in response['value-columns']]

            if 'date-time' in temp_df.columns:
                temp_df['date-time'] = pd.to_datetime(temp_df['date-time'], unit='ms')
        response = temp_df

    return response