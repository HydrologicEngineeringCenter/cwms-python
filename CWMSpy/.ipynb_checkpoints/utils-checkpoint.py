import requests
import pandas as pd
import json

def queryCDA(url, payload, headerList, output, dict_key):
    """Send a query.

    Wrapper for requests.get that handles errors and returns response.

    Parameters
    ----------
    url: string
        URL to query
    payload: dict
        query parameters passed to ``requests.get``

    Returns
    -------
    string: query response
        The response from the API query ``requests.get`` function call.
    """


    response = requests.get(url, params=payload, headers=headerList)

    if response.status_code == 400:
        raise ValueError(
            f'Bad Request, check that your parameters are correct. URL: {response.url}'
        )
    elif response.status_code == 404:
        raise ValueError(
            'Page Not Found Error. May be the result of an empty query. '
            + f'URL: {response.url}'
        )

    return output_type(response, output, dict_key)

def output_type(response, output, dict_key):

    if output in ['dataframe','dictionary']:
        response = response.json()

    if output == 'dataframe':
        temp = pd.DataFrame(response[dict_key])
        if dict_key == 'values':
            temp.columns = [sub['name'] for sub in response['value-columns']]

            if 'date-time' in temp.columns:
                temp['date-time'] = pd.to_datetime(temp['date-time'], unit='ms')
        response = temp

    return response