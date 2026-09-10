# CWMSpy

CWMS REST API for Data Retrieval

## Requirements.

Python 3.9+

## Installation & Usage

### pip install

```sh
pip install cwms-python
```

Then import the package:

```python
import cwms
```

### Authentication

`cwms.init_session()` supports both CDA API keys and Keycloak access tokens.
Use `api_key=` for the headless CDA API key flow, or `token=` for an OIDC access
token such as one saved by [`cwms-cli login`]().

```python
import cwms

cwms.init_session(
    api_root="https://cwms-data.usace.army.mil/cwms-data/",
    token="ACCESS_TOKEN",
)
```

If both `token` and `api_key` are provided, `cwms-python` will use the token
and log a warning.

### Errors and debugging

Failed HTTP requests raise `cwms.api.ApiError`. Its message includes the HTTP
status, method, URL, and CDA response body (including incident details when
provided). The original response is available as `error.response`. Custom
user-management errors retain these details too. Network exceptions propagate
with their original type. Invalid JSON responses raise `ApiError` with the
decoding exception as the cause; empty response bodies return an empty dictionary.

Concurrent time-series reads and writes raise `cwms.api.BatchError` if any
series or chunk fails. It subclasses `RuntimeError`, and `error.failures`
contains `(series_or_chunk, original_exception)` pairs for every failure.
Reads do not return incomplete results as success. Successful writes are not
rolled back. Failures while looking up time-series extents also propagate.
Chunk retries are limited to connection errors, timeouts, and HTTP
429/500/502/503/504; validation and other permanent errors fail immediately
at the chunk layer. The shared HTTP adapter retains its existing retry policy.

Enable request outcome and chunk diagnostics with Python logging:

```python
import logging

logging.basicConfig(level=logging.WARNING)
logging.getLogger("cwms").setLevel(logging.DEBUG)
```

Request diagnostics include the method, endpoint, and response status, without
request bodies or authentication headers.

## Getting Started

```python
import cwms
from datetime import datetime, timedelta

end = datetime.now()
begin = end - timedelta(days = 10)
data = cwms.get_timeseries(ts_id='Some.Fully.Qualified.Ts.Id',office_id='OFFICE1' , begin = begin, end = end)

#a cwms data object will be provided this object containes both the JSON as well
#as the values converted into a dataframe

#display the dataframe

df = data.df
print(df)
```

```
     date-time 	value 	quality-code
0 	2024-04-23 08:15:00 	86.57 	3
1 	2024-04-23 08:30:00 	86.57 	3
2 	2024-04-23 08:45:00 	86.58 	3
3 	2024-04-23 09:00:00 	86.58 	3
4 	2024-04-23 09:15:00 	86.58 	3
5 	2024-04-23 09:30:00 	86.58 	3
6 	2024-04-23 09:45:00 	86.59 	3
7 	2024-04-23 10:00:00 	86.58 	3
```

```python
#display JSON
json = data.JSON
print(json)
```

```
{'name': 'Some.Fully.Qualified.Ts.Id',
 'office-id': 'MVP',
 'units': 'ft',
 'values': [['2024-04-23T08:15:00', 86.57, 3],
  ['2024-04-23T08:30:00', 86.57, 3],
  ['2024-04-23T08:45:00', 86.57999999999997, 3],
  ['2024-04-23T09:00:00', 86.57999999999997, 3],
  ['2024-04-23T09:15:00', 86.57999999999997, 3],
  ['2024-04-23T09:30:00', 86.57999999999997, 3],
  ['2024-04-23T09:45:00', 86.59, 3],
  ['2024-04-23T10:00:00', 86.57999999999997, 3]],
 'version-date': None}
```

## TimeSeries Profile API Compatibility Warning

Currently, the TimeSeries Profile API may not be fully supported
until a new version of cwms-data-access is released with the updated 
endpoint implementation.

## Contributing

Please view the contribution documentation here: [CONTRIBUTING.md]

## Contributing and releases

See [CONTRIBUTING.md](CONTRIBUTING.md) for development checks, PR title conventions,
and the Release Please publishing workflow.
