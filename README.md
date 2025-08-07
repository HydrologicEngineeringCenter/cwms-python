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

## CLI Utilities

You can invoke various utilities from cwms-python. 

To see a list of utilities run:
    `cwms`

Then to run a utility's help you can run:
    `cwms store_file` or `cwms store_file -h`
    *This will show the arguments*

### Storing Files

You can store files from your local disk to the blob endpoint with this script:
`cwms store_file`

An example call for this might be:
`cwms store_file "C:/path/to/MY_TEXT FILE.shef" MY_TEXT_FILE.SHEF`
*NOTE* Blob Ids get stored all UPPERCASE even if the output name (second argument) has lowercase in it!

The `store_file` utility expects you to have environment variables set or via the CLI arguments.  

#### Environment Variable Options  

- `OFFICE`
- `CDA_API_ROOT`  
- `CDA_API_KEY`  
- `LOG_LEVEL` (Options: "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")  

#### Arguments

##### Required

- input_file  
- output_id  

##### Optional

- Help [-h]  
- Blob Description [--description DESCRIPTION]  
- File Media Type [--media-type MEDIA_TYPE] - [Mime-Type Options](https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/MIME_types/Common_types)
- 3/4 Letter Office ID [--office-id OFFICE_ID]  
- CDA Endpoint [--api-root CDA_API_ROOT] (Overrides Env Var)
- CDA API KEY [--api-key CDA_API_KEY] (Overrides Env Var)
- [--log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}]  

**Some args become required if their environment variable is not set!**

### Environment Variables

The common environment variables names agreed upon by the CWMS community are:  

- `OFFICE="SWT"`  
- `ENVIRONMENT="dev"` (or "test" or "prod")  
- `CDA_API_ROOT="https://cwms-data.usace.army.mil/cwms-data"` (or your T7 URL, or Dev, or Test)  
- `CDA_API_KEY="LongKeyFromAuthEndpointInCDA"`  
