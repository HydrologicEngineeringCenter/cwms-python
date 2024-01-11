# CWMSpy
CWMS REST API for Data Retrieval

## Requirements.

Python 3.9+

## Installation & Usage
### pip install

If the python package is hosted on Github, you can install directly from Github

```sh
pip install git+https://github.com/Enovotny/CWMSpy.git
```
(you may need to run `pip` with root permission: `sudo pip install git+https://github.com/Enovotny/CWMSpy.git`)

Then import the package:
```python
import CWMSpy as CWMS
```

## Getting Started

Please follow the [installation procedure](#installation--usage) and then run the following:

```python
import CWMSpy as CWMS
from datetime import datetime, timedelta

apiRoot = 'CDA url to connect to'

cwms = CWMS()
cwms.connect(apiRoot)


end = datetime.now()
start = end - timedelta(days = 10)
df cwms.retrieve_ts(p_tsId='Some.Fully.Qualified.Ts.Id',p_start_date = start, p_end_date = end)
ts_df.head()
```
```
date-time	value	quality-code
0	2023-12-25 06:00:00	1432.82	0
1	2023-12-28 06:00:00	1432.86	0
2	2023-12-29 06:00:00	1432.92	0
3	2023-12-30 06:00:00	1432.92	0
4	2023-12-31 06:00:00	1432.91	0
```
