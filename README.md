# CWMSpy
CWMS REST API for Data Retrieval

## Development

In order to set up the development environment you will need to have [poetry][poetry] installed on your computed. To install all of the project's dependencies, run the following command:

```sh
poetry install
```

This will create a virtual environment in `.venv/` in the project's root directory.


### Running Tests and Type Checker

The following commands can be used to run the test suite and the static type checker.

```sh
# Run all tests
poetry run pytest -v tests/

# Check types
poetry run mypy --strict cwms/
```

Contributors are encouraged to run the tests and type checker before making commits, since both of these are included in the CI pipeline.


### Contributing

In order for code to be committed to the repo it must be formatted using [black][black] and [isort][isort]. Developers are encouraged to integrate these tools into their workflow. Run the following command to install both tools as pre-commit hooks. Once installed, all staged source files will be automatically formatted before being committed.

```sh
poetry run pre-commit install
```

It is also possible to run the pre-commit hooks without committing if you simply want to format the code.

```sh
# Will format all staged files
poetry run pre-commit run

# Will format all project files
poetry run pre-commit run --all-files
```


[black]: https://black.readthedocs.io/en/stable/
[isort]: https://pycqa.github.io/isort/index.html
[poetry]: https://python-poetry.org/docs/


## Requirements.

Python 3.8+

## Installation & Usage
### pip install

If the python package is hosted on Github, you can install directly from Github

```sh
pip install git+https://github.com/HydrologicEngineeringCenter/cwms-python.git
```
(you may need to run `pip` with root permission: `sudo pip install git+https://github.com/HydrologicEngineeringCenter/cwms-python.git`)

Then import the package:
```python
from CWMS import CWMS
```

## Getting Started

Please follow the [installation procedure](#installation--usage) and then run the following:

```python
from CWMS import CWMS
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
