# Contributing to cwms-python

For the Contributors/Maintainers of cwms-python

cwms-python uses:

- [Poetry](https://python-poetry.org/docs/) for package and dependency management.
- [pytest](https://pypi.org/project/pytest/) for testing of python functions as they are made.
- [requests-mock](https://pypi.org/project/requests-mock/) for simulating CDA to provide network-less tests, effectively mocking the requests to CDA.  
- [black](https://black.readthedocs.io/en/stable/) for code formatting.
- [isort](https://pycqa.github.io/isort/index.html) - for python imports sorting.


## Getting Started

In order to set up the development environment you will need to have [poetry][poetry] installed on your computer.

1. To install poetry (with python 3.8+) run:  
    `python -m pip install poetry` or `pip install poetry`

2. You can then install all of the project dependencies by running the following command.

    ```sh
    poetry install
    ```

    1. In VSCode you will be prompted to "activate venv", click accept on this to switch to this new poetry venv.  ![alt text](docs/images/poetry-venv.png)
    2. *NOTE: If you do not have your python `Scripts` directory in your path this will fail.  
    `Scripts` is located in your python install directory, add this to your PATH.*  

    This will create a virtual environment in `.venv/` in the project's root directory.

### Check Your Changes

**Before submitting** a pull request (PR), you should verify that all the tests are passing and that the types are correct.

```sh
poetry run pytest -v tests/

poetry run mypy --strict cwms/
```

Run poetry against a single file with:  
*\*From the root of the project\**

```sh
poetry run pytest tests/turbines/turbines_test.py
```

### Code Style

In order for a pull request to be accepted, python code must be formatted using [black][black] and [isort][isort]. YAML files should also be formatted using either Prettier or the provided pre-commit hook. Developer are encouraged to integrate these tools into their workflow. Pre-commit hooks can be installed to automatically validate any code changes, and reformat if necessary.

```sh
poetry run pre-commit install
```

It is also possible to run the pre-commit hooks without committing if you simply want to format the code.

```sh
# Format staged files without committing
poetry run pre-commit run

# Format all source files
poetry run pre-commit run --all-files
```


### Commiting to main and releases

when creating a pull request to main and you want the build to be pushed to test pypi the verion needs to be updated in pyproject.toml file. For pull requests that are only updating testpypi increase the third number `0.1.2` -> `0.1.3`. Releases that will also update to pypi tag a new release in the repo and update the pyproject.toml increasing the minor version `0.1.1` -> `0.2.0`.

to grab the module from testpypi for testin run

```sh
python3 -m pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ cwms-python
```