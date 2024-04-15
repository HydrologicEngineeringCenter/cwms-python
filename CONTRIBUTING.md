# Contributing to cwms-python

## Getting Started

In order to set up the development environment you will need to have [poetry][poetry] installed on your computer. You can then install all of the project dependencies by running the following command.

```sh
poetry install
```

This will create a virtual environment in `.venv/` in the project's root directory.

### Check Your Changes

Before submitting a pull request, you should verify that all the tests are passing and that the types are correct.

```sh
poetry run pytest -v tests/

poetry run mypy --strict cwms/
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

[black]: https://black.readthedocs.io/en/stable/
[isort]: https://pycqa.github.io/isort/index.html
[poetry]: https://python-poetry.org/docs/
