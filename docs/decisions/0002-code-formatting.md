# Code Formatting 

We want to automatically format source code to ensure consistency across the codebase.

## Summary

Developers should use a formatter to automatically format their code. To ensure that the code in the repository is always properly formatted, the project will include pre-commit hooks to validate the source files (and re-format if necessary) before the code is committed.

## Considered Options

Both [black][black] and [autopep8][autopep8] are popular python formatters. Black has the advantage of not having any configurable options, which eliminates issues which could arise due to developers having different configuration preferences. In addition to formatting the source code, import statements can be sorted and formatted using [isort][isort].

The [pre-commit][pre-commit] framework can be used to manage the git pre-commit hooks.

Another promising looking option is [ruff][ruff], which combines the functionality of multiple tools (black, isort, flake8, etc) in a single dependency.

[autopep8]: https://pypi.org/project/autopep8/
[black]: https://black.readthedocs.io/en/stable/
[isort]: https://pycqa.github.io/isort/index.html
[pre-commit]: https://pre-commit.com/
[ruff]: https://github.com/astral-sh/ruff

## Decision Outcome

Since the project was initially formatted using black, we will standardize its use it going forward. Additionally, we will use isort to format import statements. Pre-commit hooks will be added so developers can automatically validate/format their code before committing changes.

This decision should be revisited at some point in the future to assess the benefits of switching to ruff.

**NOTE:** Any existing code that needs to be re-formatted should be done so by the original author (if possible) to help preserve the commit history.

## Resources

1. https://pycqa.github.io/isort/docs/configuration/black_compatibility.html