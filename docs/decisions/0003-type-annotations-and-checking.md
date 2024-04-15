# Type Annotations and Type Checking

We want to utilize type annotations for the `cwms-python` source code. 

## Summary

Type hints should be provided for all function arguments and return types. Developers should use a static type checker to check for type errors before committing code. Additionally, type checking should be added to the CI pipeline.

## Considered Options

There are several static type checkers available for python, but [mypy][mypy] is one of the most popular, and most well-documented. It is actively maintained and can be integrated into most text editors.

[mypy]: https://mypy-lang.org/

### Strict Mode

From the mypy website:

> If you run mypy with the --strict flag, you will basically never get a type related error at runtime without a corresponding mypy error, unless you explicitly circumvent mypy somehow.

Given the benefits of strict type checking, and the fact that our codebase is relatively new and has included type annotations from the start, it makes sense to include the `--strict` flag when validating the source files.

# Decision Outcome

We will use mypy to enforce type checking of the `cwms-python` codebase. A type checking step will be added to the CI pipeline and run on all push and pull request events. A pre-commit hook will be added so developers can validate their changes before committing.

Strict type checking will be initially enabled by default. If we encounter significant issues we have the option of adding a configuration file to disable individual checks.

## Resources

1. https://mypy.readthedocs.io/en/latest/getting_started.html#strict-mode-and-configuration
2. https://github.com/microsoft/vscode-mypy