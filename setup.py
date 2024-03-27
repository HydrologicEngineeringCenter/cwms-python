# To use a consistent encoding
from codecs import open
from os import path

from setuptools import find_packages, setup  # type: ignore

# The directory containing this file
HERE = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(HERE, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="cwms-python",
    packages=find_packages(include=["cwms"]),
    url="https://github.com/HydrologicEngineeringCenter/cwms-python",
    keywords=["Swagger", "CWMS Data API"],
    version="0.1.0",
    description="A python implementation of the CWMS Data API (CDA)",
    author="Eric Novotny",
    install_requires=["pandas", "requests_toolbelt"],
    requires_python=[">=3.8.0"],
)
