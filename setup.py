from setuptools import find_packages, setup

# To use a consistent encoding
from codecs import open
from os import path

# The directory containing this file
HERE = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(HERE, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='CWMSpy',
    packages=find_packages(include=['CWMSpy']),
    url="https://github.com/Enovotny/CWMSpy",
    keywords=["Swagger", "CWMS Data API"],
    version='0.1.0',
    description='A python implementation of the CWMS Data API (CDA)',
    author='Eric Novotny',
    install_requires=["pandas","requests_toolbelt"],
    requires_python=[">=3.9.0"],
)
