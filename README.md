# openeo-python-client

Python client API for OpenEO. Allows you to interact with OpenEO backends from your own environment.

[![Status](https://img.shields.io/pypi/status/openeo)]()
[![Build Status](https://travis-ci.org/Open-EO/openeo-python-client.svg?branch=master)](https://travis-ci.org/Open-EO/openeo-python-client)
![PyPI](https://img.shields.io/pypi/v/openeo)

## Requirements

* Python 3.5 or higher

Windows users: It is recommended to install Anaconda Python as shapely may need to be installed separately using the Anaconda Navigator.

## Usage
[Basic concepts and examples](https://github.com/Open-EO/openeo-python-client/blob/master/examples)

[General OpenEO background](https://open-eo.github.io/openeo-api/)

[API docs](https://open-eo.github.io/openeo-python-client/)

## Development

Install locally checked out version with additional development related dependencies:
```bash
pip install -e .[dev]
```
Building the documentation:

As HTML:
```bash
python setup.py build_sphinx -c docs
 ```
As Latex: 
```bash
python setup.py build_sphinx -c docs -b latex
```
