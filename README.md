
[![Status](https://img.shields.io/pypi/status/openeo)]()
[![Build Status](https://travis-ci.org/Open-EO/openeo-python-client.svg?branch=master)](https://travis-ci.org/Open-EO/openeo-python-client)
![PyPI](https://img.shields.io/pypi/v/openeo)

# openeo-python-client

Python client API for openEO. Allows you to interact with openEO backends from your own environment. 
[Read more on usage in the documentation.](https://open-eo.github.io/openeo-python-client/)

[Installation guide](https://openeo.org/documentation/1.0/python/#installation)


## Requirements

* Python 3.6 or higher

Windows users: It is recommended to install Anaconda Python as shapely may need to be installed separately using the Anaconda Navigator.

## Usage
[Python client documentation](https://open-eo.github.io/openeo-python-client/)

[Some example scripts](https://github.com/Open-EO/openeo-python-client/blob/master/examples)

[General OpenEO background](https://open-eo.github.io/openeo-api/)


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
