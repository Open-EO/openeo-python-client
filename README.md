# openeo-python-client

Python client API for OpenEO. Allows you to interact with OpenEO backends from your own environment.

[![Status](https://img.shields.io/badge/Status-proof--of--concept-yellow.svg)]()

## Requirements

* Python 3.5
* Pip3

## Usage
[Basic concepts and examples](https://github.com/Open-EO/openeo-python-client/blob/master/examples)

[General OpenEO background](https://open-eo.github.io/openeo-api/)

[API docs](https://open-eo.github.io/openeo-python-client/)

## Development

Installing locally checked out version:
```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
pip install --user -e .
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
