
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/openeo)
![Status](https://img.shields.io/pypi/status/openeo)
[![Lint](https://github.com/Open-EO/openeo-python-client/actions/workflows/lint.yml/badge.svg?branch=master)](https://github.com/Open-EO/openeo-python-client/actions/workflows/lint.yml)
[![Tests](https://github.com/Open-EO/openeo-python-client/actions/workflows/unittests.yml/badge.svg?branch=master)](https://github.com/Open-EO/openeo-python-client/actions/workflows/unittests.yml)
[![PyPI](https://img.shields.io/pypi/v/openeo)](https://pypi.org/project/openeo/)
[![Conda (channel only)](https://img.shields.io/conda/vn/conda-forge/openeo)](https://anaconda.org/conda-forge/openeo)


# openEO Python Client

Python Client Library for the [openEO API](https://github.com/Open-EO/openeo-api).
Allows you to interact with openEO backends from your own (local) Python environment.

[openEO Python Client Library docs](https://open-eo.github.io/openeo-python-client/)


## Usage example

A simple example, to give a feel of using this library:

```python
import openeo

# Connect to openEO back-end.
connection = openeo.connect("openeo.vito.be").authenticate_oidc()

# Load data cube from TERRASCOPE_S2_NDVI_V2 collection.
cube = connection.load_collection(
    "TERRASCOPE_S2_NDVI_V2",
    spatial_extent={"west": 5.05, "south": 51.21, "east": 5.1, "north": 51.23},
    temporal_extent=["2022-05-01", "2022-05-30"],
    bands=["NDVI_10M"],
)
# Rescale digital number to physical values and take temporal maximum.
cube = cube.apply(lambda x: 0.004 * x - 0.08).max_time()

cube.download("ndvi-max.tiff")
```

![Example result](https://raw.githubusercontent.com/Open-EO/openeo-python-client/master/docs/_static/images/welcome.png)


See the [openEO Python Client Library documentation](https://open-eo.github.io/openeo-python-client/) for more details,
examples and in-depth discussion.


## Installation

As always, it is recommended to work in some kind of virtual environment
(using `venv`, `virtualenv`, conda, docker, ...)
to install the `openeo` package and its dependencies:

    pip install openeo

See the [installation docs](https://open-eo.github.io/openeo-python-client/installation.html)
for more information, extras and alternatives.



## General openEO background and links

- [openEO.org](https://openeo.org/)
- [api.openEO.org](https://api.openeo.org/)
- [processes.openEO.org](https://processes.openeo.org/)


## Contributions and funding

The authors acknowledge the financial support for the development of this package
during the H2020 project "openEO" (Oct 2017 to Sept 2020) by the European Union, funded by call EO-2-2017: EO Big Data Shift, under grant number 776242.
We also acknowledge the financial support received from ESA for the project "openEO Platform" (Sept 2020 to Sept 2023).

This package received major contributions from the following organizations:

[<img src="https://raw.githubusercontent.com/Open-EO/openeo-python-client/master/docs/_static/images/vito-logo.png" alt="VITO Remote Sensing logo" title="VITO Remote Sensing" height="50">](https://remotesensing.vito.be/) &emsp;
[<img src="https://www.uni-muenster.de/imperia/md/images/allgemein/farbunabhaengig/wwu.svg" alt="WWU Münster logo" title="University of Münster" height="50">](https://www.uni-muenster.de/) &emsp;
[<img src="https://upload.wikimedia.org/wikipedia/commons/9/9b/Eurac_Research_-_logo.png" alt="Eurac Research logo" title="Eurac Research" height="50">](https://www.eurac.edu/) &emsp;
[<img src="https://upload.wikimedia.org/wikipedia/commons/e/e5/TU_Signet_CMYK.svg" alt="TU Wien Logo" title="Technische Universität Wien" height="50">](https://www.tuwien.at/) &emsp;
