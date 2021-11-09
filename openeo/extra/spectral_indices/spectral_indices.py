import json
from pathlib import Path
from typing import Dict

import numpy as np

from openeo.processes import ProcessBuilder, array_modify, array_create
from openeo.rest.datacube import DataCube

BAND_MAPPING_LANDSAT457 = {
    "B1": "B", "B2": "G", "B3": "R", "B4": "N", "B5": "S1", "B6": "T1", "B7": "S2"
}
BAND_MAPPING_LANDSAT8 = {
    "B1": "A", "B2": "B", "B3": "G", "B4": "R", "B5": "N", "B6": "S1", "B7": "S2", "B10": "T1", "B11": "T2"
}
BAND_MAPPING_MODIS = {
    "B3": "B", "B4": "G", "B1": "R", "B2": "N", "B5": np.nan, "B6": "S1", "B7": "S2"
}
BAND_MAPPING_PROBAV = {
    "BLUE": "B", "RED": "R", "NIR": "N", "SWIR": "S1"
}
BAND_MAPPING_SENTINEL2 = {
    "B1": "A", "B2": "B", "B3": "G", "B4": "R", "B5": "RE1", "B6": "RE2", "B7": "RE3", "B8": "N",
    "B8A": "RE4", "B9": "WV", "B11": "S1", "B12": "S2"
}


def _get_expression_map(cube: DataCube, x: ProcessBuilder) -> Dict[str, ProcessBuilder]:
    """Build mapping of formula variable names to `array_element` nodes."""
    # TODO: more robust way of figuring out the satellite platform?
    collection_id = cube.metadata.get("id").upper()
    # TODO: See if we can use common band names from collections instead of hardcoded mapping
    if "LANDSAT8" in collection_id:
        band_mapping = BAND_MAPPING_LANDSAT8
    elif "LANDSAT" in collection_id:
        band_mapping = BAND_MAPPING_LANDSAT457
    elif "MODIS" in collection_id:
        band_mapping = BAND_MAPPING_MODIS
    elif "PROBAV" in collection_id:
        band_mapping = BAND_MAPPING_PROBAV
    elif "TERRASCOPE_S2" in collection_id or "SENTINEL2" in collection_id:
        band_mapping = BAND_MAPPING_SENTINEL2
    else:
        raise ValueError("Could not detect supported satellite platform from {cid!r} for index computation!".format(
            cid=collection_id
        ))

    cube_bands = [band.replace("0", "").upper() for band in cube.metadata.band_names]
    # TODO: use `label` parameter from `array_element` to avoid index based band references.
    return {band_mapping[b]: x.array_element(i) for i, b in enumerate(cube_bands) if b in band_mapping}

def _check_params(item,params):
    range_vals = ["input_range","output_range"]
    if set(params) != set(range_vals):
        raise ValueError("You have set the following parameters {} on {}, while the following are required {}".format(params,item,range_vals))
    for rng in range_vals:
        if params[rng] == None:
            continue
        if len(params[rng]) != 2:
            raise ValueError("The list of values you have supplied {} for parameter {} for {} is not of length 2".format(params[rng], rng, item))
        if not all(isinstance(val, int) for val in params[rng]):
            raise ValueError("The ranges you supplied are not all of type int")
    if (params["input_range"] == None) != (params["output_range"] == None):
        raise ValueError("The index_range and output_range of {} should either be both supplied, or both None".format(item))

def _check_validity_index_dict(index_dict: dict, index_specs: dict):
    input_vals = ["collection", "indices"]
    if set(index_dict.keys()) != set(input_vals):
        raise ValueError("The first level of the dictionary should contain the keys 'collection' and 'indices', but they contain {}".format(index_dict.keys()))
    _check_params("collection",index_dict["collection"])
    for index,params in index_dict["indices"].items():
        if index not in index_specs.keys():
            raise NotImplementedError("Index " + index + " has not been implemented.")
        _check_params(index, params)


def _callback(x: ProcessBuilder, index_dict: list, datacube: DataCube, index_specs, append) -> ProcessBuilder:
    index_values = []
    x_res = x

    idx_data = _get_expression_map(datacube, x)
    for index, params in index_dict["indices"].items():
        index_result = eval(index_specs[index]["formula"], idx_data)
        if params["input_range"] is not None:
            index_result = index_result.linear_scale_range(*params["input_range"], *params["output_range"])
        index_values.append(index_result)
    if index_dict["collection"]["input_range"] is not None:
        x_res = x_res.linear_scale_range(*index_dict["collection"]["input_range"], *index_dict["collection"]["output_range"])
    if append:
        return array_modify(data=x_res, values=index_values, index=len(datacube.metadata.band_names))
    else:
        return array_create(data=index_values)


def compute_and_rescale_indices(datacube: DataCube, index_dict: dict, append=False) -> DataCube:
    """
    Computes a list of indices from a datacube

    param datacube: an instance of openeo.rest.DataCube
    param index_dict: a dictionary that contains the input- and output range of the collection on which you calculate the indices
     as well as the indices that you want to calculate with their responding input- and output ranges
    It follows the following format:
    {
        "collection": {
            "input_range": [0,8000],
            "output_range": [0,250]
        },
        "indices": {
            "NDVI": {
                "input_range": [-1,1],
                "output_range": [0,250]
            },
        }
    }
    The indices that are implemented are derived from the eemont package created by davemlz:
    https://github.com/davemlz/eemont/blob/master/eemont/data/spectral-indices-dict.json and have been further supplemented by
    Vito with the indices ANIR, NDGI, NDMI, NDRE1, NDRE2 and NDRE5.
    If you don't want to rescale your data, you can fill the input-, index- and output range with None.
    return: the datacube with the indices attached as bands
    """
    # TODO: use pkg_resources here instead of direct file reading
    with (Path(__file__).parent / "resources/spectral-indices-dict.json").open() as f:
        index_specs = json.load(f)["SpectralIndices"]
    with (Path(__file__).parent / "resources/vito-indices-dict.json").open() as f:
        index_specs.update(json.load(f)["SpectralIndices"])

    _check_validity_index_dict(index_dict, index_specs)
    res = datacube.apply_dimension(dimension="bands",process=lambda x: _callback(x, index_dict, datacube, index_specs, append))
    if append:
        return res.rename_labels('bands',target=datacube.metadata.band_names + list(index_dict["indices"].keys()))
    else:
        return res.rename_labels('bands',target=list(index_dict["indices"].keys()))

def append_and_rescale_indices(datacube: DataCube, index_dict: dict) -> DataCube:
    """
    Computes a list of indices from a datacube and appends them to the existing datacube

    param datacube: an instance of openeo.rest.DataCube
    param index_dict: a dictionary that contains the input- and output range of the collection on which you calculate the indices
     as well as the indices that you want to calculate with their responding input- and output ranges
    It follows the following format:
    {
        "collection": {
            "input_range": [0,8000],
            "output_range": [0,250]
        },
        "indices": {
            "NDVI": {
                "input_range": [-1,1],
                "output_range": [0,250]
            },
        }
    }
    The indices that are implemented are derived from the eemont package created by davemlz:
    https://github.com/davemlz/eemont/blob/master/eemont/data/spectral-indices-dict.json and have been further supplemented by
    Vito with the indices ANIR, NDGI, NDMI, NDRE1, NDRE2 and NDRE5.
    If you don't want to rescale your data, you can fill the input-, index- and output range with None.
    return: the datacube with the indices attached as bands
    """
    return compute_and_rescale_indices(datacube, index_dict, True)

def compute_indices(datacube: DataCube, indices: list, append=False):
    """
    Computes an indefinite number of indices specified by the user from a datacube

    param datacube: an instance of openeo.rest.DataCube
    param index: the index you want to calculate
    The indices that are implemented are derived from the eemont package created by davemlz:
    https://github.com/davemlz/eemont/blob/master/eemont/data/spectral-indices-dict.json and have been further supplemented by
    Vito with the indices ANIR, NDGI, NDMI, NDRE1, NDRE2 and NDRE5.
    return: a new datacube with the index as band
    """
    index_dict = {
        "collection": {
            "input_range": None,
            "output_range": None
        },
        "indices": {
            index: { "input_range": None, "output_range": None } for index in indices
        }
    }
    return compute_and_rescale_indices(datacube, index_dict, append)

def append_indices(datacube: DataCube, indices: list):
    """
    Calculate an indefinite number of indices specified by the user and appends them to a datacube

    param datacube: an instance of openeo.rest.DataCube
    param indices: the indices you want to calculate and append to the cube
    The indices that are implemented are derived from the eemont package created by davemlz:
    https://github.com/davemlz/eemont/blob/master/eemont/data/spectral-indices-dict.json and have been further supplemented by
    Vito with the indices ANIR, NDGI, NDMI, NDRE1, NDRE2 and NDRE5.
    return: the old datacube with the indices as bands attached at the last index
    """

    return compute_indices(datacube, indices, True)

def compute_index(datacube: DataCube, index: str, append=False):
    """
    Computes a single index from a datacube

    param datacube: an instance of openeo.rest.DataCube
    param index: the index you want to calculate
    The indices that are implemented are derived from the eemont package created by davemlz:
    https://github.com/davemlz/eemont/blob/master/eemont/data/spectral-indices-dict.json and have been further supplemented by
    Vito with the indices ANIR, NDGI, NDMI, NDRE1, NDRE2 and NDRE5.
    return: a new datacube with the index as band
    """
    return compute_indices(datacube, [index], append)

def append_index(datacube: DataCube, index: str):
    """
    Calculate a single index and appends it to a datacube

    param datacube: an instance of openeo.rest.DataCube
    param index: the index you want to calculate and append to the cube
    The indices that are implemented are derived from the eemont package created by davemlz:
    https://github.com/davemlz/eemont/blob/master/eemont/data/spectral-indices-dict.json and have been further supplemented by
    Vito with the indices ANIR, NDGI, NDMI, NDRE1, NDRE2 and NDRE5.
    return: the old datacube with the index as a band attached at the last index
    """
    return compute_index(datacube, index, True)