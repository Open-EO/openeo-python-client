import json
import pkg_resources
from typing import Dict, List

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


def load_indices() -> Dict[str, dict]:
    """Load set of supported spectral indices."""
    specs = {}

    for path in [
        "resources/awesome-spectral-indices/spectral-indices-dict.json",
        "resources/extra-indices-dict.json"
    ]:
        with pkg_resources.resource_stream("openeo.extra.spectral_indices", path) as stream:
            specs.update(json.load(stream)["SpectralIndices"])

    return specs


def list_indices() -> List[str]:
    """List names of supported spectral indices"""
    specs = load_indices()
    return list(specs.keys())


def _check_params(item,params):
    range_vals = ["input_range","output_range"]
    if set(params) != set(range_vals):
        raise ValueError("You have set the following parameters {} on {}, while the following are required {}".format(params,item,range_vals))
    for rng in range_vals:
        if params[rng] == None:
            continue
        if len(params[rng]) != 2:
            raise ValueError("The list of values you have supplied {} for parameter {} for {} is not of length 2".format(params[rng], rng, item))
        # TODO: allow float too?
        if not all(isinstance(val, int) for val in params[rng]):
            raise ValueError("The ranges you supplied are not all of type int")
    if (params["input_range"] == None) != (params["output_range"] == None):
        raise ValueError("The index_range and output_range of {} should either be both supplied, or both None".format(item))


def _check_validity_index_dict(index_dict: dict, index_specs: dict):
    # TODO: this `index_dict` API needs some more rethinking:
    #   - the dictionary has no explicit order of indices, which can be important for end user
    #   - allow "collection" to be missing (e.g. if no rescaling is desired, or input data is not kept)?
    #   - option to define default output range, instead of having it to specify it for each index?
    #   - keep "rescaling" feature separate/orthogonal from "spectral indices" feature. It could be useful as
    #       a more generic machine learning data preparation feature
    input_vals = ["collection", "indices"]
    if set(index_dict.keys()) != set(input_vals):
        raise ValueError("The first level of the dictionary should contain the keys 'collection' and 'indices', but they contain {}".format(index_dict.keys()))
    _check_params("collection",index_dict["collection"])
    for index,params in index_dict["indices"].items():
        if index not in index_specs.keys():
            raise NotImplementedError("Index " + index + " is not supported.")
        _check_params(index, params)


def _callback(x: ProcessBuilder, index_dict: dict, datacube: DataCube, index_specs, append) -> ProcessBuilder:
    index_values = []
    x_res = x

    idx_data = _get_expression_map(datacube, x)
    # TODO: user might want to control order of indices, which is tricky through a dictionary.
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
    Computes a list of indices from a data cube

    :param datacube: input data cube
    :param index_dict: a dictionary that contains the input- and output range of the collection on which you calculate the indices
        as well as the indices that you want to calculate with their responding input- and output ranges
        It follows the following format::

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

        If you don't want to rescale your data, you can fill the input-, index- and output-range with ``None``.

        See `list_indices()` for supported indices.

    :return: the datacube with the indices attached as bands

    .. warning:: this "rescaled" index helper uses an experimental API (e.g. `index_dict` argument) that is subject to change.
    """
    index_specs = load_indices()

    _check_validity_index_dict(index_dict, index_specs)
    res = datacube.apply_dimension(dimension="bands",process=lambda x: _callback(x, index_dict, datacube, index_specs, append))
    if append:
        return res.rename_labels('bands',target=datacube.metadata.band_names + list(index_dict["indices"].keys()))
    else:
        return res.rename_labels('bands',target=list(index_dict["indices"].keys()))


def append_and_rescale_indices(datacube: DataCube, index_dict: dict) -> DataCube:
    """
    Computes a list of indices from a datacube and appends them to the existing datacube

    :param datacube: input data cube
    :param index_dict: a dictionary that contains the input- and output range of the collection on which you calculate the indices
        as well as the indices that you want to calculate with their responding input- and output ranges
        It follows the following format::

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

        See `list_indices()` for supported indices.

    :return: data cube with appended indices

    .. warning:: this "rescaled" index helper uses an experimental API (e.g. `index_dict` argument) that is subject to change.
    """
    return compute_and_rescale_indices(datacube=datacube, index_dict=index_dict, append=True)


def compute_indices(datacube: DataCube, indices: List[str], append: bool = False) -> DataCube:
    """
    Compute multiple spectral indices from the given data cube.

    :param datacube: input data cube
    :param indices: list of names of the indices to compute and append. See `list_indices()` for supported indices.
    :return: data cube containing the indices as bands
    """
    # TODO: it's bit weird to have to specify all these None's in this structure
    index_dict = {
        "collection": {
            "input_range": None,
            "output_range": None
        },
        "indices": {
            index: {"input_range": None, "output_range": None} for index in indices
        }
    }
    return compute_and_rescale_indices(datacube=datacube, index_dict=index_dict, append=append)


def append_indices(datacube: DataCube, indices: List[str]) -> DataCube:
    """
    Compute multiple spectral indices and append them to the given data cube.

    :param datacube: input data cube
    :param indices: list of names of the indices to compute and append. See `list_indices()` for supported indices.
    :return: data cube with appended indices
    """

    return compute_indices(datacube=datacube, indices=indices, append=True)


def compute_index(datacube: DataCube, index: str) -> DataCube:
    """
    Compute a single spectral index from a data cube.

    :param datacube: input data cube
    :param index: name of the index to compute. See `list_indices()` for supported indices.
    :return: data cube containing the index as band
    """
    # TODO: option to compute the index with `reduce_dimension` instead of `apply_dimension`?
    return compute_indices(datacube=datacube, indices=[index], append=False)


def append_index(datacube: DataCube, index: str) -> DataCube:
    """
    Compute a single spectral index and append it to the given data cube.

    :param cube: input data cube
    :param index: name of the index to compute and append. See `list_indices()` for supported indices.
    :return: data cube with appended index
    """
    return compute_indices(datacube=datacube, indices=[index], append=True)
