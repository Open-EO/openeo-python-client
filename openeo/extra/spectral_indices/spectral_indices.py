from openeo.processes import ProcessBuilder, array_modify
from openeo.rest.datacube import DataCube
import numpy as np

def _get_expression_map(cube: DataCube, x: ProcessBuilder):
    collection_id = cube.graph["loadcollection1"]["arguments"]["id"]
    bands = [band.replace("0", "").upper() for band in cube.metadata.band_names]

    def check_validity():
        if not all(band in orig_bands for band in bands):
            raise ValueError(
                "The bands in your cube {} are not all Sentinel-2 bands (the following are: {})".format(bands,orig_bands))

    def get_params():
        lookup = dict(zip(orig_bands, norm_bands))
        return {lookup[key]: x.array_element(i) for i, key in enumerate(bands)}

    def lookupS2():
        orig_bands = ["B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B8A", "B9", "B11", "B12"]
        norm_bands = ["A", "B", "G", "R", "RE1", "RE2", "RE3", "N", "RE4", "WV", "S1", "S2"]
        return orig_bands, norm_bands

    def lookupL8():
        orig_bands = ["B1", "B2", "B3", "B4", "B5", "B6", "B7", "B10", "B11"]
        norm_bands = ["A", "B", "G", "R", "N", "S1", "S2", "T1", "T2"]
        return orig_bands, norm_bands

    def lookupL457():
        orig_bands = ["B1", "B2", "B3", "B4", "B5", "B6", "B7"]
        norm_bands = ["B", "G", "R", "N", "S1", "T1", "S2"]
        return orig_bands, norm_bands

    def lookupModis():
        orig_bands = ["B3", "B4", "B1", "B2", "B5", "B6", "B7"]
        norm_bands = ["B", "G", "R", "N", np.nan, "S1", "S2"]
        return orig_bands, norm_bands

    def lookupProbav():
        orig_bands = ["BLUE", "RED", "NIR", "SWIR"]
        norm_bands = ["B", "R", "N", "S1"]
        return orig_bands, norm_bands

    lookupPlatform = {
        "LANDSAT4-5_TM_L1": lookupL457,
        "LANDSAT4-5_TM_L2": lookupL457,
        "LANDSAT8_L1": lookupL8,
        "LANDSAT8_L2": lookupL8,
        "MODIS": lookupModis,
        "PROBAV_L3_S5_TOC_100M": lookupProbav,
        "PROBAV_L3_S10_TOC_333M": lookupProbav,
        "SENTINEL2_L1C_SENTINELHUB": lookupS2,
        "SENTINEL2_L2A_SENTINELHUB": lookupS2,
        "TERRASCOPE_S2_TOC_V2": lookupS2,
    }

    if collection_id not in list(lookupPlatform.keys()):
        raise Exception("Sorry, satellite platform not supported for index computation!")

    orig_bands, norm_bands = lookupPlatform[collection_id]()
    return get_params()


def _callback(x: ProcessBuilder, index_list: list, datacube: DataCube, scaling_factor: int, index_specs) -> ProcessBuilder:
    index_values = []
    x_res = x

    params = _get_expression_map(datacube, x)
    for index_name in index_list:
        if index_name not in index_specs.keys():
            raise NotImplementedError("Index " + index_name + " has not been implemented.")
        index_result = eval(index_specs[index_name]["formula"], params)
        if scaling_factor is not None:
            if "range" not in index_specs[index_name].keys():
                raise ValueError(
                    "You want to scale the " + index_name + ", however the range of this index has not been supplied in the indices json yet.")
            index_result = index_result.linear_scale_range(*eval(index_specs[index_name]["range"]), 0, scaling_factor)
        index_values.append(index_result)
    if scaling_factor is not None:
        x_res = x_res.linear_scale_range(0, 8000, 0, scaling_factor)
    return array_modify(data=x_res, values=index_values, index=len(datacube.metadata.band_names))


def compute_indices(datacube: DataCube, index_list: list, scaling_factor: int = None) -> DataCube:
    """
    Computes a list of indices from a datacube

    param datacube: an instance of openeo.rest.DataCube
    param index_list: a list of indices. The indices that are implemented are derived from the eemont package created by davemlz:
    https://github.com/davemlz/eemont/blob/master/eemont/data/spectral-indices-dict.json
    param scaling_factor: the upper range to which you want to scale the value (the result after rescaling will be [0,scaling_factor])
    return: the datacube with the indices attached as bands

    """
    with open("resources/spectral-indices-dict.json") as f:
        index_specs = eval(f.read())["SpectralIndices"]
    return datacube.apply_dimension(dimension="bands",
                                    process=lambda x: _callback(x, index_list, datacube, scaling_factor,
                                                                index_specs)).rename_labels('bands',
                                                                                            target=datacube.metadata.band_names + index_list)