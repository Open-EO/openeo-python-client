from openeo.processes import ProcessBuilder, array_modify
from openeo.rest.datacube import DataCube
import numpy as np

def _get_expression_map(cube: DataCube, x: ProcessBuilder):
    collection_id = cube.metadata.get("id")
    bands = [band.replace("0", "").upper() for band in cube.metadata.band_names]

    def check_validity():
        if not all(band in band_mapping.keys() for band in bands):
            raise ValueError(
                "The bands in your cube {} are not all {} bands (the following are: {})".format(bands,collection_id,band_mapping.keys()))

    def get_params():
        return {band_mapping[key]: x.array_element(i) for i, key in enumerate(bands)}

    landsat457_mapping = {"B1":"B", "B2":"G", "B3":"R", "B4":"N", "B5":"S1", "B6":"T1", "B7":"S2"}
    landsat8_mapping = {"B1":"A", "B2":"B", "B3":"G", "B4":"R", "B5":"N", "B6":"S1", "B7":"S2", "B10":"T1", "B11":"T2"}
    modis_mapping = {"B3":"B", "B4":"G", "B1":"R", "B2":"N", "B5":np.nan, "B6":"S1", "B7":"S2"}
    probav_mapping = {"BLUE":"B", "RED":"R", "NIR":"N", "SWIR":"S1"}
    sentinel2_mapping = {"B1":"A", "B2":"B", "B3":"G", "B4":"R", "B5":"RE1", "B6":"RE2", "B7":"RE3", "B8":"N", "B8A":"RE4", "B9":"WV", "B11":"S1", "B12":"S2"}

    if "LANDSAT8" in collection_id:
        band_mapping = landsat8_mapping
    elif "LANDSAT" in collection_id:
        band_mapping = landsat457_mapping
    elif "MODIS" in collection_id:
        band_mapping = modis_mapping
    elif "PROBAV" in collection_id:
        band_mapping = probav_mapping
    elif "TERRASCOPE_S2" in collection_id or "SENTINEL2" in collection_id:
        band_mapping = sentinel2_mapping
    else:
        raise Exception("Sorry, satellite platform "+collection_id+" is not supported for index computation!")

    check_validity()
    return get_params()


def _callback(x: ProcessBuilder, index_list: list, datacube: DataCube, uplim_rescale: int, index_specs) -> ProcessBuilder:
    index_values = []
    x_res = x

    params = _get_expression_map(datacube, x)
    for index_name in index_list:
        if index_name not in index_specs.keys():
            raise NotImplementedError("Index " + index_name + " has not been implemented.")
        index_result = eval(index_specs[index_name]["formula"], params)
        if uplim_rescale is not None:
            if "range" not in index_specs[index_name].keys():
                raise ValueError(
                    "You want to scale the " + index_name + ", however the range of this index has not been supplied in the indices json yet.")
            index_result = index_result.linear_scale_range(*eval(index_specs[index_name]["range"]), 0, uplim_rescale)
        index_values.append(index_result)
    if uplim_rescale is not None:
        x_res = x_res.linear_scale_range(0, 8000, 0, uplim_rescale)
    return array_modify(data=x_res, values=index_values, index=len(datacube.metadata.band_names))


def compute_indices(datacube: DataCube, index_list: list, uplim_rescale: int = None) -> DataCube:
    """
    Computes a list of indices from a datacube

    param datacube: an instance of openeo.rest.DataCube
    param index_list: a list of indices. The indices that are implemented are derived from the eemont package created by davemlz:
    https://github.com/davemlz/eemont/blob/master/eemont/data/spectral-indices-dict.json
    param uplim_rescale: the upper range to which you want to scale the value (the result after rescaling will be [0,uplim_rescale])
    return: the datacube with the indices attached as bands

    """
    with open("resources/spectral-indices-dict.json") as f:
        index_specs = eval(f.read())["SpectralIndices"]
    return datacube.apply_dimension(dimension="bands",
                                    process=lambda x: _callback(x, index_list, datacube, uplim_rescale,
                                                                index_specs)).rename_labels('bands',
                                                                                            target=datacube.metadata.band_names + index_list)