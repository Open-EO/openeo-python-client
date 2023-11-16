import functools
import json
import re
from typing import Dict, List, Optional, Set

from openeo import BaseOpenEoException
from openeo.processes import ProcessBuilder, array_create, array_modify
from openeo.rest.datacube import DataCube

try:
    import importlib_resources
except ImportError:
    import importlib.resources as importlib_resources


@functools.lru_cache(maxsize=1)
def load_indices() -> Dict[str, dict]:
    """Load set of supported spectral indices."""
    # TODO: encapsulate all this json loading in a single Awesome Spectral Indices registry class?
    specs = {}

    for path in [
        "resources/awesome-spectral-indices/spectral-indices-dict.json",
        # TODO #506 Deprecate extra-indices-dict.json as a whole
        #      and provide an alternative mechanism to work with custom indices
        "resources/extra-indices-dict.json",
    ]:
        with importlib_resources.files("openeo.extra.spectral_indices") / path as resource_path:
            data = json.loads(resource_path.read_text(encoding="utf8"))
            overwrites = set(specs.keys()).intersection(data["SpectralIndices"].keys())
            if overwrites:
                raise RuntimeError(f"Duplicate spectral indices: {overwrites} from {path}")
            specs.update(data["SpectralIndices"])

    return specs


@functools.lru_cache(maxsize=1)
def load_constants() -> Dict[str, float]:
    """Load constants defined by Awesome Spectral Indices."""
    # TODO: encapsulate all this json loading in a single Awesome Spectral Indices registry class?
    with importlib_resources.files(
        "openeo.extra.spectral_indices"
    ) / "resources/awesome-spectral-indices/constants.json" as resource_path:
        data = json.loads(resource_path.read_text(encoding="utf8"))

    return {k: v["default"] for k, v in data.items() if isinstance(v["default"], (int, float))}


@functools.lru_cache(maxsize=1)
def _load_bands() -> Dict[str, dict]:
    """Load band name mapping defined by Awesome Spectral Indices."""
    # TODO: encapsulate all this json loading in a single Awesome Spectral Indices registry class?
    with importlib_resources.files(
        "openeo.extra.spectral_indices"
    ) / "resources/awesome-spectral-indices/bands.json" as resource_path:
        data = json.loads(resource_path.read_text(encoding="utf8"))
    return data


class BandMappingException(BaseOpenEoException):
    """Failure to determine band-variable mapping."""


class _BandMapping:
    """
    Helper class to extract mappings between band names and variable names used in Awesome Spectral Indices formulas.
    """

    _EXTRA = {
        "sentinel1": {"HH": "HH", "HV": "HV", "VH": "VH", "VV": "VV"},
    }

    def __init__(self):
        # Load bands.json from Awesome Spectral Indices
        self._band_data = _load_bands()

    @staticmethod
    def _normalize_platform(platform: str) -> str:
        platform = platform.lower().replace("-", "").replace(" ", "")
        if platform in {"sentinel2a", "sentinel2b"}:
            platform = "sentinel2"
        return platform

    @staticmethod
    def _normalize_band_name(band_name: str) -> str:
        band_name = band_name.upper()
        # Normalize band names like "B01" to "B1"
        band_name = re.sub(r"^B0+(\d+)$", r"B\1", band_name)
        return band_name

    @functools.lru_cache(maxsize=1)
    def get_platforms(self) -> Set[str]:
        """Get list of supported (normalized) satellite platforms."""
        platforms = {p for var_data in self._band_data.values() for p in var_data.get("platforms", {}).keys()}
        platforms.update(self._EXTRA.keys())
        platforms.update({self._normalize_platform(p) for p in platforms})
        return platforms

    def guess_platform(self, name: str) -> str:
        """Guess platform from given collection id or name."""
        # First check original id, then retry with removed separators as last resort.
        for haystack in [name.lower(), re.sub("[_ -]", "", name.lower())]:
            for platform in sorted(self.get_platforms(), key=len, reverse=True):
                if platform in haystack:
                    return platform
        raise BandMappingException(f"Unable to guess satellite platform from id {name!r}.")

    def variable_to_band_name_map(self, platform: str) -> Dict[str, str]:
        """
        Build mapping from Awesome Spectral Indices variable names to (normalized) band names for given satellite platform.
        """
        platform_normalized = self._normalize_platform(platform)
        if platform_normalized in self._EXTRA:
            return self._EXTRA[platform_normalized]

        var_to_band = {
            var: pf_data["band"]
            for var, var_data in self._band_data.items()
            for pf, pf_data in var_data.get("platforms", {}).items()
            if self._normalize_platform(pf) == platform_normalized
        }
        if not var_to_band:
            raise BandMappingException(f"Empty band mapping derived for satellite platform {platform!r}")
        return var_to_band

    def actual_band_name_to_variable_map(self, platform: str, band_names: List[str]) -> Dict[str, str]:
        """Build mapping from actual band names (as given) to Awesome Spectral Indices variable names."""
        var_to_band = self.variable_to_band_name_map(platform=platform)
        band_to_var = {
            band_name: var
            for var, normalized_band_name in var_to_band.items()
            for band_name in band_names
            if self._normalize_band_name(band_name) == normalized_band_name
        }
        return band_to_var


def list_indices() -> List[str]:
    """List names of supported spectral indices"""
    specs = load_indices()
    return list(specs.keys())


def _check_params(item, params):
    range_vals = ["input_range", "output_range"]
    if set(params) != set(range_vals):
        raise ValueError(
            f"You have set the parameters {params} on {item}, while the following are required {range_vals}"
        )
    for rng in range_vals:
        if params[rng] is None:
            continue
        if len(params[rng]) != 2:
            raise ValueError(
                f"The list of provided values {params[rng]} for parameter {rng} for {item} is not of length 2"
            )
        # TODO: allow float too?
        if not all(isinstance(val, int) for val in params[rng]):
            raise ValueError("The ranges you supplied are not all of type int")
    if (params["input_range"] is None) != (params["output_range"] is None):
        raise ValueError(f"The index_range and output_range of {item} should either be both supplied, or both None")


def _check_validity_index_dict(index_dict: dict, index_specs: dict):
    # TODO: this `index_dict` API needs some more rethinking:
    #   - the dictionary has no explicit order of indices, which can be important for end user
    #   - allow "collection" to be missing (e.g. if no rescaling is desired, or input data is not kept)?
    #   - option to define default output range, instead of having it to specify it for each index?
    #   - keep "rescaling" feature separate/orthogonal from "spectral indices" feature. It could be useful as
    #       a more generic machine learning data preparation feature
    input_vals = ["collection", "indices"]
    if set(index_dict.keys()) != set(input_vals):
        raise ValueError(
            f"The first level of the dictionary should contain the keys 'collection' and 'indices', but they contain {index_dict.keys()}"
        )
    _check_params("collection", index_dict["collection"])
    for index, params in index_dict["indices"].items():
        if index not in index_specs.keys():
            raise NotImplementedError("Index " + index + " is not supported.")
        _check_params(index, params)


def _callback(
    x: ProcessBuilder,
    index_dict: dict,
    index_specs: dict,
    append: bool,
    band_names: List[str],
    band_to_var: Dict[str, str],
) -> ProcessBuilder:
    index_values = []
    x_res = x

    # TODO: use `label` parameter of `array_element` to avoid index based band references
    variables = {band_to_var[bn]: x.array_element(i) for i, bn in enumerate(band_names) if bn in band_to_var}
    eval_globals = {
        **load_constants(),
        **variables,
    }
    # TODO: user might want to control order of indices, which is tricky through a dictionary.
    for index, params in index_dict["indices"].items():
        index_result = eval(index_specs[index]["formula"], eval_globals)
        if params["input_range"] is not None:
            index_result = index_result.linear_scale_range(*params["input_range"], *params["output_range"])
        index_values.append(index_result)
    if index_dict["collection"]["input_range"] is not None:
        x_res = x_res.linear_scale_range(
            *index_dict["collection"]["input_range"], *index_dict["collection"]["output_range"]
        )
    if append:
        return array_modify(data=x_res, values=index_values, index=len(band_names))
    else:
        return array_create(data=index_values)


def compute_and_rescale_indices(
    datacube: DataCube,
    index_dict: dict,
    *,
    append: bool = False,
    variable_map: Optional[Dict[str, str]] = None,
    platform: Optional[str] = None,
) -> DataCube:
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

    :param append: append the indices as bands to the given data cube
        instead of creating a new cube with only the calculated indices
    :param variable_map: (optional) mapping from Awesome Spectral Indices formula variable to actual cube band names.
        To be specified if the given data cube has non-standard band names,
        or the satellite platform can not be recognized from the data cube metadata.
        See :ref:`spectral_indices_manual_band_mapping` for more information.
    :param platform: optionally specify the satellite platform (to determine band name mapping)
        if the given data cube has no or an unhandled collection id in its metadata.
        See :ref:`spectral_indices_manual_band_mapping` for more information.

    :return: the datacube with the indices attached as bands

    .. warning:: this "rescaled" index helper uses an experimental API (e.g. `index_dict` argument) that is subject to change.

    .. versionadded:: 0.26.0
        Added `variable_map` and `platform` arguments.

    """
    index_specs = load_indices()

    _check_validity_index_dict(index_dict, index_specs)

    if variable_map is None:
        # Automatic band mapping
        band_mapping = _BandMapping()
        if platform is None:
            if datacube.metadata and datacube.metadata.get("id"):
                platform = band_mapping.guess_platform(name=datacube.metadata.get("id"))
            else:
                raise BandMappingException("Unable to determine satellite platform from data cube metadata")
        band_to_var = band_mapping.actual_band_name_to_variable_map(
            platform=platform, band_names=datacube.metadata.band_names
        )
    else:
        band_to_var = {b: v for v, b in variable_map.items()}

    res = datacube.apply_dimension(
        dimension="bands",
        process=lambda x: _callback(
            x,
            index_dict=index_dict,
            index_specs=index_specs,
            append=append,
            band_names=datacube.metadata.band_names,
            band_to_var=band_to_var,
        ),
    )
    if append:
        return res.rename_labels("bands", target=datacube.metadata.band_names + list(index_dict["indices"].keys()))
    else:
        return res.rename_labels("bands", target=list(index_dict["indices"].keys()))


def append_and_rescale_indices(
    datacube: DataCube,
    index_dict: dict,
    *,
    variable_map: Optional[Dict[str, str]] = None,
    platform: Optional[str] = None,
) -> DataCube:
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

    :param variable_map: (optional) mapping from Awesome Spectral Indices formula variable to actual cube band names.
        To be specified if the given data cube has non-standard band names,
        or the satellite platform can not be recognized from the data cube metadata.
        See :ref:`spectral_indices_manual_band_mapping` for more information.
    :param platform: optionally specify the satellite platform (to determine band name mapping)
        if the given data cube has no or an unhandled collection id in its metadata.
        See :ref:`spectral_indices_manual_band_mapping` for more information.

    :return: data cube with appended indices

    .. warning:: this "rescaled" index helper uses an experimental API (e.g. `index_dict` argument) that is subject to change.

    .. versionadded:: 0.26.0
        Added `variable_map` and `platform` arguments.
    """
    return compute_and_rescale_indices(
        datacube=datacube, index_dict=index_dict, append=True, variable_map=variable_map, platform=platform
    )


def compute_indices(
    datacube: DataCube,
    indices: List[str],
    *,
    append: bool = False,
    variable_map: Optional[Dict[str, str]] = None,
    platform: Optional[str] = None,
) -> DataCube:
    """
    Compute multiple spectral indices from the given data cube.

    :param datacube: input data cube
    :param indices: list of names of the indices to compute and append. See `list_indices()` for supported indices.
    :param append: append the indices as bands to the given data cube
        instead of creating a new cube with only the calculated indices
    :param variable_map: (optional) mapping from Awesome Spectral Indices formula variable to actual cube band names.
        To be specified if the given data cube has non-standard band names,
        or the satellite platform can not be recognized from the data cube metadata.
        See :ref:`spectral_indices_manual_band_mapping` for more information.
    :param platform: optionally specify the satellite platform (to determine band name mapping)
        if the given data cube has no or an unhandled collection id in its metadata.
        See :ref:`spectral_indices_manual_band_mapping` for more information.

    :return: data cube containing the indices as bands

    .. versionadded:: 0.26.0
        Added `variable_map` and `platform` arguments.
    """
    # TODO: it's bit weird to have to specify all these None's in this structure
    index_dict = {
        "collection": {
            "input_range": None,
            "output_range": None,
        },
        "indices": {index: {"input_range": None, "output_range": None} for index in indices},
    }
    return compute_and_rescale_indices(
        datacube=datacube, index_dict=index_dict, append=append, variable_map=variable_map, platform=platform
    )


def append_indices(
    datacube: DataCube,
    indices: List[str],
    *,
    variable_map: Optional[Dict[str, str]] = None,
    platform: Optional[str] = None,
) -> DataCube:
    """
    Compute multiple spectral indices and append them to the given data cube.

    :param datacube: input data cube
    :param indices: list of names of the indices to compute and append. See `list_indices()` for supported indices.
    :param variable_map: (optional) mapping from Awesome Spectral Indices formula variable to actual cube band names.
        To be specified if the given data cube has non-standard band names,
        or the satellite platform can not be recognized from the data cube metadata.
        See :ref:`spectral_indices_manual_band_mapping` for more information.
    :param platform: optionally specify the satellite platform (to determine band name mapping)
        if the given data cube has no or an unhandled collection id in its metadata.
        See :ref:`spectral_indices_manual_band_mapping` for more information.

    :return: data cube with appended indices

    .. versionadded:: 0.26.0
        Added `variable_map` and `platform` arguments.
    """

    return compute_indices(
        datacube=datacube, indices=indices, append=True, variable_map=variable_map, platform=platform
    )


def compute_index(
    datacube: DataCube, index: str, *, variable_map: Optional[Dict[str, str]] = None, platform: Optional[str] = None
) -> DataCube:
    """
    Compute a single spectral index from a data cube.

    :param datacube: input data cube
    :param index: name of the index to compute. See `list_indices()` for supported indices.
    :param variable_map: (optional) mapping from Awesome Spectral Indices formula variable to actual cube band names.
        To be specified if the given data cube has non-standard band names,
        or the satellite platform can not be recognized from the data cube metadata.
        See :ref:`spectral_indices_manual_band_mapping` for more information.
    :param platform: optionally specify the satellite platform (to determine band name mapping)
        if the given data cube has no or an unhandled collection id in its metadata.
        See :ref:`spectral_indices_manual_band_mapping` for more information.

    :return: data cube containing the index as band

    .. versionadded:: 0.26.0
        Added `variable_map` and `platform` arguments.
    """
    # TODO: option to compute the index with `reduce_dimension` instead of `apply_dimension`?
    return compute_indices(
        datacube=datacube, indices=[index], append=False, variable_map=variable_map, platform=platform
    )


def append_index(
    datacube: DataCube, index: str, *, variable_map: Optional[Dict[str, str]] = None, platform: Optional[str] = None
) -> DataCube:
    """
    Compute a single spectral index and append it to the given data cube.

    :param cube: input data cube
    :param index: name of the index to compute and append. See `list_indices()` for supported indices.
    :param variable_map: (optional) mapping from Awesome Spectral Indices formula variable to actual cube band names.
        To be specified if the given data cube has non-standard band names,
        or the satellite platform can not be recognized from the data cube metadata.
        See :ref:`spectral_indices_manual_band_mapping` for more information.
    :param platform: optionally specify the satellite platform (to determine band name mapping)
        if the given data cube has no or an unhandled collection id in its metadata.
        See :ref:`spectral_indices_manual_band_mapping` for more information.

    :return: data cube with appended index

    .. versionadded:: 0.26.0
        Added `variable_map` and `platform` arguments.
    """
    return compute_indices(
        datacube=datacube, indices=[index], append=True, variable_map=variable_map, platform=platform
    )
