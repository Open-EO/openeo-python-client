from __future__ import annotations

import functools
import logging
import warnings
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Set, Tuple, Union

import pystac
import pystac.extensions.datacube
import pystac.extensions.eo
import pystac.extensions.item_assets

from openeo.internal.jupyter import render_component
from openeo.util import Rfc3339, deep_get

_log = logging.getLogger(__name__)


class MetadataException(Exception):
    pass


class DimensionAlreadyExistsException(MetadataException):
    pass


# TODO: make these dimension classes immutable data classes
class Dimension:
    """Base class for dimensions."""

    def __init__(self, type: str, name: str):
        self.type = type
        self.name = name

    def __repr__(self):
        return "{c}({f})".format(
            c=self.__class__.__name__,
            f=", ".join("{k!s}={v!r}".format(k=k, v=v) for (k, v) in self.__dict__.items())
        )

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.__dict__ == other.__dict__

    def rename(self, name) -> Dimension:
        """Create new dimension with new name."""
        return Dimension(type=self.type, name=name)

    def rename_labels(self, target, source) -> Dimension:
        """
        Rename labels, if the type of dimension allows it.

        :param target: List of target labels
        :param source: Source labels, or empty list
        :return: A new dimension with modified labels, or the same if no change is applied.
        """
        # In general, we don't have/manage label info here, so do nothing.
        return Dimension(type=self.type, name=self.name)


class SpatialDimension(Dimension):
    DEFAULT_CRS = 4326

    def __init__(
        self,
        name: str,
        extent: Union[Tuple[float, float], List[float]],
        crs: Union[str, int, dict] = DEFAULT_CRS,
        step=None,
    ):
        """

        @param name:
        @param extent:
        @param crs:
        @param step: The space between the values. Use null for irregularly spaced steps.
        """
        super().__init__(type="spatial", name=name)
        self.extent = extent
        self.crs = crs
        self.step = step

    def rename(self, name) -> Dimension:
        return SpatialDimension(name=name, extent=self.extent, crs=self.crs, step=self.step)


class TemporalDimension(Dimension):
    def __init__(self, name: str, extent: Union[Tuple[str, str], List[str]]):
        super().__init__(type="temporal", name=name)
        self.extent = extent

    def rename(self, name) -> Dimension:
        return TemporalDimension(name=name, extent=self.extent)

    def rename_labels(self, target, source) -> Dimension:
        # TODO should we check if the extent has changed with the new labels?
        return TemporalDimension(name=self.name, extent=self.extent)


class Band(NamedTuple):
    """
    Simple container class for band metadata.
    Based on https://github.com/stac-extensions/eo#band-object
    """

    name: str
    common_name: Optional[str] = None
    # wavelength in micrometer
    wavelength_um: Optional[float] = None
    aliases: Optional[List[str]] = None
    # "openeo:gsd" field (https://github.com/Open-EO/openeo-stac-extensions#GSD-Object)
    gsd: Optional[dict] = None


class BandDimension(Dimension):
    # TODO #575 support unordered bands and avoid assumption that band order is known.
    def __init__(self, name: str, bands: List[Band]):
        super().__init__(type="bands", name=name)
        self.bands = bands

    @property
    def band_names(self) -> List[str]:
        return [b.name for b in self.bands]

    @property
    def band_aliases(self) -> List[List[str]]:
        return [b.aliases for b in self.bands]

    @property
    def common_names(self) -> List[str]:
        return [b.common_name for b in self.bands]

    def band_index(self, band: Union[int, str]) -> int:
        """
        Resolve a given band (common) name/index to band index

        :param band: band name, common name or index
        :return int: band index
        """
        band_names = self.band_names
        if isinstance(band, int) and 0 <= band < len(band_names):
            return band
        elif isinstance(band, str):
            common_names = self.common_names
            # First try common names if possible
            if band in common_names:
                return common_names.index(band)
            if band in band_names:
                return band_names.index(band)
            # Check band aliases to still support old band names
            aliases = [True if aliases and band in aliases else False for aliases in self.band_aliases]
            if any(aliases):
                return aliases.index(True)
        raise ValueError("Invalid band name/index {b!r}. Valid names: {n!r}".format(b=band, n=band_names))

    def band_name(self, band: Union[str, int], allow_common=True) -> str:
        """Resolve (common) name or index to a valid (common) name"""
        if isinstance(band, str):
            if band in self.band_names:
                return band
            elif band in self.common_names:
                if allow_common:
                    return band
                else:
                    return self.band_names[self.common_names.index(band)]
            elif any([True if aliases and band in aliases else False for aliases in self.band_aliases]):
                return self.band_names[self.band_index(band)]
        elif isinstance(band, int) and 0 <= band < len(self.bands):
            return self.band_names[band]
        raise ValueError("Invalid band name/index {b!r}. Valid names: {n!r}".format(b=band, n=self.band_names))

    def filter_bands(self, bands: List[Union[int, str]]) -> BandDimension:
        """
        Construct new BandDimension with subset of bands,
        based on given band indices or (common) names
        """
        return BandDimension(
            name=self.name,
            bands=[self.bands[self.band_index(b)] for b in bands]
        )

    def append_band(self, band: Band) -> BandDimension:
        """Create new BandDimension with appended band."""
        if band.name in self.band_names:
            raise ValueError("Duplicate band {b!r}".format(b=band))

        return BandDimension(
            name=self.name,
            bands=self.bands + [band]
        )

    def rename_labels(self, target, source) -> Dimension:
        if source:
            if len(target) != len(source):
                raise ValueError(
                    "In rename_labels, `target` and `source` should have same number of labels, "
                    "but got: `target` {t} and `source` {s}".format(t=target, s=source)
                )
            new_bands = self.bands.copy()
            for old_name, new_name in zip(source, target):
                band_index = self.band_index(old_name)
                the_band = new_bands[band_index]
                new_bands[band_index] = Band(
                    name=new_name,
                    common_name=the_band.common_name,
                    wavelength_um=the_band.wavelength_um,
                    aliases=the_band.aliases,
                    gsd=the_band.gsd,
                )
        else:
            new_bands = [Band(name=n) for n in target]
        return BandDimension(name=self.name, bands=new_bands)

    def rename(self, name) -> Dimension:
        return BandDimension(name=name, bands=self.bands)


class GeometryDimension(Dimension):
    # TODO: how to model/store labels of geometry dimension?
    def __init__(self, name: str):
        super().__init__(name=name, type="geometry")

    def rename(self, name) -> Dimension:
        return GeometryDimension(name=name)

    def rename_labels(self, target, source) -> Dimension:
        return GeometryDimension(name=self.name)


class CubeMetadata:
    """
    Interface for metadata of a data cube.

    Allows interaction with the cube dimensions and their labels (if available).
    """

    def __init__(self, dimensions: Optional[List[Dimension]] = None):
        # Original collection metadata (actual cube metadata might be altered through processes)
        self._dimensions = dimensions
        self._band_dimension = None
        self._temporal_dimension = None

        if dimensions is not None:
            for dim in self._dimensions:
                # TODO: here we blindly pick last bands or temporal dimension if multiple. Let user choose?
                # TODO: add spatial dimension handling?
                if dim.type == "bands":
                    if isinstance(dim, BandDimension):
                        self._band_dimension = dim
                    else:
                        raise MetadataException("Invalid band dimension {d!r}".format(d=dim))
                if dim.type == "temporal":
                    if isinstance(dim, TemporalDimension):
                        self._temporal_dimension = dim
                    else:
                        raise MetadataException("Invalid temporal dimension {d!r}".format(d=dim))

    def __eq__(self, o: Any) -> bool:
        return isinstance(o, type(self)) and self._dimensions == o._dimensions

    def _clone_and_update(self, dimensions: Optional[List[Dimension]] = None, **kwargs) -> CubeMetadata:
        """Create a new instance (of same class) with copied/updated fields."""
        cls = type(self)
        if dimensions is None:
            dimensions = self._dimensions
        return cls(dimensions=dimensions, **kwargs)

    def dimension_names(self) -> List[str]:
        return list(d.name for d in self._dimensions)

    def assert_valid_dimension(self, dimension: str) -> str:
        """Make sure given dimension name is valid."""
        names = self.dimension_names()
        if dimension not in names:
            raise ValueError(f"Invalid dimension {dimension!r}. Should be one of {names}")
        return dimension

    def has_band_dimension(self) -> bool:
        return isinstance(self._band_dimension, BandDimension)

    @property
    def band_dimension(self) -> BandDimension:
        """Dimension corresponding to spectral/logic/thematic "bands"."""
        if not self.has_band_dimension():
            raise MetadataException("No band dimension")
        return self._band_dimension

    def has_temporal_dimension(self) -> bool:
        return isinstance(self._temporal_dimension, TemporalDimension)

    @property
    def temporal_dimension(self) -> TemporalDimension:
        if not self.has_temporal_dimension():
            raise MetadataException("No temporal dimension")
        return self._temporal_dimension

    @property
    def spatial_dimensions(self) -> List[SpatialDimension]:
        return [d for d in self._dimensions if isinstance(d, SpatialDimension)]

    def has_geometry_dimension(self):
        return any(isinstance(d, GeometryDimension) for d in self._dimensions)

    @property
    def geometry_dimension(self) -> GeometryDimension:
        for d in self._dimensions:
            if isinstance(d, GeometryDimension):
                return d
        raise MetadataException("No geometry dimension")

    @property
    def bands(self) -> List[Band]:
        """Get band metadata as list of Band metadata tuples"""
        return self.band_dimension.bands

    @property
    def band_names(self) -> List[str]:
        """Get band names of band dimension"""
        return self.band_dimension.band_names

    @property
    def band_common_names(self) -> List[str]:
        return self.band_dimension.common_names

    def get_band_index(self, band: Union[int, str]) -> int:
        # TODO: eliminate this shortcut for smaller API surface
        return self.band_dimension.band_index(band)

    def filter_bands(self, band_names: List[Union[int, str]]) -> CubeMetadata:
        """
        Create new `CubeMetadata` with filtered band dimension
        :param band_names: list of band names/indices to keep
        :return:
        """
        assert self.band_dimension
        return self._clone_and_update(
            dimensions=[d.filter_bands(band_names) if isinstance(d, BandDimension) else d for d in self._dimensions]
        )

    def append_band(self, band: Band) -> CubeMetadata:
        """
        Create new `CubeMetadata` with given band added to band dimension.
        """
        assert self.band_dimension
        return self._clone_and_update(
            dimensions=[d.append_band(band) if isinstance(d, BandDimension) else d for d in self._dimensions]
        )

    def rename_labels(self, dimension: str, target: list, source: list = None) -> CubeMetadata:
        """
        Renames the labels of the specified dimension from source to target.

        :param dimension: Dimension name
        :param target: The new names for the labels.
        :param source: The names of the labels as they are currently in the data cube.

        :return: Updated metadata
        """
        self.assert_valid_dimension(dimension)
        loc = self.dimension_names().index(dimension)
        new_dimensions = self._dimensions.copy()
        new_dimensions[loc] = new_dimensions[loc].rename_labels(target, source)

        return self._clone_and_update(dimensions=new_dimensions)

    def rename_dimension(self, source: str, target: str) -> CubeMetadata:
        """
        Rename source dimension into target, preserving other properties
        """
        self.assert_valid_dimension(source)
        loc = self.dimension_names().index(source)
        new_dimensions = self._dimensions.copy()
        new_dimensions[loc] = new_dimensions[loc].rename(target)

        return self._clone_and_update(dimensions=new_dimensions)

    def reduce_dimension(self, dimension_name: str) -> CubeMetadata:
        """Create new CubeMetadata object by collapsing/reducing a dimension."""
        # TODO: option to keep reduced dimension (with a single value)?
        # TODO: rename argument to `name` for more internal consistency
        # TODO: merge with drop_dimension (which does the same).
        self.assert_valid_dimension(dimension_name)
        loc = self.dimension_names().index(dimension_name)
        dimensions = self._dimensions[:loc] + self._dimensions[loc + 1 :]
        return self._clone_and_update(dimensions=dimensions)

    def reduce_spatial(self) -> CubeMetadata:
        """Create new CubeMetadata object by reducing the spatial dimensions."""
        dimensions = [d for d in self._dimensions if not isinstance(d, SpatialDimension)]
        return self._clone_and_update(dimensions=dimensions)

    def add_dimension(self, name: str, label: Union[str, float], type: Optional[str] = None) -> CubeMetadata:
        """Create new CubeMetadata object with added dimension"""
        if any(d.name == name for d in self._dimensions):
            raise DimensionAlreadyExistsException(f"Dimension with name {name!r} already exists")
        if type == "bands":
            dim = BandDimension(name=name, bands=[Band(name=label)])
        elif type == "spatial":
            dim = SpatialDimension(name=name, extent=[label, label])
        elif type == "temporal":
            dim = TemporalDimension(name=name, extent=[label, label])
        elif type == "geometry":
            dim = GeometryDimension(name=name)
        else:
            dim = Dimension(type=type or "other", name=name)
        return self._clone_and_update(dimensions=self._dimensions + [dim])

    def drop_dimension(self, name: str = None) -> CubeMetadata:
        """Create new CubeMetadata object without dropped dimension with given name"""
        dimension_names = self.dimension_names()
        if name not in dimension_names:
            raise ValueError("No dimension named {n!r} (valid names: {ns!r})".format(n=name, ns=dimension_names))
        return self._clone_and_update(dimensions=[d for d in self._dimensions if not d.name == name])

    def __str__(self) -> str:
        bands = self.band_names if self.has_band_dimension() else "no bands dimension"
        return f"CubeMetadata({bands} - {self.dimension_names()})"


class CollectionMetadata(CubeMetadata):
    """
    Wrapper for EO Data Collection metadata.

    Simplifies getting values from deeply nested mappings,
    allows additional parsing and normalizing compatibility issues.

    Metadata is expected to follow format defined by
    https://openeo.org/documentation/1.0/developers/api/reference.html#operation/describe-collection
    (with partial support for older versions)

    """

    def __init__(self, metadata: dict, dimensions: List[Dimension] = None):
        self._orig_metadata = metadata
        if dimensions is None:
            dimensions = self._parse_dimensions(self._orig_metadata)

        super().__init__(dimensions=dimensions)

    @classmethod
    def _parse_dimensions(cls, spec: dict, complain: Callable[[str], None] = warnings.warn) -> List[Dimension]:
        """
        Extract data cube dimension metadata from STAC-like description of a collection.

        Dimension metadata comes from different places in spec:
        - 'cube:dimensions' has dimension names (e.g. 'x', 'y', 't'), dimension extent info
            and band names for band dimensions
        - 'eo:bands' has more detailed band information like "common" name and wavelength info

        This helper tries to normalize/combine these sources.

        :param spec: STAC like collection metadata dict
        :param complain: handler for warnings
        :return list: list of `Dimension` objects

        """

        # Dimension info is in `cube:dimensions` (or 0.4-style `properties/cube:dimensions`)
        cube_dimensions = (
            deep_get(spec, "cube:dimensions", default=None)
            or deep_get(spec, "properties", "cube:dimensions", default=None)
            or {}
        )
        if not cube_dimensions:
            complain("No cube:dimensions metadata")
        dimensions = []
        for name, info in cube_dimensions.items():
            dim_type = info.get("type")
            if dim_type == "spatial":
                dimensions.append(
                    SpatialDimension(
                        name=name,
                        extent=info.get("extent"),
                        crs=info.get("reference_system", SpatialDimension.DEFAULT_CRS),
                        step=info.get("step", None),
                    )
                )
            elif dim_type == "temporal":
                dimensions.append(TemporalDimension(name=name, extent=info.get("extent")))
            elif dim_type == "bands":
                bands = [Band(name=b) for b in info.get("values", [])]
                if not bands:
                    complain("No band names in dimension {d!r}".format(d=name))
                dimensions.append(BandDimension(name=name, bands=bands))
            else:
                complain("Unknown dimension type {t!r}".format(t=dim_type))
                dimensions.append(Dimension(name=name, type=dim_type))

        # Detailed band information: `summaries/[eo|raster]:bands` (and 0.4 style `properties/eo:bands`)
        eo_bands = (
            deep_get(spec, "summaries", "eo:bands", default=None)
            or deep_get(spec, "summaries", "raster:bands", default=None)
            or deep_get(spec, "properties", "eo:bands", default=None)
        )
        if eo_bands:
            # center_wavelength is in micrometer according to spec
            bands_detailed = [
                Band(
                    name=b["name"],
                    common_name=b.get("common_name"),
                    wavelength_um=b.get("center_wavelength"),
                    aliases=b.get("aliases"),
                    gsd=b.get("openeo:gsd"),
                )
                for b in eo_bands
            ]
            # Update band dimension with more detailed info
            band_dimensions = [d for d in dimensions if d.type == "bands"]
            if len(band_dimensions) == 1:
                dim = band_dimensions[0]
                # Update band values from 'cube:dimensions' with more detailed 'eo:bands' info
                eo_band_names = [b.name for b in bands_detailed]
                cube_dimension_band_names = [b.name for b in dim.bands]
                if eo_band_names == cube_dimension_band_names:
                    dim.bands = bands_detailed
                else:
                    complain("Band name mismatch: {a} != {b}".format(a=cube_dimension_band_names, b=eo_band_names))
            elif len(band_dimensions) == 0:
                if len(dimensions) == 0:
                    complain("Assuming name 'bands' for anonymous band dimension.")
                    dimensions.append(BandDimension(name="bands", bands=bands_detailed))
                else:
                    complain("No 'bands' dimension in 'cube:dimensions' while having 'eo:bands' or 'raster:bands'")
            else:
                complain("Multiple dimensions of type 'bands'")

        return dimensions

    def _clone_and_update(
        self, metadata: dict = None, dimensions: List[Dimension] = None, **kwargs
    ) -> CollectionMetadata:
        """
        Create a new instance (of same class) with copied/updated fields.

        This overrides the method in `CubeMetadata` to keep the original metadata.
        """
        cls = type(self)
        if metadata is None:
            metadata = self._orig_metadata
        if dimensions is None:
            dimensions = self._dimensions
        return cls(metadata=metadata, dimensions=dimensions, **kwargs)

    def get(self, *args, default=None):
        return deep_get(self._orig_metadata, *args, default=default)

    @property
    def extent(self) -> dict:
        # TODO: is this currently used and relevant?
        # TODO: check against extent metadata in dimensions
        return self._orig_metadata.get("extent")

    def _repr_html_(self):
        return render_component("collection", data=self._orig_metadata)

    def __str__(self) -> str:
        bands = self.band_names if self.has_band_dimension() else "no bands dimension"
        return f"CollectionMetadata({self.extent} - {bands} - {self.dimension_names()})"


def metadata_from_stac(url: str) -> CubeMetadata:
    """
    Reads the band metadata a static STAC catalog or a STAC API Collection and returns it as a :py:class:`CubeMetadata`

    :param url: The URL to a static STAC catalog (STAC Item, STAC Collection, or STAC Catalog) or a specific STAC API Collection
    :return: A :py:class:`CubeMetadata` containing the DataCube band metadata from the url.
    """

    # TODO move these nested functions and other logic to _StacMetadataParser

    def get_band_metadata(eo_bands_location: dict) -> List[Band]:
        # TODO: return None iso empty list when no metadata?
        return [
            Band(name=band["name"], common_name=band.get("common_name"), wavelength_um=band.get("center_wavelength"))
            for band in eo_bands_location.get("eo:bands", [])
        ]

    def get_band_names(bands: List[Band]) -> List[str]:
        return [band.name for band in bands]

    def is_band_asset(asset: pystac.Asset) -> bool:
        return "eo:bands" in asset.extra_fields

    stac_object = pystac.read_file(href=url)

    if isinstance(stac_object, pystac.Item):
        item = stac_object
        if "eo:bands" in item.properties:
            eo_bands_location = item.properties
        elif item.get_collection() is not None:
            # TODO: Also do asset based band detection (like below)?
            eo_bands_location = item.get_collection().summaries.lists
        else:
            eo_bands_location = {}
        bands = get_band_metadata(eo_bands_location)

    elif isinstance(stac_object, pystac.Collection):
        collection = stac_object
        bands = get_band_metadata(collection.summaries.lists)

        # Summaries is not a required field in a STAC collection, so also check the assets
        for itm in collection.get_items():
            band_assets = {asset_id: asset for asset_id, asset in itm.get_assets().items() if is_band_asset(asset)}

            for asset in band_assets.values():
                asset_bands = get_band_metadata(asset.extra_fields)
                for asset_band in asset_bands:
                    if asset_band.name not in get_band_names(bands):
                        bands.append(asset_band)
        if _PYSTAC_1_9_EXTENSION_INTERFACE and collection.ext.has("item_assets"):
            # TODO #575 support unordered band names and avoid conversion to a list.
            bands = list(_StacMetadataParser().get_bands_from_item_assets(collection.ext.item_assets))

    elif isinstance(stac_object, pystac.Catalog):
        catalog = stac_object
        bands = get_band_metadata(catalog.extra_fields.get("summaries", {}))
    else:
        raise ValueError(stac_object)

    # TODO: conditionally include band dimension when there was actual indication of band metadata?
    band_dimension = BandDimension(name="bands", bands=bands)
    dimensions = [band_dimension]

    # TODO: is it possible to derive the actual name of temporal dimension that the backend will use?
    temporal_dimension = _StacMetadataParser().get_temporal_dimension(stac_object)
    if temporal_dimension:
        dimensions.append(temporal_dimension)

    metadata = CubeMetadata(dimensions=dimensions)
    return metadata

# Sniff for PySTAC extension API since version 1.9.0 (which is not available below Python 3.9)
# TODO: remove this once support for Python 3.7 and 3.8 is dropped
_PYSTAC_1_9_EXTENSION_INTERFACE = hasattr(pystac.Item, "ext")


class _StacMetadataParser:
    """
    Helper to extract openEO metadata from STAC metadata resource
    """

    def __init__(self):
        # TODO: toggles for how to handle strictness, warnings, logging, etc
        pass

    def _get_band_from_eo_bands_item(self, eo_band: Union[dict, pystac.extensions.eo.Band]) -> Band:
        if isinstance(eo_band, pystac.extensions.eo.Band):
            return Band(
                name=eo_band.name,
                common_name=eo_band.common_name,
                wavelength_um=eo_band.center_wavelength,
            )
        elif isinstance(eo_band, dict) and "name" in eo_band:
            return Band(
                name=eo_band["name"],
                common_name=eo_band.get("common_name"),
                wavelength_um=eo_band.get("center_wavelength"),
            )
        else:
            raise ValueError(eo_band)

    def get_bands_from_eo_bands(self, eo_bands: List[Union[dict, pystac.extensions.eo.Band]]) -> List[Band]:
        """
        Extract bands from STAC `eo:bands` array

        :param eo_bands: List of band objects, as dict or `pystac.extensions.eo.Band` instances
        """
        # TODO: option to skip bands that failed to parse in some way?
        return [self._get_band_from_eo_bands_item(band) for band in eo_bands]

    def _get_bands_from_item_asset(
        self, item_asset: pystac.extensions.item_assets.AssetDefinition, *, _warn: Callable[[str], None] = _log.warning
    ) -> Union[List[Band], None]:
        """Get bands from a STAC 'item_assets' asset definition."""
        if _PYSTAC_1_9_EXTENSION_INTERFACE and item_asset.ext.has("eo"):
            if item_asset.ext.eo.bands is not None:
                return self.get_bands_from_eo_bands(item_asset.ext.eo.bands)
        elif "eo:bands" in item_asset.properties:
            # TODO: skip this in strict mode?
            if _PYSTAC_1_9_EXTENSION_INTERFACE:
                _warn("Extracting band info from 'eo:bands' metadata, but 'eo' STAC extension was not declared.")
            return self.get_bands_from_eo_bands(item_asset.properties["eo:bands"])

    def get_bands_from_item_assets(
        self, item_assets: Dict[str, pystac.extensions.item_assets.AssetDefinition]
    ) -> Set[Band]:
        """
        Get bands extracted from "item_assets" objects (defined by "item-assets" extension,
        in combination with "eo" extension) at STAC Collection top-level,

        Note that "item_assets" in STAC is a mapping, so the band order is undefined,
        which is why we return a set of bands here.

        :param item_assets: a STAC `item_assets` mapping
        """
        bands = set()
        # Trick to just warn once per collection
        _warn = functools.lru_cache()(_log.warning)
        for item_asset in item_assets.values():
            asset_bands = self._get_bands_from_item_asset(item_asset, _warn=_warn)
            if asset_bands:
                bands.update(asset_bands)
        return bands

    def get_temporal_dimension(self, stac_obj: pystac.STACObject) -> Union[TemporalDimension, None]:
        """
        Extract the temporal dimension from a STAC Collection/Item (if any)
        """
        # TODO: also extract temporal dimension from assets?
        if _PYSTAC_1_9_EXTENSION_INTERFACE:
            if stac_obj.ext.has("cube") and hasattr(stac_obj.ext, "cube"):
                temporal_dims = [
                    (n, d.extent or [None, None])
                    for (n, d) in stac_obj.ext.cube.dimensions.items()
                    if d.dim_type == pystac.extensions.datacube.DimensionType.TEMPORAL
                ]
                if len(temporal_dims) == 1:
                    name, extent = temporal_dims[0]
                    return TemporalDimension(name=name, extent=extent)
            elif isinstance(stac_obj, pystac.Collection) and stac_obj.extent.temporal:
                # No explicit "cube:dimensions": build fallback from "extent.temporal",
                # with dimension name "t" (openEO API recommendation).
                extent = [Rfc3339(propagate_none=True).normalize(d) for d in stac_obj.extent.temporal.intervals[0]]
                return TemporalDimension(name="t", extent=extent)
        else:
            if isinstance(stac_obj, pystac.Item):
                cube_dimensions = stac_obj.properties.get("cube:dimensions", {})
            elif isinstance(stac_obj, pystac.Collection):
                cube_dimensions = stac_obj.extra_fields.get("cube:dimensions", {})
            else:
                cube_dimensions = {}
            temporal_dims = [
                (n, d.get("extent", [None, None])) for (n, d) in cube_dimensions.items() if d.get("type") == "temporal"
            ]
            if len(temporal_dims) == 1:
                name, extent = temporal_dims[0]
                return TemporalDimension(name=name, extent=extent)
