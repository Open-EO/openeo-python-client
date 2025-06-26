from __future__ import annotations

import functools
import logging
import warnings
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Union,
)

import pystac
import pystac.extensions.datacube
import pystac.extensions.eo
import pystac.extensions.item_assets

from openeo.internal.jupyter import render_component
from openeo.util import Rfc3339, deep_get
from openeo.utils.normalize import normalize_resample_resolution, unique

_log = logging.getLogger(__name__)


class MetadataException(Exception):
    pass


class DimensionAlreadyExistsException(MetadataException):
    pass


# TODO: make these dimension classes immutable data classes
# TODO: align better with STAC datacube extension
# TODO: align/adapt/integrate with pystac's datacube extension implementation?
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
    # TODO: align better with STAC datacube extension: e.g. support "axis" (x or y)

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

    def contains_band(self, band: Union[int, str]) -> bool:
        """
        Check if the given band name or index is present in the dimension.
        """
        try:
            self.band_index(band)
            return True
        except ValueError:
            return False


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

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(dimensions={self._dimensions!r})"

    def __str__(self) -> str:
        bands = self.band_names if self.has_band_dimension() else "no bands dimension"
        return f"CubeMetadata({bands} - {self.dimension_names()})"

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
        return self._clone_and_update(
            dimensions=[
                d.rename_labels(target=target, source=source) if d.name == dimension else d for d in self._dimensions
            ]
        )

    def rename_dimension(self, source: str, target: str) -> CubeMetadata:
        """
        Rename source dimension into target, preserving other properties
        """
        self.assert_valid_dimension(source)
        return self._clone_and_update(
            dimensions=[d.rename(name=target) if d.name == source else d for d in self._dimensions]
        )

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

    def _ensure_band_dimension(
        self, *, name: Optional[str] = None, bands: List[Union[Band, str]], warning: str
    ) -> CubeMetadata:
        """
        Create new CubeMetadata object, ensuring a band dimension with given bands.
        This will override any existing band dimension, and is intended for
        special cases where pragmatism necessitates to ignore the original metadata.
        For example, to overrule badly/incomplete detected band names from STAC metadata.

        .. note::
            It is required to specify a warning message as this method is only intended
            to be used as temporary stop-gap solution for use cases that are possibly not future-proof.
            Enforcing a warning should make that clear and avoid that users unknowingly depend on
            metadata handling behavior that is not guaranteed to be stable.
        """
        _log.warning(warning or "ensure_band_dimension: overriding band dimension metadata with user-defined bands.")
        if name is None:
            # Preserve original band dimension name if possible
            name = self.band_dimension.name if self.has_band_dimension() else "bands"
        bands = [b if isinstance(b, Band) else Band(name=b) for b in bands]
        band_dimension = BandDimension(name=name, bands=bands)
        return self._clone_and_update(
            dimensions=[d for d in self._dimensions if not isinstance(d, BandDimension)] + [band_dimension]
        )

    def drop_dimension(self, name: str = None) -> CubeMetadata:
        """Create new CubeMetadata object without dropped dimension with given name"""
        dimension_names = self.dimension_names()
        if name not in dimension_names:
            raise ValueError("No dimension named {n!r} (valid names: {ns!r})".format(n=name, ns=dimension_names))
        return self._clone_and_update(dimensions=[d for d in self._dimensions if not d.name == name])

    def resample_spatial(
        self,
        resolution: Union[float, Tuple[float, float], List[float]] = 0.0,
        projection: Union[int, str, None] = None,
    ) -> CubeMetadata:
        resolution = normalize_resample_resolution(resolution)
        if self._dimensions is None:
            # Best-effort fallback to work with
            dimensions = [
                SpatialDimension(name="x", extent=[None, None]),
                SpatialDimension(name="y", extent=[None, None]),
            ]
        else:
            # Make sure to work with a copy (to edit in-place)
            dimensions = list(self._dimensions)

        # Find and replace spatial dimensions
        spatial_indices = [i for i, d in enumerate(dimensions) if isinstance(d, SpatialDimension)]
        if len(spatial_indices) != 2:
            raise MetadataException(f"Expected two spatial dimensions but found {spatial_indices=}")
        assert len(resolution) == 2
        for i, r in zip(spatial_indices, resolution):
            dim: SpatialDimension = dimensions[i]
            dimensions[i] = SpatialDimension(
                name=dim.name,
                extent=dim.extent,
                crs=projection or dim.crs,
                step=r if r != 0.0 else dim.step,
            )

        return self._clone_and_update(dimensions=dimensions)

    def resample_cube_spatial(self, target: CubeMetadata) -> CubeMetadata:
        # Replace spatial dimensions with ones from target, but keep other dimensions
        dimensions = [d for d in (self._dimensions or []) if not isinstance(d, SpatialDimension)]
        dimensions.extend(target.spatial_dimensions)
        return self._clone_and_update(dimensions=dimensions)


class CollectionMetadata(CubeMetadata):
    """
    Wrapper for EO Data Collection metadata.

    Simplifies getting values from deeply nested mappings,
    allows additional parsing and normalizing compatibility issues.

    Metadata is expected to follow format defined by
    https://openeo.org/documentation/1.0/developers/api/reference.html#operation/describe-collection
    (with partial support for older versions)

    """

    def __init__(self, metadata: dict, dimensions: List[Dimension] = None, _federation: Optional[dict] = None):
        self._orig_metadata = metadata
        if dimensions is None:
            dimensions = self._parse_dimensions(self._orig_metadata)
        super().__init__(dimensions=dimensions)

        self._federation = _federation

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
        return render_component("collection", data=self._orig_metadata, parameters={"federation": self._federation})

    def __str__(self) -> str:
        bands = self.band_names if self.has_band_dimension() else "no bands dimension"
        return f"CollectionMetadata({self.extent} - {bands} - {self.dimension_names()})"


def metadata_from_stac(url: str) -> CubeMetadata:
    """
    Reads the band metadata a static STAC catalog or a STAC API Collection and returns it as a :py:class:`CubeMetadata`

    :param url: The URL to a static STAC catalog (STAC Item, STAC Collection, or STAC Catalog) or a specific STAC API Collection
    :return: A :py:class:`CubeMetadata` containing the DataCube band metadata from the url.
    """
    stac_object = pystac.read_file(href=url)
    bands = _StacMetadataParser().bands_from_stac_object(stac_object)

    # At least assume there are spatial dimensions
    # TODO #743: are there conditions in which we even should not assume the presence of spatial dimensions?
    dimensions = [
        SpatialDimension(name="x", extent=[None, None]),
        SpatialDimension(name="y", extent=[None, None]),
    ]

    # TODO #743: conditionally include band dimension when there was actual indication of band metadata?
    band_dimension = BandDimension(name="bands", bands=bands)
    dimensions.append(band_dimension)

    # TODO: is it possible to derive the actual name of temporal dimension that the backend will use?
    temporal_dimension = _StacMetadataParser().get_temporal_dimension(stac_object)
    if temporal_dimension:
        dimensions.append(temporal_dimension)

    metadata = CubeMetadata(dimensions=dimensions)
    return metadata

# Sniff for PySTAC extension API since version 1.9.0 (which is not available below Python 3.9)
# TODO: remove this once support for Python 3.7 and 3.8 is dropped
_PYSTAC_1_9_EXTENSION_INTERFACE = hasattr(pystac.Item, "ext")

# Sniff for PySTAC support for Collection.item_assets (in STAC core since 1.1)
# (supported since PySTAC 1.12.0, which requires Python>=3.10)
_PYSTAC_1_12_ITEM_ASSETS = hasattr(pystac.Collection, "item_assets")


class _BandList(list):
    """
    Internal wrapper for list of ``Band`` objects.

    .. warning::
        This is an internal, experimental helper, with an API that is subject to change.
        Do not use/expose it directly in user (facing) code
    """

    def __init__(self, bands: Iterable[Band]):
        super().__init__(bands)

    def band_names(self) -> List[str]:
        return [band.name for band in self]

    @classmethod
    def merge(cls, band_lists: Iterable[_BandList]) -> _BandList:
        """Merge multiple lists of bands into a single list (unique by name)."""
        all_bands = (band for bands in band_lists for band in bands)
        return cls(unique(all_bands, key=lambda b: b.name))


_ON_EMPTY_WARN = "warn"
_ON_EMPTY_IGNORE = "ignore"


class _StacMetadataParser:
    """
    Helper to extract openEO metadata from STAC metadata resources (Collection, Item, Asset, etc.).

    .. warning::
        This is an internal, experimental helper, with an API that is subject to change.
        Do not use/expose it directly in user (facing) code
    """

    def __init__(self, *, logger=_log, log_level=logging.DEBUG, supress_duplicate_warnings: bool = True):
        # TODO: argument to set some kind of reference to a root document to improve logging messages?
        self._logger = logger
        self._log_level = log_level
        self._log = lambda msg, **kwargs: self._logger.log(msg=msg, level=self._log_level, **kwargs)
        self._warn = lambda msg, **kwargs: self._logger.warning(msg=msg, **kwargs)
        if supress_duplicate_warnings:
            # Use caching trick to avoid duplicate warnings
            self._warn = functools.lru_cache(maxsize=1000)(self._warn)

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

    def _band_from_eo_bands_metadata(self, band: Union[dict, pystac.extensions.eo.Band]) -> Band:
        """Construct band from metadata in eo v1.1 style"""
        if isinstance(band, pystac.extensions.eo.Band):
            return Band(
                name=band.name,
                common_name=band.common_name,
                wavelength_um=band.center_wavelength,
            )
        elif isinstance(band, dict) and "name" in band:
            return Band(
                name=band["name"],
                common_name=band.get("common_name"),
                wavelength_um=band.get("center_wavelength"),
            )
        else:
            raise ValueError(band)

    def _band_from_common_bands_metadata(self, data: dict) -> Band:
        """Construct band from metadata dict in STAC 1.1 + eo v2 style metadata"""
        # TODO: also support pystac wrapper when available (pystac v2?)
        return Band(
            name=data["name"],
            common_name=data.get("eo:common_name"),
            wavelength_um=data.get("eo:center_wavelength"),
        )

    def bands_from_stac_object(self, obj: Union[pystac.STACObject, pystac.Asset]) -> _BandList:
        """
        Extract band listing from a STAC object (Collection, Catalog, Item or Asset).
        """
        # Note: first check for Collection, as it is a subclass of Catalog
        if isinstance(obj, pystac.Collection):
            return self.bands_from_stac_collection(collection=obj)
        elif isinstance(obj, pystac.Catalog):
            return self.bands_from_stac_catalog(catalog=obj)
        elif isinstance(obj, pystac.Item):
            return self.bands_from_stac_item(item=obj)
        elif isinstance(obj, pystac.Asset):
            return self.bands_from_stac_asset(asset=obj)
        else:
            # TODO: also support dictionary with raw STAC metadata?
            raise ValueError(f"Unsupported STAC object: {obj!r}")

    def bands_from_stac_catalog(self, catalog: pystac.Catalog, *, on_empty: str = _ON_EMPTY_WARN) -> _BandList:
        """
        Extract band listing from a STAC Catalog.
        """
        # TODO: "eo:bands" vs "bands" priority based on STAC and EO extension version information
        summaries = catalog.extra_fields.get("summaries", {})
        if "eo:bands" in summaries:
            if _PYSTAC_1_9_EXTENSION_INTERFACE and not catalog.ext.has("eo"):
                self._warn_undeclared_metadata(field="eo:bands", ext="eo")
            return _BandList(self._band_from_eo_bands_metadata(b) for b in summaries["eo:bands"])
        elif "bands" in summaries:
            return _BandList(self._band_from_common_bands_metadata(b) for b in summaries["bands"])

        if on_empty == _ON_EMPTY_WARN:
            self._warn("bands_from_stac_catalog: no band name source found")
        return _BandList([])

    def bands_from_stac_collection(
        self,
        collection: pystac.Collection,
        *,
        consult_items: bool = True,
        consult_assets: bool = True,
        on_empty: str = _ON_EMPTY_WARN,
    ) -> _BandList:
        """
        Extract band listing from a STAC Collection.
        """
        # TODO: "eo:bands" vs "bands" priority based on STAC and EO extension version information
        self._log(f"bands_from_stac_collection with {collection.summaries.lists.keys()=}")
        # Look for band metadata in collection summaries
        if "eo:bands" in collection.summaries.lists:
            if _PYSTAC_1_9_EXTENSION_INTERFACE and not collection.ext.has("eo"):
                self._warn_undeclared_metadata(field="eo:bands", ext="eo")
            return _BandList(self._band_from_eo_bands_metadata(b) for b in collection.summaries.lists["eo:bands"])
        elif "bands" in collection.summaries.lists:
            return _BandList(self._band_from_common_bands_metadata(b) for b in collection.summaries.lists["bands"])
        elif "bands" in collection.extra_fields:
            # TODO: is this actually valid and necessary to support? https://github.com/radiantearth/stac-spec/issues/1346
            # TODO: avoid `extra_fields`, but built-in "bands" support seems to be scheduled for pystac V2
            return _BandList(self._band_from_common_bands_metadata(b) for b in collection.extra_fields["bands"])
        # Check item assets if available
        elif _PYSTAC_1_12_ITEM_ASSETS and collection.item_assets:
            return self._bands_from_item_assets(collection.item_assets)
        elif _PYSTAC_1_9_EXTENSION_INTERFACE and collection.ext.has("item_assets") and collection.ext.item_assets:
            return self._bands_from_item_assets(collection.ext.item_assets)
        elif collection.extra_fields.get("item_assets"):
            # Workaround for lack of support for STAC 1.1 core item_assets with pystac < 1.12
            item_assets = {
                k: pystac.extensions.item_assets.AssetDefinition(properties=v, owner=collection)
                for k, v in collection.extra_fields["item_assets"].items()
            }
            return self._bands_from_item_assets(item_assets)
        # If no band metadata so far: traverse items in collection
        elif consult_items:
            bands = _BandList.merge(
                self.bands_from_stac_item(
                    item=i, consult_collection=False, consult_assets=consult_assets, on_empty=_ON_EMPTY_IGNORE
                )
                for i in collection.get_items()
            )
            if bands:
                return bands

        if on_empty == _ON_EMPTY_WARN:
            self._warn("bands_from_stac_collection: no band name source found")
        return _BandList([])

    def bands_from_stac_item(
        self,
        item: pystac.Item,
        *,
        consult_collection: bool = True,
        consult_assets: bool = True,
        on_empty: str = _ON_EMPTY_WARN,
    ) -> _BandList:
        """
        Extract band listing from a STAC Item.
        """
        # TODO: "eo:bands" vs "bands" priority based on STAC and EO extension version information
        self._log(f"bands_from_stac_item with {item.properties.keys()=}")
        if "eo:bands" in item.properties:
            return _BandList(self._band_from_eo_bands_metadata(b) for b in item.properties["eo:bands"])
        elif "bands" in item.properties:
            return _BandList(self._band_from_common_bands_metadata(b) for b in item.properties["bands"])
        elif consult_collection and (parent_collection := item.get_collection()) is not None:
            return self.bands_from_stac_collection(collection=parent_collection, consult_items=False)
        elif consult_assets:
            # TODO: filter on asset roles?
            bands = _BandList.merge(
                self.bands_from_stac_asset(asset=a, on_empty=_ON_EMPTY_IGNORE) for a in item.get_assets().values()
            )
            if bands:
                return bands

        if on_empty == _ON_EMPTY_WARN:
            self._warn("bands_from_stac_item: no band name source found")
        return _BandList([])

    def _warn_undeclared_metadata(self, *, field: str, ext: str):
        """Helper to warn about using metadata from undeclared STAC extension"""
        self._warn(f"Using {field!r} metadata, but STAC extension {ext} was not declared.")

    def bands_from_stac_asset(self, asset: pystac.Asset, *, on_empty: str = _ON_EMPTY_WARN) -> _BandList:
        """
        Extract band listing from a STAC Asset.
        """
        # TODO: "eo:bands" vs "bands" priority based on STAC and EO extension version information
        # TODO: filter on asset roles?
        if _PYSTAC_1_9_EXTENSION_INTERFACE and asset.owner and asset.ext.has("eo") and asset.ext.eo.bands is not None:
            return _BandList(self._band_from_eo_bands_metadata(b) for b in asset.ext.eo.bands)
        elif "eo:bands" in asset.extra_fields:
            if _PYSTAC_1_9_EXTENSION_INTERFACE and asset.owner and not asset.ext.has("eo"):
                self._warn_undeclared_metadata(field="eo:bands", ext="eo")
            return _BandList(self._band_from_eo_bands_metadata(b) for b in asset.extra_fields["eo:bands"])
        elif "bands" in asset.extra_fields:
            # TODO: avoid `extra_fields`, but built-in "bands" support seems to be scheduled for pystac V2
            return _BandList(self._band_from_common_bands_metadata(b) for b in asset.extra_fields["bands"])

        if on_empty == _ON_EMPTY_WARN:
            self._warn("bands_from_stac_asset: no band name source found")
        return _BandList([])

    def _bands_from_item_asset_definition(
        self,
        asset: Union[
            pystac.extensions.item_assets.AssetDefinition,
            "pystac.ItemAssetDefinition",  # TODO: non-string type hint once pystac dependency is bumped to at least 1.12
        ],
    ) -> _BandList:
        """
        Extract band listing from a STAC Asset definition
        (as used in the item-assets extension, or STAC 1.1 item-assets).
        """
        if isinstance(asset, pystac.extensions.item_assets.AssetDefinition):
            if "eo:bands" in asset.properties:
                if _PYSTAC_1_9_EXTENSION_INTERFACE and asset.owner and not asset.ext.has("eo"):
                    self._warn_undeclared_metadata(field="eo:bands", ext="eo")
                return _BandList(self._band_from_eo_bands_metadata(b) for b in asset.properties["eo:bands"])
            elif "bands" in asset.properties:
                return _BandList(self._band_from_common_bands_metadata(b) for b in asset.properties["bands"])
        elif _PYSTAC_1_12_ITEM_ASSETS and isinstance(asset, pystac.ItemAssetDefinition):
            if "bands" in asset.properties:
                return _BandList(self._band_from_common_bands_metadata(b) for b in asset.properties["bands"])
            elif "eo:bands" in asset.properties:
                if _PYSTAC_1_9_EXTENSION_INTERFACE and asset.owner and not asset.ext.has("eo"):
                    self._warn_undeclared_metadata(field="eo:bands", ext="eo")
                return _BandList(self._band_from_eo_bands_metadata(b) for b in asset.properties["eo:bands"])
        else:
            self._warn(f"_bands_from_item_asset_definition: unsupported {type(asset)=}")
        return _BandList([])

    def _bands_from_item_assets(
        self,
        item_assets: Dict[
            str,
            Union[
                pystac.extensions.item_assets.AssetDefinition,
                "pystac.ItemAssetDefinition",  # TODO: non-string type hint once pystac dependency is bumped to at least 1.12
            ],
        ],
    ) -> _BandList:
        """
        Get bands extracted from assets defined under
        a collection's "item_assets" field

        Note that "item_assets" in STAC is a mapping, which means that the
        band order might be ill-defined.
        """
        self._warn("Deriving band listing from unordered `item_assets`")
        # TODO: filter on asset roles?
        return _BandList.merge(self._bands_from_item_asset_definition(a) for a in item_assets.values())
