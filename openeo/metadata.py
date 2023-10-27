from __future__ import annotations

import logging
import warnings
from typing import Any, Callable, List, NamedTuple, Optional, Tuple, Union

from openeo.internal.jupyter import render_component
from openeo.util import deep_get

_log = logging.getLogger(__name__)


class MetadataException(Exception):
    pass


class DimensionAlreadyExistsException(MetadataException):
    pass


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


class CollectionMetadata:
    """
    Wrapper for Image Collection metadata.

    Simplifies getting values from deeply nested mappings,
    allows additional parsing and normalizing compatibility issues.

    Metadata is expected to follow format defined by
    https://openeo.org/documentation/1.0/developers/api/reference.html#operation/describe-collection
    (with partial support for older versions)

    """

    # TODO: "CollectionMetadata" is also used as "cube metadata" where the link to original collection
    #       might be lost (if any). Better separation between rich EO raster collection metadata and
    #       essential cube metadata? E.g.: also thing of vector cubes.

    def __init__(self, metadata: dict, dimensions: List[Dimension] = None):
        # Original collection metadata (actual cube metadata might be altered through processes)
        self._orig_metadata = metadata

        if dimensions == None:
            self._dimensions = self._parse_dimensions(self._orig_metadata)
        else:
            self._dimensions = dimensions
        self._band_dimension = None
        self._temporal_dimension = None
        for dim in self._dimensions:
            # TODO: here we blindly pick last bands or temporal dimension if multiple. Let user choose?
            if dim.type == "bands":
                self._band_dimension = dim
            if dim.type == "temporal":
                self._temporal_dimension = dim

    def __eq__(self, o: Any) -> bool:
        return isinstance(o, CollectionMetadata) and self._dimensions == o._dimensions

    def _clone_and_update(
        self, metadata: dict = None, dimensions: List[Dimension] = None, **kwargs
    ) -> CollectionMetadata:
        """Create a new instance (of same class) with copied/updated fields."""
        cls = type(self)
        if dimensions == None:
            dimensions = self._dimensions
        return cls(metadata=metadata or self._orig_metadata, dimensions=dimensions, **kwargs)

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

    def get(self, *args, default=None):
        return deep_get(self._orig_metadata, *args, default=default)

    @property
    def extent(self) -> dict:
        # TODO: is this currently used and relevant?
        # TODO: check against extent metadata in dimensions
        return self._orig_metadata.get("extent")

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

    def filter_bands(self, band_names: List[Union[int, str]]) -> CollectionMetadata:
        """
        Create new `CollectionMetadata` with filtered band dimension
        :param band_names: list of band names/indices to keep
        :return:
        """
        assert self.band_dimension
        return self._clone_and_update(dimensions=[
            d.filter_bands(band_names) if isinstance(d, BandDimension) else d
            for d in self._dimensions
        ])

    def append_band(self, band: Band) -> CollectionMetadata:
        """
        Create new `CollectionMetadata` with given band added to band dimension.
        """
        assert self.band_dimension
        return self._clone_and_update(dimensions=[
            d.append_band(band) if isinstance(d, BandDimension) else d
            for d in self._dimensions
        ])

    def rename_labels(self, dimension: str, target: list, source: list = None) -> CollectionMetadata:
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

    def rename_dimension(self, source: str, target: str) -> CollectionMetadata:
        """
        Rename source dimension into target, preserving other properties
        """
        self.assert_valid_dimension(source)
        loc = self.dimension_names().index(source)
        new_dimensions = self._dimensions.copy()
        new_dimensions[loc] = new_dimensions[loc].rename(target)

        return self._clone_and_update(dimensions=new_dimensions)

    def reduce_dimension(self, dimension_name: str) -> CollectionMetadata:
        """Create new metadata object by collapsing/reducing a dimension."""
        # TODO: option to keep reduced dimension (with a single value)?
        # TODO: rename argument to `name` for more internal consistency
        # TODO: merge with drop_dimension (which does the same).
        self.assert_valid_dimension(dimension_name)
        loc = self.dimension_names().index(dimension_name)
        dimensions = self._dimensions[:loc] + self._dimensions[loc + 1:]
        return self._clone_and_update(dimensions=dimensions)

    def reduce_spatial(self) -> CollectionMetadata:
        """Create new metadata object by reducing the spatial dimensions."""
        dimensions = [d for d in self._dimensions if not isinstance(d, SpatialDimension)]
        return self._clone_and_update(dimensions=dimensions)

    def add_dimension(self, name: str, label: Union[str, float], type: str = None) -> CollectionMetadata:
        """Create new metadata object with added dimension"""
        if any(d.name == name for d in self._dimensions):
            raise DimensionAlreadyExistsException(f"Dimension with name {name!r} already exists")
        if type == "bands":
            dim = BandDimension(name=name, bands=[Band(name=label)])
        elif type == "spatial":
            dim = SpatialDimension(name=name, extent=[label, label])
        elif type == "temporal":
            dim = TemporalDimension(name=name, extent=[label, label])
        else:
            dim = Dimension(type=type or "other", name=name)
        return self._clone_and_update(dimensions=self._dimensions + [dim])

    def drop_dimension(self, name: str = None) -> CollectionMetadata:
        """Drop dimension with given name"""
        dimension_names = self.dimension_names()
        if name not in dimension_names:
            raise ValueError("No dimension named {n!r} (valid names: {ns!r})".format(n=name, ns=dimension_names))
        return self._clone_and_update(dimensions=[d for d in self._dimensions if not d.name == name])

    def _repr_html_(self):
        return render_component("collection", data=self._orig_metadata)

    def __str__(self) -> str:
        bands = self.band_names if self.has_band_dimension() else "no bands dimension"
        return f"CollectionMetadata({self.extent} - {bands} - {self.dimension_names()})"
