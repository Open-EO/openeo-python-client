from typing import List, Optional, Union


class StacDummyBuilder:
    """
    Helper to compactly produce STAC Item/Collection/Catalog/... dicts for test purposes

    .. warning::
        This is an experimental API subject to change.
    """

    _EXT_DATACUBE = "https://stac-extensions.github.io/datacube/v2.2.0/schema.json"

    @classmethod
    def item(
        cls,
        *,
        id: str = "item123",
        stac_version="1.0.0",
        datetime: str = "2024-03-08",
        properties: Optional[dict] = None,
        cube_dimensions: Optional[dict] = None,
        stac_extensions: Optional[List[str]] = None,
        **kwargs,
    ) -> dict:
        """Create a STAC Item represented as dictionary."""
        properties = properties or {}
        properties.setdefault("datetime", datetime)

        if cube_dimensions is not None:
            properties["cube:dimensions"] = cube_dimensions
            stac_extensions = cls._add_stac_extension(stac_extensions, cls._EXT_DATACUBE)

        d = {
            "type": "Feature",
            "stac_version": stac_version,
            "id": id,
            "geometry": None,
            "properties": properties,
            "links": [],
            "assets": {},
            **kwargs,
        }

        if stac_extensions is not None:
            d["stac_extensions"] = stac_extensions
        return d

    @classmethod
    def _add_stac_extension(cls, stac_extensions: Union[List[str], None], stac_extension: str) -> List[str]:
        stac_extensions = list(stac_extensions or [])
        if stac_extension not in stac_extensions:
            stac_extensions.append(stac_extension)
        return stac_extensions

    @classmethod
    def collection(
        cls,
        *,
        id: str = "collection123",
        description: str = "Collection 123",
        stac_version: str = "1.0.0",
        stac_extensions: Optional[List[str]] = None,
        license: str = "proprietary",
        extent: Optional[dict] = None,
        cube_dimensions: Optional[dict] = None,
        summaries: Optional[dict] = None,
    ) -> dict:
        """Create a STAC Collection represented as dictionary."""
        if extent is None:
            extent = {"spatial": {"bbox": [[3, 4, 5, 6]]}, "temporal": {"interval": [["2024-01-01", "2024-05-05"]]}}

        d = {
            "type": "Collection",
            "stac_version": stac_version,
            "id": id,
            "description": description,
            "license": license,
            "extent": extent,
            "links": [],
        }
        if cube_dimensions is not None:
            d["cube:dimensions"] = cube_dimensions
            stac_extensions = cls._add_stac_extension(stac_extensions, cls._EXT_DATACUBE)
        if summaries is not None:
            d["summaries"] = summaries
        if stac_extensions is not None:
            d["stac_extensions"] = stac_extensions
        return d

    @classmethod
    def catalog(
        cls,
        *,
        id: str = "catalog123",
        stac_version: str = "1.0.0",
        description: str = "Catalog 123",
        stac_extensions: Optional[List[str]] = None,
    ) -> dict:
        """Create a STAC Catalog represented as dictionary."""
        d = {
            "type": "Catalog",
            "stac_version": stac_version,
            "id": id,
            "description": description,
            "links": [],
        }
        if stac_extensions is not None:
            d["stac_extensions"] = stac_extensions
        return d
