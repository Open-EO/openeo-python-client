from __future__ import annotations

import json
import re
from typing import List

import dirty_equals
import pystac
import pytest

from openeo.metadata import (
    _PYSTAC_1_9_EXTENSION_INTERFACE,
    Band,
    BandDimension,
    CollectionMetadata,
    CubeMetadata,
    Dimension,
    DimensionAlreadyExistsException,
    MetadataException,
    SpatialDimension,
    TemporalDimension,
    _StacMetadataParser,
    metadata_from_stac,
)
from openeo.testing.stac import StacDummyBuilder

CUBE_METADATA_XYTB = CubeMetadata(
    dimensions=[
        SpatialDimension(name="x", extent=[2, 7], crs=4326, step=0.1),
        SpatialDimension(name="y", extent=[49, 52], crs=4326, step=0.1),
        TemporalDimension(name="t", extent=["2024-09-01", "2024-12-01"]),
        BandDimension(name="bands", bands=[Band("B2"), Band("B3")]),
    ]
)

CUBE_METADATA_TBXY = CubeMetadata(
    dimensions=[
        TemporalDimension(name="t", extent=["2024-09-01", "2024-12-01"]),
        BandDimension(name="bands", bands=[Band("B2"), Band("B3")]),
        SpatialDimension(name="x", extent=[2, 7], crs=4326, step=0.1),
        SpatialDimension(name="y", extent=[49, 52], crs=4326, step=0.1),
    ]
)


class TestCollectionMetadata:

    def test_repr_html_basic(self):
        metadata = CollectionMetadata({"hello": "world"})
        assert metadata._repr_html_() == dirty_equals.IsStr(
            regex=r'.*<openeo-collection>.*"hello":\s*"world".*</openeo-collection>.*', regex_flags=re.DOTALL
        )


def test_metadata_get():
    metadata = CollectionMetadata({"foo": "bar", "very": {"deeply": {"nested": {"path": {"to": "somewhere"}}}}})
    assert metadata.get("foo") == "bar"
    assert metadata.get("very", "deeply", "nested", "path", "to") == "somewhere"
    assert metadata.get("invalid", "key") is None
    assert metadata.get("invalid", "key", default="nope") == "nope"


def test_metadata_extent():
    metadata = CollectionMetadata({"extent": {"spatial": {"xmin": 4, "xmax": 10}}})
    assert metadata.extent == {"spatial": {"xmin": 4, "xmax": 10}}


def test_band_minimal():
    band = Band("red")
    assert band.name == "red"


def test_band_dimension():
    bdim = BandDimension(
        name="spectral",
        bands=[
            Band("B02", "blue", 0.490),
            Band("B03", "green", 0.560),
            Band("B04", "red", 0.665),
        ],
    )
    assert bdim.band_names == ["B02", "B03", "B04"]
    assert bdim.common_names == ["blue", "green", "red"]


def test_band_dimension_band_index():
    bdim = BandDimension(
        name="spectral",
        bands=[
            Band("B02", "blue", 0.490),
            Band("B03", "green", 0.560),
            Band("B04", "red", 0.665),
            Band("B08", "nir", 0.842),
        ],
    )
    assert bdim.band_index(0) == 0
    assert bdim.band_index(2) == 2
    with pytest.raises(ValueError, match="Invalid band name/index"):
        bdim.band_index(-1)
    with pytest.raises(ValueError, match="Invalid band name/index"):
        bdim.band_index(4)
    assert bdim.band_index("B02") == 0
    assert bdim.band_index("B03") == 1
    assert bdim.band_index("B08") == 3
    assert bdim.band_index("blue") == 0
    assert bdim.band_index("green") == 1
    assert bdim.band_index("red") == 2
    assert bdim.band_index("nir") == 3
    with pytest.raises(ValueError, match="Invalid band name/index"):
        bdim.band_index("B05")
    with pytest.raises(ValueError, match="Invalid band name/index"):
        bdim.band_index("yellow")


def test_band_dimension_band_index_with_aliases():
    bdim = BandDimension(
        name="spectral",
        bands=[
            Band("B02", "blue", 0.490, aliases=["sky"]),
            Band("B03", "green", 0.560, aliases=["grass", "apple"]),
            Band("B04", "red", 0.560, aliases=["apple"]),
        ],
    )
    assert bdim.band_index("sky") == 0
    assert bdim.band_index("grass") == 1

    with pytest.raises(ValueError, match=r"Multiple alias matches for band 'apple': \['B03', 'B04'\]"):
        bdim.band_index("apple")


def test_band_dimension_contains_band():
    bdim = BandDimension(
        name="spectral",
        bands=[
            Band("B02", "blue", 0.490),
            Band("B03", "green", 0.560),
            Band("B04", "red", 0.665),
        ],
    )

    # Test band names
    assert bdim.contains_band("B02")
    assert not bdim.contains_band("B05")

    # Test indexes
    assert bdim.contains_band(0)
    assert not bdim.contains_band(4)

    # Test common names
    assert bdim.contains_band("blue")
    assert not bdim.contains_band("yellow")

def test_band_dimension_band_name():
    bdim = BandDimension(
        name="spectral",
        bands=[
            Band("B02", "blue", 0.490),
            Band("B03", "green", 0.560),
        ],
    )
    assert bdim.band_name("B02") == "B02"
    assert bdim.band_name("B03") == "B03"
    with pytest.raises(ValueError, match="Invalid band name/index"):
        bdim.band_name("B04")

    assert bdim.band_name("blue") == "blue"
    assert bdim.band_name("blue", allow_common=False) == "B02"
    assert bdim.band_name("green") == "green"
    assert bdim.band_name("green", allow_common=False) == "B03"
    with pytest.raises(ValueError, match="Invalid band name/index"):
        bdim.band_name("red")

    assert bdim.band_name(0) == "B02"
    assert bdim.band_name(1) == "B03"
    with pytest.raises(ValueError, match="Invalid band name/index"):
        bdim.band_name(2)


def test_band_dimension_band_name_with_aliases():
    bdim = BandDimension(
        name="spectral",
        bands=[
            Band("B02", "blue", 0.490, aliases=["sky"]),
            Band("B03", "green", 0.560, aliases=["grass", "apple"]),
            Band("B04", "red", 0.560, aliases=["apple"]),
        ],
    )
    assert bdim.band_name("sky") == "B02"
    assert bdim.band_name("grass") == "B03"

    with pytest.raises(ValueError, match=r"Multiple alias matches for band 'apple': \['B03', 'B04'\]"):
        bdim.band_name("apple")


def test_band_dimension_filter_bands():
    b02 = Band("B02", "blue", 0.490)
    b03 = Band("B03", "green", 0.560)
    b04 = Band("B04", "red", 0.665)
    bdim = BandDimension(name="bs", bands=[b02, b03, b04])
    assert bdim.filter_bands(["B03", "B04"]) == BandDimension(name="bs", bands=[b03, b04])
    assert bdim.filter_bands(["B04", "blue"]) == BandDimension(name="bs", bands=[b04, b02])
    assert bdim.filter_bands(["green", 2]) == BandDimension(name="bs", bands=[b03, b04])


def test_band_dimension_rename_labels():
    b02 = Band("B02", "blue", 0.490)
    b03 = Band("B03", "green", 0.560)
    b04 = Band("B04", "red", 0.665)
    bdim = BandDimension(name="bs", bands=[b02, b03, b04])

    metadata = CollectionMetadata({}, dimensions=[bdim])
    newdim = metadata.rename_labels("bs", target=["1", "2", "3"]).band_dimension
    assert metadata.band_dimension.band_names == ["B02", "B03", "B04"]
    assert newdim.band_names == ["1", "2", "3"]

    metadata = CubeMetadata(dimensions=[bdim])
    newdim = metadata.rename_labels("bs", target=["1", "2", "3"]).band_dimension
    assert metadata.band_dimension.band_names == ["B02", "B03", "B04"]
    assert newdim.band_names == ["1", "2", "3"]


def test_band_dimension_set_labels():
    bdim = BandDimension(name="bs", bands=[Band("some_name", None, None)])

    metadata = CollectionMetadata({}, dimensions=[bdim])
    newdim = metadata.rename_labels("bs", target=["1", "2", "3"]).band_dimension
    assert metadata.band_dimension.band_names == ["some_name"]
    assert newdim.band_names == ["1", "2", "3"]

    metadata = CubeMetadata(dimensions=[bdim])
    newdim = metadata.rename_labels("bs", target=["1", "2", "3"]).band_dimension
    assert metadata.band_dimension.band_names == ["some_name"]
    assert newdim.band_names == ["1", "2", "3"]


def test_band_dimension_rename_labels_with_source():
    b02 = Band("B02", "blue", 0.490)
    b03 = Band("B03", "green", 0.560)
    b04 = Band("B04", "red", 0.665)
    bdim = BandDimension(name="bs", bands=[b02, b03, b04])

    metadata = CollectionMetadata({}, dimensions=[bdim])
    newdim = metadata.rename_labels("bs", target=["2"], source=["B03"]).band_dimension
    assert metadata.band_dimension.band_names == ["B02", "B03", "B04"]
    assert newdim.band_names == ["B02", "2", "B04"]

    metadata = CubeMetadata(dimensions=[bdim])
    newdim = metadata.rename_labels("bs", target=["2"], source=["B03"]).band_dimension
    assert metadata.band_dimension.band_names == ["B02", "B03", "B04"]
    assert newdim.band_names == ["B02", "2", "B04"]


def test_band_dimension_rename_labels_with_source_mismatch():
    b02 = Band("B02", "blue", 0.490)
    b03 = Band("B03", "green", 0.560)
    bdim = BandDimension(name="bs", bands=[b02, b03])

    metadata = CollectionMetadata({}, dimensions=[bdim])
    with pytest.raises(ValueError, match="should have same number of labels, but got"):
        _ = metadata.rename_labels("bs", target=["2", "3"], source=["B03"])

    metadata = CubeMetadata(dimensions=[bdim])
    with pytest.raises(ValueError, match="should have same number of labels, but got"):
        _ = metadata.rename_labels("bs", target=["2", "3"], source=["B03"])


def test_band_dimension_append_band():
    bdim1 = BandDimension(
        name="spectral",
        bands=[
            Band("B02", "blue", 0.490),
            Band("B04", "red", 0.665),
        ],
    )
    bdim2 = bdim1.append_band(Band("B08", "nir", 0.842))
    bdim3 = bdim1.append_band("B03")
    assert bdim1.band_names == ["B02", "B04"]
    assert bdim1.common_names == ["blue", "red"]
    assert bdim2.band_names == ["B02", "B04", "B08"]
    assert bdim2.common_names == ["blue", "red", "nir"]
    assert bdim3.band_names == ["B02", "B04", "B03"]
    assert bdim3.common_names == ["blue", "red", None]


def assert_same_dimensions(dims1: List[Dimension], dims2: List[Dimension]):
    assert sorted(dims1, key=lambda d: d.name) == sorted(dims2, key=lambda d: d.name)


def test_get_dimensions_cube_dimensions_empty():
    dims = CollectionMetadata._parse_dimensions({})
    assert_same_dimensions(dims, [])


def test_get_dimensions_cube_dimensions_spatial_xyt():
    dims = CollectionMetadata._parse_dimensions(
        {
            "cube:dimensions": {
                "xx": {"type": "spatial", "extent": [-10, 10]},
                "yy": {"type": "spatial", "extent": [-56, 83], "reference_system": 123},
                "tt": {"type": "temporal", "extent": ["2020-02-20", None]},
            }
        }
    )
    assert_same_dimensions(
        dims,
        [
            SpatialDimension(name="xx", extent=[-10, 10]),
            SpatialDimension(name="yy", extent=[-56, 83], crs=123),
            TemporalDimension(name="tt", extent=["2020-02-20", None]),
        ],
    )


def test_get_dimensions_cube_dimensions_spatial_xyt_bands():
    dims = CollectionMetadata._parse_dimensions(
        {
            "cube:dimensions": {
                "x": {"type": "spatial", "extent": [-10, 10]},
                "y": {"type": "spatial", "extent": [-56, 83], "reference_system": 123},
                "t": {"type": "temporal", "extent": ["2020-02-20", None]},
                "spectral": {"type": "bands", "values": ["red", "green", "blue"]},
            }
        }
    )
    assert_same_dimensions(
        dims,
        [
            SpatialDimension(name="x", extent=[-10, 10]),
            SpatialDimension(name="y", extent=[-56, 83], crs=123),
            TemporalDimension(name="t", extent=["2020-02-20", None]),
            BandDimension(
                name="spectral",
                bands=[
                    Band("red", None, None),
                    Band("green", None, None),
                    Band("blue", None, None),
                ],
            ),
        ],
    )


def test_get_dimensions_cube_dimensions_non_standard_type():
    logs = []
    dims = CollectionMetadata._parse_dimensions(
        {
            "cube:dimensions": {
                "bar": {"type": "foo"},
            },
        },
        complain=logs.append,
    )
    assert_same_dimensions(dims, [Dimension(type="foo", name="bar")])
    assert logs == ["Unknown dimension type 'foo'"]


def test_get_dimensions_cube_dimensions_no_band_names():
    logs = []
    dims = CollectionMetadata._parse_dimensions(
        {
            "cube:dimensions": {
                "spectral": {"type": "bands"},
            },
        },
        complain=logs.append,
    )
    assert_same_dimensions(dims, [BandDimension(name="spectral", bands=[])])
    assert logs == ["No band names in dimension 'spectral'"]


@pytest.mark.parametrize(
    "summaries",
    [
        {
            # Pre-STAC-1.1-style eo:bands
            "eo:bands": [
                {"name": "r", "common_name": "red", "center_wavelength": 5},
                {"name": "g", "center_wavelength": 8},
                {"name": "b", "common_name": "blue"},
            ]
        },
        {
            # STAC 1.1-style bands
            "bands": [
                {"name": "r", "eo:common_name": "red", "eo:center_wavelength": 5},
                {"name": "g", "eo:center_wavelength": 8},
                {"name": "b", "eo:common_name": "blue"},
            ]
        },
    ],
)
def test_get_dimensions_cube_dimensions_eo_bands(summaries):
    dims = CollectionMetadata._parse_dimensions(
        {
            "cube:dimensions": {
                "x": {"type": "spatial", "extent": [-10, 10]},
                "y": {"type": "spatial", "extent": [-56, 83], "reference_system": 123},
                "t": {"type": "temporal", "extent": ["2020-02-20", None]},
                "spectral": {"type": "bands", "values": ["r", "g", "b"]},
            },
            "summaries": summaries,
        }
    )
    assert_same_dimensions(
        dims,
        [
            SpatialDimension(name="x", extent=[-10, 10]),
            SpatialDimension(name="y", extent=[-56, 83], crs=123),
            TemporalDimension(name="t", extent=["2020-02-20", None]),
            BandDimension(
                name="spectral",
                bands=[
                    Band("r", "red", 5),
                    Band("g", None, 8),
                    Band("b", "blue", None),
                ],
            ),
        ],
    )


@pytest.mark.parametrize(
    "summaries",
    [
        {
            # Pre-STAC-1.1-style eo:bands
            "eo:bands": [
                {"name": "y", "common_name": "yellow", "center_wavelength": 5},
                {"name": "c", "center_wavelength": 8},
                {"name": "m", "common_name": "magenta"},
            ]
        },
        {
            # STAC 1.1-style bands
            "bands": [
                {"name": "y", "eo:common_name": "yellow", "eo:center_wavelength": 5},
                {"name": "c", "eo:center_wavelength": 8},
                {"name": "m", "eo:common_name": "magenta"},
            ]
        },
    ],
)
def test_get_dimensions_cube_dimensions_eo_bands_mismatch(summaries):
    logs = []
    dims = CollectionMetadata._parse_dimensions(
        {
            "cube:dimensions": {
                "x": {"type": "spatial", "extent": [-10, 10]},
                "spectral": {"type": "bands", "values": ["r", "g", "b"]},
            },
            "summaries": summaries,
        },
        complain=logs.append,
    )
    assert_same_dimensions(
        dims,
        [
            SpatialDimension(name="x", extent=[-10, 10]),
            BandDimension(
                name="spectral",
                bands=[
                    Band("r", None, None),
                    Band("g", None, None),
                    Band("b", None, None),
                ],
            ),
        ],
    )
    assert logs == ["Band name mismatch: ['r', 'g', 'b'] != ['y', 'c', 'm']"]


@pytest.mark.parametrize(
    "summaries",
    [
        {
            # Pre-STAC-1.1-style eo:bands
            "eo:bands": [
                {"name": "y", "common_name": "yellow", "center_wavelength": 5},
                {"name": "c", "center_wavelength": 8},
                {"name": "m", "common_name": "magenta"},
            ]
        },
        {
            # STAC 1.1-style bands
            "bands": [
                {"name": "y", "eo:common_name": "yellow", "eo:center_wavelength": 5},
                {"name": "c", "eo:center_wavelength": 8},
                {"name": "m", "eo:common_name": "magenta"},
            ]
        },
    ],
)
def test_get_dimensions_eo_bands_only(summaries):
    logs = []
    dims = CollectionMetadata._parse_dimensions(
        {
            "summaries": {
                "eo:bands": [
                    {"name": "y", "common_name": "yellow", "center_wavelength": 5},
                    {"name": "c", "center_wavelength": 8},
                    {"name": "m", "common_name": "magenta"},
                ]
            }
        },
        complain=logs.append,
    )
    assert_same_dimensions(
        dims,
        [
            BandDimension(
                name="bands",
                bands=[
                    Band("y", "yellow", 5),
                    Band("c", None, 8),
                    Band("m", "magenta", None),
                ],
            )
        ],
    )
    assert logs == ["No cube:dimensions metadata", "Assuming name 'bands' for anonymous band dimension."]


@pytest.mark.parametrize(
    "summaries",
    [
        {
            # Pre-STAC-1.1-style eo:bands
            "eo:bands": [
                {"name": "y", "common_name": "yellow", "center_wavelength": 5},
                {"name": "c", "center_wavelength": 8},
                {"name": "m", "common_name": "magenta"},
            ]
        },
        {
            # STAC 1.1-style bands
            "bands": [
                {"name": "y", "eo:common_name": "yellow", "eo:center_wavelength": 5},
                {"name": "c", "eo:center_wavelength": 8},
                {"name": "m", "eo:common_name": "magenta"},
            ]
        },
    ],
)
def test_get_dimensions_no_band_dimension_with_eo_bands(summaries):
    logs = []
    dims = CollectionMetadata._parse_dimensions(
        {
            "cube:dimensions": {
                "x": {"type": "spatial", "extent": [-10, 10]},
            },
            "summaries": summaries,
        },
        complain=logs.append,
    )
    assert_same_dimensions(
        dims,
        [
            SpatialDimension(name="x", extent=[-10, 10]),
        ],
    )
    assert logs == ["No 'bands' dimension in 'cube:dimensions' while having 'eo:bands' or 'raster:bands'"]


@pytest.mark.parametrize(
    "summaries",
    [
        {
            # Pre-STAC-1.1-style eo:bands
            "eo:bands": [
                {"name": "zu", "common_name": "foo"},
            ]
        },
        {
            # STAC 1.1-style bands
            "bands": [
                {"name": "zu", "eo:common_name": "foo"},
            ]
        },
    ],
)
def test_get_dimensions_multiple_band_dimensions_with_eo_bands(summaries):
    logs = []
    dims = CollectionMetadata._parse_dimensions(
        {
            "cube:dimensions": {
                "x": {"type": "spatial", "extent": [-10, 10]},
                "spectral": {"type": "bands", "values": ["alpha", "beta"]},
                "bands": {"type": "bands", "values": ["r", "g", "b"]},
            },
            "summaries": summaries,
        },
        complain=logs.append,
    )
    assert_same_dimensions(
        dims,
        [
            SpatialDimension(name="x", extent=[-10, 10]),
            BandDimension(
                name="spectral",
                bands=[
                    Band("alpha", None, None),
                    Band("beta", None, None),
                ],
            ),
            BandDimension(
                name="bands",
                bands=[
                    Band("r", None, None),
                    Band("g", None, None),
                    Band("b", None, None),
                ],
            ),
        ],
    )
    assert logs == ["Multiple dimensions of type 'bands'"]


@pytest.mark.parametrize(
    "spec",
    [
        # API 0.4 style
        {
            "properties": {
                "cube:dimensions": {
                    "x": {"type": "spatial", "axis": "x"},
                    "b": {"type": "bands", "values": ["foo", "bar"]},
                }
            }
        },
        # API 1.0 style
        {"cube:dimensions": {"x": {"type": "spatial", "axis": "x"}, "b": {"type": "bands", "values": ["foo", "bar"]}}},
    ],
)
def test_collectionmetadata_bands_dimension_cube_dimensions(spec):
    metadata = CollectionMetadata(spec)
    assert metadata.band_dimension.name == "b"
    assert metadata.bands == [Band("foo", None, None), Band("bar", None, None)]
    assert metadata.band_names == ["foo", "bar"]
    assert metadata.band_common_names == [None, None]


def test_cubemetdata_bands_dimension():
    metadata = CubeMetadata(
        dimensions=[
            SpatialDimension(name="x", extent=None),
            BandDimension(name="b", bands=[Band("foo"), Band("bar")]),
        ]
    )
    assert metadata.band_dimension.name == "b"
    assert metadata.bands == [Band("foo", None, None), Band("bar", None, None)]
    assert metadata.band_names == ["foo", "bar"]
    assert metadata.band_common_names == [None, None]


def test_collectionmetadata_bands_dimension_no_band_dimensions():
    metadata = CollectionMetadata(
        {
            "cube:dimensions": {
                "x": {"type": "spatial", "axis": "x"},
            }
        }
    )
    with pytest.raises(MetadataException, match="No band dimension"):
        metadata.band_dimension
    with pytest.raises(MetadataException, match="No band dimension"):
        metadata.bands
    with pytest.raises(MetadataException, match="No band dimension"):
        metadata.band_common_names
    with pytest.raises(MetadataException, match="No band dimension"):
        metadata.get_band_index("red")
    with pytest.raises(MetadataException, match="No band dimension"):
        metadata.filter_bands(["red"])


def test_cubemetadata_bands_dimension_no_band_dimensions():
    metadata = CubeMetadata(dimensions=[SpatialDimension(name="x", extent=None)])
    with pytest.raises(MetadataException, match="No band dimension"):
        metadata.band_dimension
    with pytest.raises(MetadataException, match="No band dimension"):
        metadata.bands
    with pytest.raises(MetadataException, match="No band dimension"):
        metadata.band_common_names
    with pytest.raises(MetadataException, match="No band dimension"):
        metadata.get_band_index("red")
    with pytest.raises(MetadataException, match="No band dimension"):
        metadata.filter_bands(["red"])


@pytest.mark.parametrize(
    "spec",
    [
        {
            # API 0.4 style
            "properties": {
                "eo:bands": [{"name": "foo", "common_name": "F00", "center_wavelength": 0.543}, {"name": "bar"}]
            }
        },
        {
            # API 1.0 style
            "summaries": {
                "eo:bands": [{"name": "foo", "common_name": "F00", "center_wavelength": 0.543}, {"name": "bar"}]
            }
        },
        {
            # STAC 1.1 style
            "summaries": {
                "bands": [{"name": "foo", "eo:common_name": "F00", "eo:center_wavelength": 0.543}, {"name": "bar"}]
            }
        },
    ],
)
def test_metadata_bands_dimension_eo_bands(spec):
    metadata = CollectionMetadata(spec)
    assert metadata.band_dimension.name == "bands"
    assert metadata.bands == [Band("foo", "F00", 0.543), Band("bar", None, None)]
    assert metadata.band_names == ["foo", "bar"]
    assert metadata.band_common_names == ["F00", None]


@pytest.mark.parametrize(
    "spec",
    [
        {
            # API 0.4 style
            "properties": {
                "cube:dimensions": {
                    "x": {"type": "spatial", "axis": "x"},
                    "b": {"type": "bands", "values": ["foo", "bar"]},
                },
                "eo:bands": [{"name": "foo", "common_name": "F00", "center_wavelength": 0.543}, {"name": "bar"}],
            }
        },
        {
            # API 1.0 style
            "cube:dimensions": {
                "x": {"type": "spatial", "axis": "x"},
                "b": {"type": "bands", "values": ["foo", "bar"]},
            },
            "summaries": {
                "eo:bands": [{"name": "foo", "common_name": "F00", "center_wavelength": 0.543}, {"name": "bar"}]
            },
        },
        {
            # STAC 1.1 bands style
            "cube:dimensions": {
                "x": {"type": "spatial", "axis": "x"},
                "b": {"type": "bands", "values": ["foo", "bar"]},
            },
            "summaries": {
                "bands": [{"name": "foo", "eo:common_name": "F00", "eo:center_wavelength": 0.543}, {"name": "bar"}]
            },
        },
    ],
)
def test_metadata_bands_dimension(spec):
    metadata = CollectionMetadata(spec)
    assert metadata.band_dimension.name == "b"
    assert metadata.bands == [Band("foo", "F00", 0.543), Band("bar", None, None)]
    assert metadata.band_names == ["foo", "bar"]
    assert metadata.band_common_names == ["F00", None]


def test_cubemetadata_rename_labels():
    metadata = CubeMetadata(
        dimensions=[
            SpatialDimension(name="x", extent=None),
            BandDimension(name="b", bands=[Band("red"), Band("green")]),
        ]
    )
    renamed = metadata.rename_labels(dimension="b", target=["raspberry", "lime"])
    assert isinstance(renamed, CubeMetadata)
    assert renamed.dimension_names() == ["x", "b"]
    assert renamed.band_names == ["raspberry", "lime"]
    assert renamed.bands == [Band("raspberry"), Band("lime")]


def test_cubemetadata_rename_labels_invalid_dimension():
    metadata = CubeMetadata(
        dimensions=[
            SpatialDimension(name="x", extent=None),
            BandDimension(name="b", bands=[Band("red"), Band("green")]),
        ]
    )
    with pytest.raises(ValueError, match=re.escape("Invalid dimension 'tempus'. Should be one of ['x', 'b']")):
        _ = metadata.rename_labels(dimension="tempus", target=["1999"])


def test_cubemetadata_rename_dimension():
    metadata = CubeMetadata(
        dimensions=[
            SpatialDimension(name="x", extent=None),
            BandDimension(name="b", bands=[Band("red"), Band("green")]),
        ]
    )
    renamed = metadata.rename_dimension("b", "bands")
    assert renamed.dimension_names() == ["x", "bands"]
    assert renamed.band_dimension.name == "bands"
    assert renamed.band_names == ["red", "green"]


def test_cubemetadata_rename_dimension_invalid_dimension():
    metadata = CubeMetadata(
        dimensions=[
            SpatialDimension(name="x", extent=None),
            BandDimension(name="b", bands=[Band("red"), Band("green")]),
        ]
    )
    with pytest.raises(ValueError, match=re.escape("Invalid dimension 'bandzz'. Should be one of ['x', 'b']")):
        _ = metadata.rename_dimension("bandzz", "bands")


def test_collectionmetadata_reduce_dimension():
    metadata = CollectionMetadata(
        {"cube:dimensions": {"x": {"type": "spatial"}, "b": {"type": "bands", "values": ["red", "green"]}}}
    )
    reduced = metadata.reduce_dimension("b")
    assert set(metadata.dimension_names()) == {"x", "b"}
    assert set(reduced.dimension_names()) == {"x"}


def test_cubemetadata_reduce_dimension():
    metadata = CubeMetadata(
        dimensions=[
            SpatialDimension(name="x", extent=None),
            BandDimension(name="b", bands=[Band("red"), Band("green")]),
        ]
    )
    reduced = metadata.reduce_dimension("b")
    assert set(metadata.dimension_names()) == {"x", "b"}
    assert set(reduced.dimension_names()) == {"x"}


def test_collectionmetadata_reduce_dimension_invalid_name():
    metadata = CollectionMetadata(
        {"cube:dimensions": {"x": {"type": "spatial"}, "b": {"type": "bands", "values": ["red", "green"]}}}
    )
    with pytest.raises(ValueError):
        metadata.reduce_dimension("y")


def test_cubemetadata_reduce_dimension_invalid_name():
    metadata = CubeMetadata(
        dimensions=[
            SpatialDimension(name="x", extent=None),
            BandDimension(name="b", bands=[Band("red"), Band("green")]),
        ]
    )
    with pytest.raises(ValueError):
        metadata.reduce_dimension("y")


def test_collectionmetadata_add_band_dimension():
    metadata = CollectionMetadata({"cube:dimensions": {"t": {"type": "temporal"}}})
    new = metadata.add_dimension("layer", "red", "bands")

    assert metadata.dimension_names() == ["t"]
    assert not metadata.has_band_dimension()

    assert new.has_band_dimension()
    assert new.dimension_names() == ["t", "layer"]
    assert new.band_dimension.name == "layer"
    assert new.band_names == ["red"]


def test_cubemetadata_add_band_dimension():
    metadata = CubeMetadata(dimensions=[TemporalDimension(name="t", extent=None)])
    new = metadata.add_dimension("layer", "red", "bands")

    assert metadata.dimension_names() == ["t"]
    assert not metadata.has_band_dimension()

    assert new.has_band_dimension()
    assert new.dimension_names() == ["t", "layer"]
    assert new.band_dimension.name == "layer"
    assert new.band_names == ["red"]


def test_collectionmetadata_add_band_dimension_duplicate():
    metadata = CollectionMetadata({"cube:dimensions": {"t": {"type": "temporal"}}})
    metadata = metadata.add_dimension("layer", "red", "bands")
    with pytest.raises(DimensionAlreadyExistsException, match="Dimension with name 'layer' already exists"):
        _ = metadata.add_dimension("layer", "red", "bands")


def test_cubemetadata_add_band_dimension_duplicate():
    metadata = CubeMetadata(dimensions=[TemporalDimension(name="t", extent=None)])
    metadata = metadata.add_dimension("layer", "red", "bands")
    with pytest.raises(DimensionAlreadyExistsException, match="Dimension with name 'layer' already exists"):
        _ = metadata.add_dimension("layer", "red", "bands")


def test_cubemetadata_append_band():
    metadata1 = CubeMetadata(
        dimensions=[
            BandDimension(name="b", bands=[Band("red"), Band("green")]),
        ]
    )
    metadata2 = metadata1.append_band(Band("blue"))
    metadata3 = metadata1.append_band("pink")
    assert metadata1.band_names == ["red", "green"]
    assert metadata2.band_names == ["red", "green", "blue"]
    assert metadata3.band_names == ["red", "green", "pink"]


def test_collectionmetadata_add_temporal_dimension():
    metadata = CollectionMetadata({"cube:dimensions": {"x": {"type": "spatial"}}})
    new = metadata.add_dimension("date", "2020-05-15", "temporal")

    assert metadata.dimension_names() == ["x"]
    assert not metadata.has_temporal_dimension()

    assert new.has_temporal_dimension()
    assert new.dimension_names() == ["x", "date"]
    assert new.temporal_dimension.name == "date"
    assert new.temporal_dimension.extent == ["2020-05-15", "2020-05-15"]


def test_cubemetadata_add_temporal_dimension():
    metadata = CubeMetadata(dimensions=[SpatialDimension(name="x", extent=None)])
    new = metadata.add_dimension("date", "2020-05-15", "temporal")

    assert metadata.dimension_names() == ["x"]
    assert not metadata.has_temporal_dimension()

    assert new.has_temporal_dimension()
    assert new.dimension_names() == ["x", "date"]
    assert new.temporal_dimension.name == "date"
    assert new.temporal_dimension.extent == ["2020-05-15", "2020-05-15"]


def test_collectionmetadata_add_temporal_dimension_duplicate():
    metadata = CollectionMetadata({"cube:dimensions": {"x": {"type": "spatial"}}})
    metadata = metadata.add_dimension("date", "2020-05-15", "temporal")
    with pytest.raises(DimensionAlreadyExistsException, match="Dimension with name 'date' already exists"):
        _ = metadata.add_dimension("date", "2020-05-15", "temporal")


def test_cubemetadata_add_temporal_dimension_duplicate():
    metadata = CubeMetadata(dimensions=[SpatialDimension(name="x", extent=None)])
    metadata = metadata.add_dimension("date", "2020-05-15", "temporal")
    with pytest.raises(DimensionAlreadyExistsException, match="Dimension with name 'date' already exists"):
        _ = metadata.add_dimension("date", "2020-05-15", "temporal")


def test_cube_metadata_add_dimension_geometry():
    orig = CubeMetadata(dimensions=[TemporalDimension(name="t", extent=None)])
    new = orig.add_dimension(name="fields", label="Mol", type="geometry")

    assert orig.dimension_names() == ["t"]
    assert not orig.has_geometry_dimension()
    with pytest.raises(MetadataException):
        orig.geometry_dimension()

    assert new.dimension_names() == ["t", "fields"]
    assert new.has_geometry_dimension()
    assert new.geometry_dimension.name == "fields"
    assert new.geometry_dimension.type == "geometry"

def test_collectionmetadata_drop_dimension():
    metadata = CollectionMetadata(
        {
            "cube:dimensions": {
                "t": {"type": "temporal"},
                "b": {"type": "bands", "values": ["red", "green"]},
            }
        }
    )

    new = metadata.drop_dimension("t")
    assert metadata.dimension_names() == ["t", "b"]
    assert new.dimension_names() == ["b"]
    assert new.band_dimension.band_names == ["red", "green"]

    new = metadata.drop_dimension("b")
    assert metadata.dimension_names() == ["t", "b"]
    assert new.dimension_names() == ["t"]
    assert new.temporal_dimension.name == "t"

    with pytest.raises(ValueError):
        metadata.drop_dimension("x")


def test_cubemetadata_drop_dimension():
    metadata = CubeMetadata(
        dimensions=[
            TemporalDimension(name="t", extent=None),
            BandDimension(name="b", bands=[Band("red"), Band("green")]),
        ]
    )

    new = metadata.drop_dimension("t")
    assert metadata.dimension_names() == ["t", "b"]
    assert new.dimension_names() == ["b"]
    assert new.band_dimension.band_names == ["red", "green"]

    new = metadata.drop_dimension("b")
    assert metadata.dimension_names() == ["t", "b"]
    assert new.dimension_names() == ["t"]
    assert new.temporal_dimension.name == "t"

    with pytest.raises(ValueError):
        metadata.drop_dimension("x")


def test_cubemetadata_ensure_band_dimension_add_bands():
    metadata = CubeMetadata(
        dimensions=[
            TemporalDimension(name="t", extent=None),
        ]
    )
    new = metadata._ensure_band_dimension(bands=["red", "green"], warning="ensure_band_dimension at work")
    assert new.has_band_dimension()
    assert new.dimension_names() == ["t", "bands"]
    assert new.band_names == ["red", "green"]


def test_cubemetadata_ensure_band_dimension_add_name_and_bands():
    metadata = CubeMetadata(
        dimensions=[
            TemporalDimension(name="t", extent=None),
        ]
    )
    new = metadata._ensure_band_dimension(
        name="bandzz", bands=["red", "green"], warning="ensure_band_dimension at work"
    )
    assert new.has_band_dimension()
    assert new.dimension_names() == ["t", "bandzz"]
    assert new.band_names == ["red", "green"]


def test_cubemetadata_ensure_band_dimension_override_bands():
    metadata = CubeMetadata(
        dimensions=[
            TemporalDimension(name="t", extent=None),
            BandDimension(name="bands", bands=[Band("red"), Band("green")]),
        ]
    )
    new = metadata._ensure_band_dimension(bands=["tomato", "lettuce"], warning="ensure_band_dimension at work")
    assert new.has_band_dimension()
    assert new.dimension_names() == ["t", "bands"]
    assert new.band_names == ["tomato", "lettuce"]


def test_cubemetadata_ensure_band_dimension_override_name_and_bands():
    metadata = CubeMetadata(
        dimensions=[
            TemporalDimension(name="t", extent=None),
            BandDimension(name="bands", bands=[Band("red"), Band("green")]),
        ]
    )
    new = metadata._ensure_band_dimension(
        name="bandzz", bands=["tomato", "lettuce"], warning="ensure_band_dimension at work"
    )
    assert new.has_band_dimension()
    assert new.dimension_names() == ["t", "bandzz"]
    assert new.band_names == ["tomato", "lettuce"]


def test_collectionmetadata_subclass():
    class MyCollectionMetadata(CollectionMetadata):
        def __init__(self, metadata: dict, dimensions: List[Dimension] = None, bbox=None):
            super().__init__(metadata=metadata, dimensions=dimensions)
            self.bbox = bbox

        def _clone_and_update(
            self, metadata: dict = None, dimensions: List[Dimension] = None, bbox=None, **kwargs
        ) -> MyCollectionMetadata:
            return super()._clone_and_update(metadata=metadata, dimensions=dimensions, bbox=bbox or self.bbox, **kwargs)

        def filter_bbox(self, bbox):
            return self._clone_and_update(bbox=bbox)

    orig = MyCollectionMetadata({"cube:dimensions": {"x": {"type": "spatial"}}})
    assert orig.bbox is None

    new = orig.add_dimension(name="layer", label="red", type="bands")
    assert isinstance(new, MyCollectionMetadata)
    assert orig.bbox is None
    assert new.bbox is None

    new = new.filter_bbox((1, 2, 3, 4))
    assert isinstance(new, MyCollectionMetadata)
    assert orig.bbox is None
    assert new.bbox == (1, 2, 3, 4)

    new = new.add_dimension(name="time", label="2020", type="time")
    assert isinstance(new, MyCollectionMetadata)
    assert orig.bbox is None
    assert new.bbox == (1, 2, 3, 4)


def test_cubemetadata_subclass():
    class MyCubeMetadata(CubeMetadata):
        def __init__(self, dimensions: List[Dimension], bbox=None):
            super().__init__(dimensions=dimensions)
            self.bbox = bbox

        def _clone_and_update(self, dimensions: List[Dimension] = None, bbox=None, **kwargs) -> MyCubeMetadata:
            return super()._clone_and_update(dimensions=dimensions, bbox=bbox or self.bbox, **kwargs)

        def filter_bbox(self, bbox):
            return self._clone_and_update(bbox=bbox)

    orig = MyCubeMetadata([SpatialDimension(name="x", extent=None)])
    assert orig.bbox is None

    new = orig.add_dimension(name="layer", label="red", type="bands")
    assert isinstance(new, MyCubeMetadata)
    assert orig.bbox is None
    assert new.bbox is None

    new = new.filter_bbox((1, 2, 3, 4))
    assert isinstance(new, MyCubeMetadata)
    assert orig.bbox is None
    assert new.bbox == (1, 2, 3, 4)

    new = new.add_dimension(name="time", label="2020", type="time")
    assert isinstance(new, MyCubeMetadata)
    assert orig.bbox is None
    assert new.bbox == (1, 2, 3, 4)


@pytest.mark.parametrize(
    "test_stac, expected",
    [
        (
            StacDummyBuilder.collection(summaries={"eo:bands": [{"name": "B01"}, {"name": "B02"}]}),
            ["B01", "B02"],
        ),
        (
            StacDummyBuilder.collection(summaries={"bands": [{"name": "B01"}, {"name": "B02"}]}),
            ["B01", "B02"],
        ),
        # TODO: test asset handling in collection?
        (
            StacDummyBuilder.catalog(),
            [],
        ),
        (
            StacDummyBuilder.item(
                properties={"datetime": "2020-05-22T00:00:00Z", "eo:bands": [{"name": "SCL"}, {"name": "B08"}]}
            ),
            ["SCL", "B08"],
        ),
        (
            StacDummyBuilder.item(
                properties={"datetime": "2020-05-22T00:00:00Z", "bands": [{"name": "SCL"}, {"name": "B08"}]}
            ),
            ["SCL", "B08"],
        ),
        # TODO: test asset handling in item?
    ],
)
def test_metadata_from_stac_bands(tmp_path, test_stac, expected):
    path = tmp_path / "stac.json"
    # TODO #738 real request mocking of STAC resources compatible with pystac?
    path.write_text(json.dumps(test_stac))
    metadata = metadata_from_stac(str(path))
    assert metadata.band_names == expected



@pytest.mark.skipif(not _PYSTAC_1_9_EXTENSION_INTERFACE, reason="Requires PySTAC 1.9+ extension interface")
@pytest.mark.parametrize(
    ["eo_extension_is_declared", "expected_warnings"],
    [
        (
            False,
            [
                "Deriving band listing from unordered `item_assets`",
                "Using 'eo:bands' metadata, but STAC extension eo was not declared.",
            ],
        ),
        (True, ["Deriving band listing from unordered `item_assets`"]),
    ],
)
def test_metadata_from_stac_collection_bands_from_item_assets(
    test_data, tmp_path, eo_extension_is_declared, caplog, expected_warnings
):
    stac_data = test_data.load_json("stac/collections/agera5_daily01.json")
    stac_data["stac_extensions"] = [
        ext
        for ext in stac_data["stac_extensions"]
        if (not ext.startswith("https://stac-extensions.github.io/eo/") or eo_extension_is_declared)
    ]
    assert (
        any(ext.startswith("https://stac-extensions.github.io/eo/") for ext in stac_data["stac_extensions"])
        == eo_extension_is_declared
    )
    path = tmp_path / "stac.json"
    path.write_text(json.dumps(stac_data))

    metadata = metadata_from_stac(str(path))
    assert sorted(metadata.band_names) == [
        "2m_temperature_max",
        "2m_temperature_min",
        "dewpoint_temperature_mean",
        "vapour_pressure",
    ]

    assert caplog.messages == expected_warnings


@pytest.mark.skipif(
    not _PYSTAC_1_9_EXTENSION_INTERFACE,
    reason="No backport of implementation/test below PySTAC 1.9 extension interface",
)
@pytest.mark.parametrize(
    ["stac_dict", "expected"],
    [
        (
            StacDummyBuilder.item(),
            None,
        ),
        (
            StacDummyBuilder.item(cube_dimensions={"t": {"type": "temporal", "extent": ["2024-04-04", "2024-06-06"]}}),
            ("t", ["2024-04-04", "2024-06-06"]),
        ),
        (
            StacDummyBuilder.item(
                cube_dimensions={"datezz": {"type": "temporal", "extent": ["2024-04-04", "2024-06-06"]}}
            ),
            ("datezz", ["2024-04-04", "2024-06-06"]),
        ),
        (
            StacDummyBuilder.collection(),
            ("t", ["2024-01-01T00:00:00Z", "2024-05-05T00:00:00Z"]),
        ),
        (
            StacDummyBuilder.collection(
                cube_dimensions={"t": {"type": "temporal", "extent": ["2024-04-04", "2024-06-06"]}}
            ),
            ("t", ["2024-04-04", "2024-06-06"]),
        ),
        (
            StacDummyBuilder.catalog(),
            None,
        ),
        (
            # Note: a catalog is not supposed to have datacube extension enabled, but we should not choke on that
            StacDummyBuilder.catalog(stac_extensions=[StacDummyBuilder._EXT_DATACUBE]),
            None,
        ),
    ],
)
def test_metadata_from_stac_temporal_dimension(tmp_path, stac_dict, expected):
    path = tmp_path / "stac.json"
    # TODO #738 real request mocking of STAC resources compatible with pystac?
    path.write_text(json.dumps(stac_dict))
    metadata = metadata_from_stac(str(path))
    if expected:
        dim = metadata.temporal_dimension
        assert isinstance(dim, TemporalDimension)
        assert (dim.name, dim.extent) == expected
    else:
        assert not metadata.has_temporal_dimension()


@pytest.mark.parametrize(
    ["kwargs", "expected_x", "expected_y"],
    [
        ({}, {"crs": 4326, "step": 0.1}, {"crs": 4326, "step": 0.1}),
        ({"resolution": 2}, {"crs": 4326, "step": 2}, {"crs": 4326, "step": 2}),
        ({"resolution": [0.5, 2]}, {"crs": 4326, "step": 0.5}, {"crs": 4326, "step": 2}),
        ({"projection": 32631}, {"crs": 32631, "step": 0.1}, {"crs": 32631, "step": 0.1}),
        ({"resolution": 10, "projection": 32631}, {"crs": 32631, "step": 10}, {"crs": 32631, "step": 10}),
        ({"resolution": [11, 22], "projection": 32631}, {"crs": 32631, "step": 11}, {"crs": 32631, "step": 22}),
    ],
)
@pytest.mark.parametrize("cube_metadata", [CUBE_METADATA_XYTB, CUBE_METADATA_TBXY])
def test_metadata_resample_spatial(cube_metadata, kwargs, expected_x, expected_y):
    metadata = cube_metadata.resample_spatial(**kwargs)
    assert isinstance(metadata, CubeMetadata)
    assert metadata.spatial_dimensions == [
        SpatialDimension(name="x", extent=[2, 7], **expected_x),
        SpatialDimension(name="y", extent=[49, 52], **expected_y),
    ]
    assert metadata.temporal_dimension == cube_metadata.temporal_dimension
    assert metadata.band_dimension == cube_metadata.band_dimension


@pytest.mark.parametrize("cube_metadata", [CUBE_METADATA_XYTB, CUBE_METADATA_TBXY])
def test_metadata_resample_cube_spatial(cube_metadata):
    metadata1 = cube_metadata.resample_spatial(resolution=(11, 22), projection=32631)
    metadata2 = cube_metadata.resample_spatial(resolution=0.5)

    assert metadata1.spatial_dimensions == [
        SpatialDimension(name="x", extent=[2, 7], crs=32631, step=11),
        SpatialDimension(name="y", extent=[49, 52], crs=32631, step=22),
    ]
    assert metadata2.spatial_dimensions == [
        SpatialDimension(name="x", extent=[2, 7], crs=4326, step=0.5),
        SpatialDimension(name="y", extent=[49, 52], crs=4326, step=0.5),
    ]

    metadata12 = metadata1.resample_cube_spatial(target=metadata2)
    assert metadata12.spatial_dimensions == [
        SpatialDimension(name="x", extent=[2, 7], crs=4326, step=0.5),
        SpatialDimension(name="y", extent=[49, 52], crs=4326, step=0.5),
    ]

    metadata21 = metadata2.resample_cube_spatial(target=metadata1)
    assert metadata21.spatial_dimensions == [
        SpatialDimension(name="x", extent=[2, 7], crs=32631, step=11),
        SpatialDimension(name="y", extent=[49, 52], crs=32631, step=22),
    ]


def test_metadata_resample_cube_spatial_preserve_non_spatial():
    xy1 = [
        SpatialDimension(name="x", extent=[12, 17], crs=4326, step=0.1),
        SpatialDimension(name="y", extent=[41, 51], crs=4326, step=0.1),
    ]
    t1 = TemporalDimension(name="t", extent=["2024-01-01", "2024-10-01"])
    b1 = BandDimension(name="bands", bands=[Band("B1"), Band("B11")])
    metadata1 = CubeMetadata(dimensions=xy1 + [t1, b1])

    xy2 = [
        SpatialDimension(name="x", extent=[22, 27], crs=4326, step=0.1),
        SpatialDimension(name="y", extent=[42, 52], crs=4326, step=0.1),
    ]
    t2 = TemporalDimension(name="t", extent=["2024-02-02", "2024-12-02"])
    metadata2 = CubeMetadata(dimensions=xy2 + [t2])

    xy3 = [
        SpatialDimension(name="x", extent=[32, 37], crs=4326, step=0.1),
        SpatialDimension(name="y", extent=[43, 53], crs=4326, step=0.1),
    ]
    b3 = BandDimension(name="bands", bands=[Band("B3"), Band("B33")])
    metadata3 = CubeMetadata(dimensions=xy3 + [b3])

    result12 = metadata1.resample_cube_spatial(target=metadata2)
    assert result12.spatial_dimensions == xy2
    assert result12.band_dimension == b1
    assert result12.temporal_dimension == t1

    result21 = metadata2.resample_cube_spatial(target=metadata1)
    assert result21.spatial_dimensions == xy1
    assert not result21.has_band_dimension()
    assert result21.temporal_dimension == t2

    result23 = metadata2.resample_cube_spatial(target=metadata3)
    assert result23.spatial_dimensions == xy3
    assert not result23.has_band_dimension()
    assert result23.temporal_dimension == t2

    result31 = metadata3.resample_cube_spatial(target=metadata1)
    assert result31.spatial_dimensions == xy1
    assert result31.band_dimension == b3
    assert not result31.has_temporal_dimension()


def test_cube_metadata_repr_empty():
    metadata = CubeMetadata()
    assert repr(metadata) == "CubeMetadata(dimensions=None)"


def test_cube_metadata_repr_with_bands():
    assert repr(CUBE_METADATA_XYTB) == "CubeMetadata(dimension_names=['x', 'y', 't', 'bands'], band_names=['B2', 'B3'])"


def test_cube_metadata_repr_no_bands():
    metadata = CubeMetadata(
        dimensions=[
            SpatialDimension(name="x", extent=[2, 7], crs=4326, step=0.1),
            TemporalDimension(name="t", extent=["2024-09-01", "2024-12-01"]),
        ]
    )
    assert repr(metadata) == "CubeMetadata(dimension_names=['x', 't'])"


class TestStacMetadataParser:
    def test_band_from_eo_bands_metadata(self):
        assert _StacMetadataParser()._band_from_eo_bands_metadata(
            {"name": "B04"},
        ) == Band(name="B04")
        assert _StacMetadataParser()._band_from_eo_bands_metadata(
            {"name": "B04", "common_name": "red", "center_wavelength": 0.665}
        ) == Band(name="B04", common_name="red", wavelength_um=0.665)

    def test_band_from_common_bands_metadata(self):
        assert _StacMetadataParser()._band_from_common_bands_metadata(
            {"name": "B04"},
        ) == Band(name="B04")
        assert _StacMetadataParser()._band_from_common_bands_metadata(
            {"name": "B04", "eo:common_name": "red", "eo:center_wavelength": 0.665}
        ) == Band(name="B04", common_name="red", wavelength_um=0.665)

    @pytest.mark.parametrize(
        ["data", "expected", "expected_warnings"],
        [
            (
                {
                    "type": "Catalog",
                    "id": "catalog123",
                    "description": "Catalog 123",
                    "stac_version": "1.0.0",
                    "stac_extensions": ["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
                    "summaries": {
                        "eo:bands": [
                            {"name": "B04", "common_name": "red", "center_wavelength": 0.665},
                            {"name": "B03", "common_name": "green", "center_wavelength": 0.560},
                        ],
                    },
                    "links": [],
                },
                [
                    Band("B04", common_name="red", wavelength_um=0.665),
                    Band("B03", common_name="green", wavelength_um=0.560),
                ],
                [
                    "bands_from_stac_catalog with summaries.keys()=dict_keys(['eo:bands']) (which is non-standard)",
                ],
            ),
            (
                {
                    "type": "Catalog",
                    "id": "catalog123",
                    "description": "Catalog 123",
                    "stac_version": "1.0.0",
                    "summaries": {
                        "eo:bands": [
                            {"name": "B04", "common_name": "red", "center_wavelength": 0.665},
                            {"name": "B03", "common_name": "green", "center_wavelength": 0.560},
                        ],
                    },
                    "links": [],
                },
                [
                    Band("B04", common_name="red", wavelength_um=0.665),
                    Band("B03", common_name="green", wavelength_um=0.560),
                ],
                [
                    "bands_from_stac_catalog with summaries.keys()=dict_keys(['eo:bands']) (which is non-standard)",
                    "Using 'eo:bands' metadata, but STAC extension eo was not declared.",
                ],
            ),
            (
                {
                    "type": "Catalog",
                    "id": "catalog123",
                    "description": "Catalog 123",
                    "stac_version": "1.1.0",
                    "summaries": {
                        "bands": [
                            {"name": "B04"},
                            {"name": "B03"},
                        ],
                    },
                    "links": [],
                },
                [Band("B04"), Band("B03")],
                [
                    "bands_from_stac_catalog with summaries.keys()=dict_keys(['bands']) (which is non-standard)",
                ],
            ),
            (
                {
                    "type": "Catalog",
                    "id": "catalog123",
                    "description": "Catalog 123",
                    "stac_version": "1.1.0",
                    "summaries": {
                        "bands": [
                            {"name": "B04", "eo:common_name": "red", "eo:center_wavelength": 0.665},
                            {"name": "B03", "eo:common_name": "green", "eo:center_wavelength": 0.560},
                        ],
                    },
                    "links": [],
                },
                [
                    Band("B04", common_name="red", wavelength_um=0.665),
                    Band("B03", common_name="green", wavelength_um=0.560),
                ],
                [
                    "bands_from_stac_catalog with summaries.keys()=dict_keys(['bands']) (which is non-standard)",
                ],
            ),
        ],
    )
    def test_bands_from_stac_catalog(self, data, expected, expected_warnings, caplog):
        catalog = pystac.Catalog.from_dict(data)
        assert _StacMetadataParser().bands_from_stac_catalog(catalog=catalog) == expected

        if _PYSTAC_1_9_EXTENSION_INTERFACE:
            assert caplog.messages == expected_warnings

    @pytest.mark.parametrize(
        ["data", "expected", "expected_warnings"],
        [
            (
                StacDummyBuilder.collection(
                    stac_version="1.0.0",
                    stac_extensions=["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
                    summaries={"eo:bands": [{"name": "B02"}, {"name": "B03"}, {"name": "B04"}]},
                ),
                [Band("B02"), Band("B03"), Band("B04")],
                [],
            ),
            (
                StacDummyBuilder.collection(
                    stac_version="1.0.0",
                    stac_extensions=["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
                    summaries={
                        "eo:bands": [
                            {"name": "B04", "common_name": "red", "center_wavelength": 0.665},
                            {"name": "B03", "common_name": "green", "center_wavelength": 0.560},
                        ],
                    },
                ),
                [
                    Band("B04", common_name="red", wavelength_um=0.665),
                    Band("B03", common_name="green", wavelength_um=0.560),
                ],
                [],
            ),
            (
                StacDummyBuilder.collection(
                    stac_version="1.0.0",
                    summaries={"eo:bands": [{"name": "B02"}, {"name": "B03"}, {"name": "B04"}]},
                ),
                [Band("B02"), Band("B03"), Band("B04")],
                ["Using 'eo:bands' metadata, but STAC extension eo was not declared."],
            ),
            (
                StacDummyBuilder.collection(
                    stac_version="1.1.0",
                    summaries={"bands": [{"name": "B02"}, {"name": "B03"}, {"name": "B04"}]},
                ),
                [Band("B02"), Band("B03"), Band("B04")],
                [],
            ),
            (
                StacDummyBuilder.collection(
                    stac_version="1.1.0",
                    stac_extensions=["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
                    summaries={
                        "bands": [
                            {"name": "B04", "eo:common_name": "red", "eo:center_wavelength": 0.665},
                            {"name": "B03", "eo:common_name": "green", "eo:center_wavelength": 0.560},
                        ],
                    },
                ),
                [
                    Band("B04", common_name="red", wavelength_um=0.665),
                    Band("B03", common_name="green", wavelength_um=0.560),
                ],
                [],
            ),
        ],
    )
    def test_bands_from_stac_collection(self, data, expected, caplog, expected_warnings):
        collection = pystac.Collection.from_dict(data)
        assert _StacMetadataParser().bands_from_stac_collection(collection=collection) == expected

        if _PYSTAC_1_9_EXTENSION_INTERFACE:
            assert caplog.messages == expected_warnings

    @pytest.mark.parametrize(
        ["entities", "kwargs", "expected", "expected_warnings"],
        [
            (
                # Basic: Collection with one item with eo:bands
                {
                    "collection.json": StacDummyBuilder.collection(
                        stac_version="1.0.0",
                        links=[
                            {"rel": "item", "href": "item.json"},
                        ],
                    ),
                    "item.json": StacDummyBuilder.item(
                        stac_version="1.0.0",
                        stac_extensions=["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
                        properties={
                            "eo:bands": [{"name": "B02"}, {"name": "B03"}],
                        },
                    ),
                },
                {},
                [Band("B02"), Band("B03")],
                [
                    "bands_from_stac_collection: consulting items for band metadata",
                ],
            ),
            (
                # Collection with two items with different bands (eo:bands metadata)
                {
                    "collection.json": StacDummyBuilder.collection(
                        stac_version="1.0.0",
                        links=[
                            {"rel": "item", "href": "item1.json"},
                            {"rel": "item", "href": "item2.json"},
                        ],
                    ),
                    "item1.json": StacDummyBuilder.item(
                        stac_version="1.0.0",
                        stac_extensions=["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
                        properties={
                            "eo:bands": [{"name": "B02"}],
                        },
                    ),
                    "item2.json": StacDummyBuilder.item(
                        stac_version="1.0.0",
                        stac_extensions=["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
                        properties={
                            "eo:bands": [{"name": "B03"}],
                        },
                    ),
                },
                {},
                [Band("B02"), Band("B03")],
                [
                    "bands_from_stac_collection: consulting items for band metadata",
                ],
            ),
            (
                # Collection with two items with different bands in different metadata format
                {
                    "collection.json": StacDummyBuilder.collection(
                        stac_version="1.0.0",
                        links=[
                            {"rel": "item", "href": "item10.json"},
                            {"rel": "item", "href": "item11.json"},
                        ],
                    ),
                    "item10.json": StacDummyBuilder.item(
                        stac_version="1.0.0",
                        stac_extensions=["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
                        properties={
                            "eo:bands": [{"name": "B02"}],
                        },
                    ),
                    "item11.json": StacDummyBuilder.item(
                        stac_version="1.1.0",
                        properties={
                            "bands": [{"name": "B03"}],
                        },
                    ),
                },
                {},
                [Band("B02"), Band("B03")],
                [
                    "bands_from_stac_collection: consulting items for band metadata",
                ],
            ),
            (
                # Collection with one item, with band metadata in asset
                {
                    "collection.json": StacDummyBuilder.collection(
                        stac_version="1.0.0",
                        links=[
                            {"rel": "item", "href": "item.json"},
                        ],
                    ),
                    "item.json": StacDummyBuilder.item(
                        stac_version="1.0.0",
                        stac_extensions=["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
                        assets={
                            "asset1": {
                                "href": "https://stac.test/asset.tif",
                                "roles": ["data"],
                                "eo:bands": [{"name": "B02"}],
                            }
                        },
                    ),
                },
                {},
                [Band("B02")],
                [
                    "bands_from_stac_collection: consulting items for band metadata",
                ],
            ),
            (
                # Collection with multiple items and assets, only partially with band metadata
                {
                    "collection.json": StacDummyBuilder.collection(
                        stac_version="1.0.0",
                        links=[
                            {"rel": "item", "href": "item1.json"},
                            {"rel": "item", "href": "item2.json"},
                        ],
                    ),
                    "item1.json": StacDummyBuilder.item(
                        stac_version="1.0.0",
                        stac_extensions=["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
                        assets={
                            "asset11": {
                                "href": "https://stac.test/asset11.tif",
                                "roles": ["data"],
                                "eo:bands": [{"name": "B02"}],
                            },
                            "asset12": {
                                "href": "https://stac.test/asset12.tif",
                                "roles": ["data"],
                            },
                        },
                    ),
                    "item2.json": StacDummyBuilder.item(),
                },
                {},
                [Band("B02")],
                [
                    "bands_from_stac_collection: consulting items for band metadata",
                ],
            ),
        ],
    )
    def test_bands_from_stac_collection_consult_items(
        self, tmp_path, entities, kwargs, expected, caplog, expected_warnings
    ):
        for name, content in entities.items():
            (tmp_path / name).write_text(json.dumps(content))
        collection = pystac.Collection.from_file(tmp_path / "collection.json")
        assert _StacMetadataParser().bands_from_stac_collection(collection=collection) == expected

        assert caplog.messages == expected_warnings

    @pytest.mark.parametrize(
        ["data", "expected"],
        [
            (
                StacDummyBuilder.item(
                    stac_version="1.0.0",
                    stac_extensions=["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
                    properties={
                        "eo:bands": [{"name": "B04"}, {"name": "B03"}],
                    },
                ),
                [Band("B04"), Band("B03")],
            ),
            (
                StacDummyBuilder.item(
                    stac_version="1.0.0",
                    stac_extensions=["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
                    properties={
                        "eo:bands": [
                            {"name": "B04", "common_name": "red", "center_wavelength": 0.665},
                            {"name": "B03", "common_name": "green", "center_wavelength": 0.560},
                        ],
                    },
                ),
                [
                    Band("B04", common_name="red", wavelength_um=0.665),
                    Band("B03", common_name="green", wavelength_um=0.560),
                ],
            ),
            (
                StacDummyBuilder.item(
                    stac_version="1.1.0",
                    properties={
                        "bands": [{"name": "B04"}, {"name": "B03"}],
                    },
                ),
                [Band("B04"), Band("B03")],
            ),
            (
                StacDummyBuilder.item(
                    stac_version="1.1.0",
                    properties={
                        "bands": [
                            {"name": "B04", "eo:common_name": "red", "eo:center_wavelength": 0.665},
                            {"name": "B03", "eo:common_name": "green", "eo:center_wavelength": 0.560},
                        ],
                    },
                ),
                [
                    Band("B04", common_name="red", wavelength_um=0.665),
                    Band("B03", common_name="green", wavelength_um=0.560),
                ],
            ),
        ],
    )
    def test_bands_from_stac_item(self, data, expected):
        item = pystac.Item.from_dict(data)
        assert _StacMetadataParser().bands_from_stac_item(item=item) == expected

    def test_bands_from_stac_item_no_bands(self):
        item = pystac.Item.from_dict(StacDummyBuilder.item())
        assert _StacMetadataParser().bands_from_stac_item(item=item) == []

    @pytest.mark.parametrize(
        ["data", "expected"],
        [
            (
                StacDummyBuilder.item(
                    stac_version="1.0.0",
                    stac_extensions=["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
                    properties={
                        "eo:bands": [{"name": "B04"}, {"name": "B03"}, {"name": "B02"}],
                    },
                ),
                ["B04", "B03", "B02"],
            ),
            (
                StacDummyBuilder.item(
                    stac_version="1.1.0",
                    properties={
                        "bands": [{"name": "B04"}, {"name": "B03"}],
                    },
                ),
                ["B04", "B03"],
            ),
        ],
    )
    def test_bands_from_stac_item_band_names(self, data, expected):
        item = pystac.Item.from_dict(data)
        assert _StacMetadataParser().bands_from_stac_item(item=item).band_names() == expected

    @pytest.mark.parametrize(
        ["entities", "kwargs", "expected"],
        [
            (
                {
                    "item.json": StacDummyBuilder.item(
                        stac_version="1.0.0",
                        links=[{"rel": "collection", "href": "collection.json"}],
                    ),
                    "collection.json": StacDummyBuilder.collection(
                        stac_version="1.0.0",
                        stac_extensions=["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
                        summaries={"eo:bands": [{"name": "B02"}, {"name": "B03"}]},
                    ),
                },
                {},
                [Band("B02"), Band("B03")],
            ),
            (
                {
                    "item.json": StacDummyBuilder.item(
                        stac_version="1.1.0",
                        links=[{"rel": "collection", "href": "collection.json"}],
                    ),
                    "collection.json": StacDummyBuilder.collection(
                        stac_version="1.1.0",
                        summaries={"bands": [{"name": "B02"}, {"name": "B03"}]},
                    ),
                },
                {},
                [Band("B02"), Band("B03")],
            ),
            (
                {
                    "item.json": StacDummyBuilder.item(
                        stac_version="1.1.0",
                        links=[{"rel": "collection", "href": "collection.json"}],
                    ),
                    "collection.json": StacDummyBuilder.collection(
                        stac_version="1.1.0",
                        summaries={"bands": [{"name": "B02"}, {"name": "B03"}]},
                    ),
                },
                {"consult_collection": False},
                [],
            ),
            (
                {
                    "item.json": StacDummyBuilder.item(
                        stac_version="1.1.0",
                        properties={"bands": [{"name": "B03"}]},
                        links=[{"rel": "collection", "href": "collection.json"}],
                    ),
                    "collection.json": StacDummyBuilder.collection(
                        stac_version="1.1.0",
                        summaries={"bands": [{"name": "B02"}, {"name": "B03"}]},
                    ),
                },
                {},
                [Band("B03")],
            ),
        ],
    )
    def test_bands_from_stac_item_consult_collection(self, tmp_path, entities, kwargs, expected):
        for name, content in entities.items():
            (tmp_path / name).write_text(json.dumps(content))
        item = pystac.Item.from_file(tmp_path / "item.json")
        assert _StacMetadataParser().bands_from_stac_item(item=item, **kwargs) == expected

    @pytest.mark.parametrize(
        ["data", "kwargs", "expected"],
        [
            (
                StacDummyBuilder.item(
                    stac_version="1.0.0",
                    stac_extensions=["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
                    properties={
                        "eo:bands": [{"name": "B03"}, {"name": "B04"}],
                    },
                    assets={
                        "asset1": {
                            "href": "https://stac.test/asset.tif",
                            "roles": ["data"],
                        }
                    },
                ),
                {},
                [Band("B03"), Band("B04")],
            ),
            (
                StacDummyBuilder.item(
                    stac_version="1.0.0",
                    stac_extensions=["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
                    properties={
                        "eo:bands": [{"name": "B03"}, {"name": "B04"}],
                    },
                    assets={
                        "asset1": {
                            "href": "https://stac.test/asset.tif",
                            "roles": ["data"],
                            "eo:bands": [{"name": "B02"}],
                        }
                    },
                ),
                {},
                [Band("B03"), Band("B04")],
            ),
            (
                StacDummyBuilder.item(
                    stac_version="1.0.0",
                    stac_extensions=["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
                    assets={
                        "asset1": {
                            "href": "https://stac.test/asset.tif",
                            "roles": ["data"],
                            "eo:bands": [{"name": "B02"}],
                        }
                    },
                ),
                {},
                [Band("B02")],
            ),
            (
                StacDummyBuilder.item(
                    stac_version="1.0.0",
                    stac_extensions=["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
                    assets={
                        "asset1": {
                            "href": "https://stac.test/asset.tif",
                            "roles": ["data"],
                            "eo:bands": [{"name": "B02"}],
                        },
                        "asset2": {
                            "href": "https://stac.test/asset.tif",
                            "roles": ["data"],
                            "eo:bands": [{"name": "B05"}],
                        },
                    },
                ),
                {},
                [Band("B02"), Band("B05")],
            ),
            (
                StacDummyBuilder.item(
                    stac_version="1.0.0",
                    stac_extensions=["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
                    assets={
                        "asset1": {
                            "href": "https://stac.test/asset.tif",
                            "roles": ["data"],
                            "eo:bands": [{"name": "B02"}],
                        }
                    },
                ),
                {"consult_assets": False},
                [],
            ),
            (
                StacDummyBuilder.item(
                    stac_version="1.0.0",
                    stac_extensions=["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
                    assets={
                        "asset1": {
                            "href": "https://stac.test/asset.tif",
                            "roles": ["data"],
                            "bands": [{"name": "B02", "eo:common_name": "red", "eo:center_wavelength": 0.665}],
                        }
                    },
                ),
                {},
                [Band("B02", common_name="red", wavelength_um=0.665)],
            ),
        ],
    )
    def test_bands_from_stac_item_consult_assets(self, data, expected, kwargs):
        item = pystac.Item.from_dict(data)
        assert _StacMetadataParser().bands_from_stac_item(item=item, **kwargs) == expected

    @pytest.mark.parametrize(
        ["data", "expected"],
        [
            (
                {
                    "href": "https://stac.test/asset.tif",
                    "eo:bands": [
                        {"name": "B04"},
                        {"name": "B03"},
                    ],
                },
                ["B04", "B03"],
            ),
            (
                {
                    "href": "https://stac.test/asset.tif",
                    "bands": [
                        {"name": "B04"},
                        {"name": "B03"},
                    ],
                },
                ["B04", "B03"],
            ),
        ],
    )
    def test_bands_from_stac_asset(self, data, expected):
        asset = pystac.Asset.from_dict(data)
        assert _StacMetadataParser().bands_from_stac_asset(asset=asset).band_names() == expected

    @pytest.mark.parametrize(
        ["stac_data", "expected_bands", "expected_warnings"],
        [
            (
                # Legacy use case: STAC 1.0 Collection with "eo" and "item_assets" extensions
                StacDummyBuilder.collection(
                    stac_version="1.0.0",
                    stac_extensions=[
                        "https://stac-extensions.github.io/eo/v1.1.0/schema.json",
                        "https://stac-extensions.github.io/item-assets/v1.0.0/schema.json",
                    ],
                    item_assets={
                        "asset1": {"eo:bands": [{"name": "B03"}, {"name": "B02"}]},
                        "asset2": {"eo:bands": [{"name": "B04"}]},
                    },
                ),
                ["B03", "B02", "B04"],
                ["Deriving band listing from unordered `item_assets`"],
            ),
            (
                # STAC 1.0, with "eo" extension used for band metadata, but not declared
                StacDummyBuilder.collection(
                    stac_version="1.0.0",
                    stac_extensions=[
                        "https://stac-extensions.github.io/item-assets/v1.0.0/schema.json",
                    ],
                    item_assets={
                        "asset1": {"eo:bands": [{"name": "B03"}, {"name": "B02"}]},
                        "asset2": {"eo:bands": [{"name": "B04"}]},
                    },
                ),
                ["B03", "B02", "B04"],
                [
                    "Deriving band listing from unordered `item_assets`",
                    "Using 'eo:bands' metadata, but STAC extension eo was not declared.",
                ],
            ),
            (
                # STAC 1.1 Collection with common/core "bands" metadata and item_assets
                StacDummyBuilder.collection(
                    stac_version="1.1.0",
                    item_assets={
                        "asset1": {"bands": [{"name": "B03"}, {"name": "B02"}]},
                        "asset2": {"bands": [{"name": "B04"}]},
                    },
                ),
                ["B03", "B02", "B04"],
                ["Deriving band listing from unordered `item_assets`"],
            ),
            (
                # STAC 1.1 Collection with "eo" extension based band metadata
                StacDummyBuilder.collection(
                    stac_version="1.1.0",
                    stac_extensions=[
                        "https://stac-extensions.github.io/eo/v1.1.0/schema.json",
                    ],
                    item_assets={
                        "asset1": {"eo:bands": [{"name": "B03"}, {"name": "B02"}]},
                        "asset2": {"eo:bands": [{"name": "B04"}]},
                    },
                ),
                ["B03", "B02", "B04"],
                ["Deriving band listing from unordered `item_assets`"],
            ),
        ],
    )
    def test_bands_from_stac_collection_with_item_assets(
        self, test_data, tmp_path, caplog, stac_data, expected_bands, expected_warnings
    ):
        collection = pystac.Collection.from_dict(stac_data)
        assert _StacMetadataParser().bands_from_stac_collection(collection).band_names() == expected_bands

        if _PYSTAC_1_9_EXTENSION_INTERFACE:
            assert caplog.messages == expected_warnings

    def test_bands_from_stac_collection_with_item_assets_extension_but_no_item_assets(self, caplog):
        """
        item-assets extension is declared, but "item_assets" field is missing
        which is wrong, but we should not crash on that.
        https://github.com/Open-EO/openeo-python-client/issues/853
        """
        stac_data = StacDummyBuilder.collection(
            stac_version="1.0.0",
            stac_extensions=[
                "https://stac-extensions.github.io/eo/v1.1.0/schema.json",
                "https://stac-extensions.github.io/item-assets/v1.0.0/schema.json",
            ],
        )
        collection = pystac.Collection.from_dict(stac_data)
        actual_bands = _StacMetadataParser().bands_from_stac_collection(collection).band_names()
        assert actual_bands == []
        assert "bands_from_stac_collection: no band name source found" in caplog.messages

    @pytest.mark.parametrize(
        ["path", "expected"],
        [
            (
                "stac/collections/terrascope-sentinel-2-l2a.json",
                ["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B09", "B11", "B12"],
            ),
            (
                "stac/collections/cdse-sentinel-2-l2a.json",
                ["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B09", "B11", "B12"],
            ),
        ],
    )
    def test_bands_from_collection_examples(self, test_data, path, expected):
        data = test_data.load_json(path)
        collection = pystac.Collection.from_dict(data)
        bands = _StacMetadataParser().bands_from_stac_collection(collection)
        assert bands.band_names() == expected
