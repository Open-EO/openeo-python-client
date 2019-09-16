from openeo.imagecollection import CollectionMetadata


def test_metadata_get():
    metadata = CollectionMetadata({
        "foo": "bar",
        "very": {
            "deeply": {"nested": {"path": {"to": "somewhere"}}}
        }
    })
    assert metadata.get("foo") == "bar"
    assert metadata.get("very", "deeply", "nested", "path", "to") == "somewhere"
    assert metadata.get("invalid", "key") is None
    assert metadata.get("invalid", "key", default="nope") == "nope"


def test_metadata_extent():
    metadata = CollectionMetadata({
        "extent": {"spatial": {"xmin": 4, "xmax": 10}}
    })
    assert metadata.extent == {"spatial": {"xmin": 4, "xmax": 10}}


def test_metadata_bands_eo_bands():
    metadata = CollectionMetadata({
        "properties": {"eo:bands": [
            {"name": "foo", "common_name": "F00", "center_wavelength": 0.543},
            {"name": "bar"}
        ]}
    })
    assert metadata.bands == [
        CollectionMetadata.Band("foo", "F00", 0.543),
        CollectionMetadata.Band("bar", None, None)
    ]
    assert metadata.band_names == ["foo", "bar"]
    assert metadata.band_common_names == ["F00", None]


def test_metadata_bands_cube_dimensions():
    metadata = CollectionMetadata({
        "properties": {"cube:dimensions": {
            "x": {"type": "spatial", "axis": "x"},
            "b": {"type": "bands", "values": ["foo", "bar"]}
        }}
    })
    assert metadata.bands == [
        CollectionMetadata.Band("foo", None, None),
        CollectionMetadata.Band("bar", None, None)
    ]
    assert metadata.band_names == ["foo", "bar"]


def test_metadata_bands_vito_bands():
    # TODO: deprecated?
    metadata = CollectionMetadata({
        "bands": [
            {"band_id": "2", "name": "blue", "wavelength_nm": 496, "offset": 0, "type": "int16", "unit": "1"},
            {"band_id": "3", "name": "green", "wavelength_nm": 560, "offset": 0, "type": "int16", "unit": "1"},
        ]
    })
    assert metadata.bands == [
        CollectionMetadata.Band("2", "blue", 0.496),
        CollectionMetadata.Band("3", "green", 0.560),
    ]
