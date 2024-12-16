import pytest

from openeo.api.process import Parameter, schema_supports


def test_parameter_defaults():
    p = Parameter(name="x")
    assert p.to_dict() == {"name": "x", "description": "x", "schema": {}}


def test_parameter_simple():
    p = Parameter(name="x", description="the x value.", schema={"type": "number"})
    assert p.to_dict() == {"name": "x", "description": "the x value.", "schema": {"type": "number"}}


def test_parameter_schema_str():
    p = Parameter(name="x", description="the x value.", schema="number")
    assert p.to_dict() == {"name": "x", "description": "the x value.", "schema": {"type": "number"}}


def test_parameter_schema_default():
    p = Parameter(name="x", description="the x value.", schema="number", default=42)
    assert p.to_dict() == {
        "name": "x", "description": "the x value.", "schema": {"type": "number"},
        "optional": True, "default": 42
    }


def test_parameter_schema_default_none():
    p = Parameter(name="x", description="the x value.", schema="number", default=None)
    assert p.to_dict() == {
        "name": "x", "description": "the x value.", "schema": {"type": "number"},
        "optional": True, "default": None
    }


def test_parameter_schema_optional():
    p = Parameter(name="x", description="the x value.", schema="number", optional=True)
    assert p.to_dict() == {
        "name": "x", "description": "the x value.", "schema": {"type": "number"},
        "optional": True,
    }


def test_parameter_raster_cube():
    p = Parameter.raster_cube(name="x")
    assert p.to_dict() == {
        "name": "x", "description": "A data cube.", "schema": {"type": "object", "subtype": "raster-cube"}
    }


def test_parameter_string():
    assert Parameter.string("color").to_dict() == {
        "name": "color", "description": "color", "schema": {"type": "string"}
    }
    assert Parameter.string("color", description="The color.").to_dict() == {
        "name": "color", "description": "The color.", "schema": {"type": "string"}
    }
    assert Parameter.string("color", default="red").to_dict() == {
        "name": "color", "description": "color", "schema": {"type": "string"}, "optional": True, "default": "red"
    }


def test_parameter_string_subtype():
    assert Parameter.string("cid", subtype="collection-id").to_dict() == {
        "name": "cid",
        "description": "cid",
        "schema": {"type": "string", "subtype": "collection-id"},
    }


def test_parameter_string_format():
    assert Parameter.string("date", subtype="date", format="date").to_dict() == {
        "name": "date",
        "description": "date",
        "schema": {"type": "string", "subtype": "date", "format": "date"},
    }


def test_parameter_integer():
    assert Parameter.integer("iterations").to_dict() == {
        "name": "iterations", "description": "iterations", "schema": {"type": "integer"}
    }
    assert Parameter.integer("iterations", description="Iterations.").to_dict() == {
        "name": "iterations", "description": "Iterations.", "schema": {"type": "integer"}
    }
    assert Parameter.integer("iterations", default=5).to_dict() == {
        "name": "iterations", "description": "iterations", "schema": {"type": "integer"}, "optional": True, "default": 5
    }


def test_parameter_number():
    assert Parameter.number("iterations").to_dict() == {
        "name": "iterations", "description": "iterations", "schema": {"type": "number"}
    }
    assert Parameter.number("iterations", description="Iterations.").to_dict() == {
        "name": "iterations", "description": "Iterations.", "schema": {"type": "number"}
    }
    assert Parameter.number("iterations", default=12.34).to_dict() == {
        "name": "iterations", "description": "iterations", "schema": {"type": "number"}, "optional": True,
        "default": 12.34
    }


def test_parameter_array():
    assert Parameter.array("bands").to_dict() == {
        "name": "bands", "description": "bands", "schema": {"type": "array"}
    }
    assert Parameter.array("bands", description="Bands.").to_dict() == {
        "name": "bands", "description": "Bands.", "schema": {"type": "array"}
    }
    assert Parameter.array("bands", default=["red", "green", "blue"]).to_dict() == {
        "name": "bands", "description": "bands", "schema": {"type": "array"}, "optional": True,
        "default": ["red", "green", "blue"]
    }
    assert Parameter.array("bands", item_schema="string").to_dict() == {
        "name": "bands",
        "description": "bands",
        "schema": {"type": "array", "items": {"type": "string"}},
    }
    assert Parameter.array("bands", item_schema={"type": "string"}).to_dict() == {
        "name": "bands",
        "description": "bands",
        "schema": {"type": "array", "items": {"type": "string"}},
    }


def test_parameter_object():
    assert Parameter.object("bbox").to_dict() == {"name": "bbox", "description": "bbox", "schema": {"type": "object"}}
    assert Parameter.object("bbox", description="Spatial").to_dict() == {
        "name": "bbox",
        "description": "Spatial",
        "schema": {"type": "object"},
    }
    assert Parameter.object("bbox", default={"west": 4}).to_dict() == {
        "name": "bbox",
        "description": "bbox",
        "schema": {"type": "object"},
        "optional": True,
        "default": {"west": 4},
    }
    assert Parameter.object("cube", subtype="datacube").to_dict() == {
        "name": "cube",
        "description": "cube",
        "schema": {"type": "object", "subtype": "datacube"},
    }


@pytest.mark.parametrize(["kwargs", "expected"], [
    ({"name": "x"}, {"name": "x", "description": "x", "schema": {}}),
    (
            {"name": "x", "schema": {"type": "number"}},
            {"name": "x", "description": "x", "schema": {"type": "number"}}),
    (
            {"name": "x", "description": "X value.", "schema": "number"},
            {"name": "x", "description": "X value.", "schema": {"type": "number"}}
    ),
    (
            {"name": "x", "description": "X value.", "schema": "number", "default": 42},
            {"name": "x", "description": "X value.", "schema": {"type": "number"}, "default": 42, "optional": True}
    ),
    (
            {"name": "x", "description": "X value.", "schema": "number", "default": None},
            {"name": "x", "description": "X value.", "schema": {"type": "number"}, "default": None, "optional": True}
    ),
    (
            {"name": "x", "description": "X value.", "schema": "number", "optional": True},
            {"name": "x", "description": "X value.", "schema": {"type": "number"}, "optional": True}
    ),
])
def test_parameter_reencode(kwargs, expected):
    p = Parameter(**kwargs)
    d = p.to_dict()
    assert d == expected
    q = Parameter(**d)
    assert q.to_dict() == expected


def test_parameter_spatial_extent():
    assert Parameter.spatial_extent().to_dict() == {
        "description": "Limits the data to process to the specified bounding box or polygons.\n"
        "\n"
        "For raster data, the process loads the pixel into the data cube if the point\n"
        "at the pixel center intersects with the bounding box or any of the polygons\n"
        "(as defined in the Simple Features standard by the OGC).\n"
        "\n"
        "For vector data, the process loads the geometry into the data cube if the geometry\n"
        "is fully within the bounding box or any of the polygons (as defined in the\n"
        "Simple Features standard by the OGC). Empty geometries may only be in the\n"
        "data cube if no spatial extent has been provided.\n"
        "\n"
        "Empty geometries are ignored.\n"
        "\n"
        "Set this parameter to null to set no limit for the spatial extent.",
        "name": "spatial_extent",
        "schema": [
            {
                "properties": {
                    "base": {
                        "default": None,
                        "description": "Base (optional, lower " "left corner, coordinate " "axis 3).",
                        "type": ["number", "null"],
                    },
                    "crs": {
                        "anyOf": [
                            {
                                "examples": [3857],
                                "minimum": 1000,
                                "subtype": "epsg-code",
                                "title": "EPSG Code",
                                "type": "integer",
                            },
                            {"subtype": "wkt2-definition", "title": "WKT2", "type": "string"},
                        ],
                        "default": 4326,
                        "description": "Coordinate reference "
                        "system of the extent, "
                        "specified as as [EPSG "
                        "code](http://www.epsg-registry.org/) "
                        "or [WKT2 CRS "
                        "string](http://docs.opengeospatial.org/is/18-010r7/18-010r7.html). "
                        "Defaults to `4326` (EPSG "
                        "code 4326) unless the "
                        "client explicitly requests "
                        "a different coordinate "
                        "reference system.",
                    },
                    "east": {"description": "East (upper right corner, " "coordinate axis 1).", "type": "number"},
                    "height": {
                        "default": None,
                        "description": "Height (optional, upper " "right corner, " "coordinate axis 3).",
                        "type": ["number", "null"],
                    },
                    "north": {"description": "North (upper right " "corner, coordinate axis " "2).", "type": "number"},
                    "south": {"description": "South (lower left " "corner, coordinate axis " "2).", "type": "number"},
                    "west": {"description": "West (lower left corner, " "coordinate axis 1).", "type": "number"},
                },
                "required": ["west", "south", "east", "north"],
                "subtype": "bounding-box",
                "title": "Bounding Box",
                "type": "object",
            },
            {
                "description": "Limits the data cube to the bounding box of the "
                "given geometries in the vector data cube. For "
                "raster data, all pixels inside the bounding box "
                "that do not intersect with any of the polygons "
                "will be set to no data (`null`). Empty geometries "
                "are ignored.",
                "dimensions": [{"type": "geometry"}],
                "subtype": "datacube",
                "title": "Vector data cube",
                "type": "object",
            },
            {
                "description": "Don't filter spatially. All data is included in " "the data cube.",
                "title": "No filter",
                "type": "null",
            },
        ],
    }


def test_schema_supports_type_basic():
    schema = {"type": "string"}
    assert schema_supports(schema, type="string") is True
    assert schema_supports(schema, type="number") is False


def test_schema_supports_type_list():
    schema = {"type": ["string", "number"]}
    assert schema_supports(schema, type="string") is True
    assert schema_supports(schema, type="number") is True
    assert schema_supports(schema, type="object") is False


def test_schema_supports_subtype():
    schema = {"type": "object", "subtype": "datacube"}
    assert schema_supports(schema, type="object") is True
    assert schema_supports(schema, type="object", subtype="datacube") is True
    assert schema_supports(schema, type="object", subtype="geojson") is False


def test_schema_supports_list():
    schema = [
        {"type": "string"},
        {"type": "object", "subtype": "datacube"},
    ]
    assert schema_supports(schema, type="string") is True
    assert schema_supports(schema, type="number") is False
    assert schema_supports(schema, type="object") is True
    assert schema_supports(schema, type="object", subtype="datacube") is True
    assert schema_supports(schema, type="object", subtype="geojson") is False
