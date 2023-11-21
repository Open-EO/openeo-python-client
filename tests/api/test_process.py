import pytest

from openeo.api.process import Parameter


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
