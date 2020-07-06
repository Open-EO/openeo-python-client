from openeo.api.process import Parameter


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


def test_parameter_raster_cube():
    p = Parameter.raster_cube(name="x")
    assert p.to_dict() == {"name": "x", "description": "A data cube.",
                           "schema": {"type": "object", "subtype": "raster-cube"}}
