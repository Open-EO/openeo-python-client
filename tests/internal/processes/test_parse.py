import pytest

from openeo.internal.processes.parse import (
    _NO_DEFAULT,
    Parameter,
    Process,
    Returns,
    Schema,
    parse_remote_process_definition,
)


def test_schema():
    s = Schema.from_dict({"type": "number"})
    assert s.schema == {"type": "number"}


def test_schema_equality():
    assert Schema({"type": "number"}) == Schema({"type": "number"})
    assert Schema({"type": "number"}) == Schema.from_dict({"type": "number"})

    assert Schema({"type": "number"}) != Schema({"type": "string"})


@pytest.mark.parametrize(
    ["schema", "expected"],
    [
        ({"type": "object", "subtype": "geojson"}, True),
        ({"type": "object"}, False),
        ({"subtype": "geojson"}, False),
        ({"type": "object", "subtype": "vectorzz"}, False),
    ],
)
def test_schema_accepts_geojson(schema, expected):
    assert Schema(schema).accepts_geojson() == expected
    assert Schema([{"type": "number"}, schema]).accepts_geojson() == expected


@pytest.mark.parametrize(
    ("schema", "expected"),
    [
        (
            Schema(
                {
                    "type": "string",
                    "enum": [
                        "average",
                        "bilinear",
                        "cubic",
                        "cubicspline",
                        "lanczos",
                        "max",
                        "med",
                        "min",
                        "mode",
                        "near",
                    ],
                },
            ),
            ["average", "bilinear", "cubic", "cubicspline", "lanczos", "max", "med", "min", "mode", "near"],
        ),
        (
            Schema([{"type": "string", "enum": ["replicate", "reflect", "reflect_pixel", "wrap"]}, {"type": "null"}]),
            ["replicate", "reflect", "reflect_pixel", "wrap", None],
        ),
        (
            Schema(
                [
                    {"type": "string", "enum": ["replicate", "reflect"]},
                    {"type": "null"},
                    {"type": "number", "enum": ["reflect_pixel", "wrap"]},
                ]
            ),
            ["replicate", "reflect", None, "reflect_pixel", "wrap"],
        ),
    ],
)
def test_get_enum_options(schema, expected):
    assert schema.get_enum_options() == expected


def test_parameter():
    p = Parameter.from_dict({
        "name": "foo",
        "description": "Foo amount",
        "schema": {"type": "number"},
    })
    assert p.name == "foo"
    assert p.description == "Foo amount"
    assert p.schema.schema == {"type": "number"}
    assert p.default is _NO_DEFAULT
    assert p.optional is False


def test_parameter_default():
    p = Parameter.from_dict({
        "name": "foo",
        "description": "Foo amount",
        "schema": {"type": "number"},
        "default": 5
    })
    assert p.default == 5


def test_parameter_default_none():
    p = Parameter.from_dict({
        "name": "foo",
        "description": "Foo amount",
        "schema": {"type": "number"},
        "default": None
    })
    assert p.default is None


def test_parameter_equality():
    p1 = Parameter.from_dict({"name": "foo", "description": "Foo", "schema": {"type": "number"}})
    p2 = Parameter.from_dict({"name": "foo", "description": "Foo", "schema": {"type": "number"}})
    p3 = Parameter.from_dict({"name": "foo", "description": "Foo", "schema": {"type": "string"}})
    assert p1 == p2
    assert p1 != p3


def test_returns():
    r = Returns.from_dict({
        "description": "Roo",
        "schema": {"type": "number"}
    })
    assert r.schema.schema == {"type": "number"}
    assert r.description == "Roo"


def test_process():
    p = Process.from_dict({
        "id": "absolute",
        "summary": "Absolute value",
        "description": "Computes the absolute value of a real number.",
        "categories": ["math"],
        "parameters": [
            {"name": "x", "description": "A number.", "schema": {"type": ["number", "null"]}},
        ],
        "returns": {
            "description": "The computed absolute value.",
            "schema": {"type": ["number", "null"], "minimum": 0}
        },
        "links": [{"rel": "about", "href": "http://example.com/abs.html"}],
    })

    assert p.id == "absolute"
    assert p.description == "Computes the absolute value of a real number."
    assert p.summary == "Absolute value"
    assert len(p.parameters) == 1
    assert p.parameters[0].name == "x"
    assert p.parameters[0].description == "A number."
    assert p.parameters[0].schema.schema == {"type": ["number", "null"]}
    assert p.returns.description == "The computed absolute value."
    assert p.returns.schema.schema == {"type": ["number", "null"], "minimum": 0}


def test_process_from_json():
    p = Process.from_json('''{
        "id": "absolute",
        "summary": "Absolute value",
        "description": "Computes the absolute value of a real number.",
        "categories": ["math"],
        "parameters": [
            {"name": "x", "description": "A number.", "schema": {"type": ["number", "null"]}}
        ],
        "returns": {
            "description": "The computed absolute value.",
            "schema": {"type": ["number", "null"], "minimum": 0}
        }
    }''')
    assert p.id == "absolute"
    assert p.description == "Computes the absolute value of a real number."
    assert p.summary == "Absolute value"
    assert len(p.parameters) == 1
    assert p.parameters[0].name == "x"
    assert p.parameters[0].description == "A number."
    assert p.parameters[0].schema.schema == {"type": ["number", "null"]}
    assert p.returns.description == "The computed absolute value."
    assert p.returns.schema.schema == {"type": ["number", "null"], "minimum": 0}


@pytest.mark.parametrize(
    ("key", "expected"), [
        ("x", Parameter.from_dict({"name": "x", "description": "A number.", "schema": {"type": ["number", "null"]}})),
        ("y", Parameter.from_dict({"name": "y", "description": "A number.", "schema": {"type": ["number", "null"]}})),
    ]
)
def test_get_parameter(key, expected):
    p = Process.from_dict({
        "id": "absolute",
        "summary": "Absolute value",
        "description": "Computes the absolute value of a real number.",
        "categories": ["math"],
        "parameters": [
            {"name": "x", "description": "A number.", "schema": {"type": ["number", "null"]}},
            {"name": "y", "description": "A number.", "schema": {"type": ["number", "null"]}},
        ],
        "returns": {
            "description": "The computed absolute value.",
            "schema": {"type": ["number", "null"], "minimum": 0}
        },
        "links": [{"rel": "about", "href": "http://example.com/abs.html"}],
    })
    assert p.get_parameter(key) == expected


def test_get_parameter_error():
    p = Process.from_dict({
        "id": "absolute",
        "summary": "Absolute value",
        "description": "Computes the absolute value of a real number.",
        "categories": ["math"],
        "parameters": [
            {"name": "x", "description": "A number.", "schema": {"type": ["number", "null"]}},
            {"name": "y", "description": "A number.", "schema": {"type": ["number", "null"]}},
        ],
        "returns": {
            "description": "The computed absolute value.",
            "schema": {"type": ["number", "null"], "minimum": 0}
        },
        "links": [{"rel": "about", "href": "http://example.com/abs.html"}],
    })
    with pytest.raises(LookupError):
        p.get_parameter("z")


def test_parse_remote_process_definition_minimal(requests_mock):
    url = "https://example.com/ndvi.json"
    requests_mock.get(url, json={"id": "ndvi"})
    process = parse_remote_process_definition(url)
    assert process.id == "ndvi"
    assert process.parameters is None
    assert process.returns is None
    assert process.description is None
    assert process.summary is None


def test_parse_remote_process_definition_parameters(requests_mock):
    url = "https://example.com/ndvi.json"
    requests_mock.get(
        url,
        json={
            "id": "ndvi",
            "parameters": [
                {"name": "incr", "description": "Increment", "schema": {"type": "number"}},
                {"name": "scales", "description": "Scales", "default": [1, 1], "schema": {"type": "number"}},
            ],
        },
    )
    process = parse_remote_process_definition(url)
    assert process.id == "ndvi"
    assert process.parameters == [
        Parameter(name="incr", description="Increment", schema=Schema({"type": "number"})),
        Parameter(name="scales", description="Scales", default=[1, 1], schema=Schema({"type": "number"})),
    ]
    assert process.returns is None
    assert process.description is None
    assert process.summary is None


def test_parse_remote_process_definition_listing(requests_mock):
    url = "https://example.com/processes.json"
    requests_mock.get(
        url,
        json={
            "processes": [
                {
                    "id": "ndvi",
                    "parameters": [{"name": "incr", "description": "Incr", "schema": {"type": "number"}}],
                },
                {
                    "id": "scale",
                    "parameters": [
                        {"name": "factor", "description": "Factor", "default": 1, "schema": {"type": "number"}}
                    ],
                },
            ],
            "links": [],
        },
    )

    # No process id given
    with pytest.raises(ValueError, match="Working with process listing, but got invalid process id None"):
        parse_remote_process_definition(url)

    # Process id not found
    with pytest.raises(LookupError, match="Process 'mehblargh' not found in process listing"):
        parse_remote_process_definition(url, process_id="mehblargh")

    # Valid proces id
    process = parse_remote_process_definition(url, process_id="ndvi")
    assert process.id == "ndvi"
    assert process.parameters == [
        Parameter(name="incr", description="Incr", schema=Schema({"type": "number"})),
    ]
    assert process.returns is None
    assert process.description is None
    assert process.summary is None

    # Another proces id
    process = parse_remote_process_definition(url, process_id="scale")
    assert process.id == "scale"
    assert process.parameters == [
        Parameter(name="factor", description="Factor", default=1, schema=Schema({"type": "number"})),
    ]
    assert process.returns is None
    assert process.description is None
    assert process.summary is None


def test_parse_remote_process_definition_inconsistency(requests_mock):
    url = "https://example.com/ndvi.json"
    requests_mock.get(url, json={"id": "nnddvvii"})
    with pytest.raises(LookupError, match="Expected process id 'ndvi', but found 'nnddvvii'"):
        _ = parse_remote_process_definition(url, process_id="ndvi")
