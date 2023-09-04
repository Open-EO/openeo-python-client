from openeo.internal.processes.parse import Parameter, Process, Returns, Schema


def test_schema():
    s = Schema.from_dict({"type": "number"})
    assert s.schema == {"type": "number"}


def test_parameter():
    p = Parameter.from_dict({
        "name": "foo",
        "description": "Foo amount",
        "schema": {"type": "number"},
    })
    assert p.name == "foo"
    assert p.description == "Foo amount"
    assert p.schema.schema == {"type": "number"}
    assert p.default is Parameter.NO_DEFAULT
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
