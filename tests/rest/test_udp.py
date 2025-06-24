import re
import warnings

import dirty_equals
import pytest

import openeo
from openeo.api.process import Parameter
from openeo.rest._testing import build_capabilities
from openeo.rest.udp import RESTUserDefinedProcess, build_process_dict

API_URL = "https://oeo.test"


@pytest.fixture
def con100(requests_mock):
    requests_mock.get(API_URL + "/", json=build_capabilities(udp=True, validation=True))
    con = openeo.connect(API_URL)
    return con


def test_describe(con100, requests_mock, test_data):
    expected_details = test_data.load_json("1.0.0/udp_details.json")
    requests_mock.get(API_URL + "/process_graphs/evi", json=expected_details)

    udp = con100.user_defined_process(user_defined_process_id='evi')
    details = udp.describe()

    assert details == expected_details


def test_repr_html(con100, requests_mock):
    requests_mock.get(API_URL + "/process_graphs/add1", json={"id": "add1"})
    udp = con100.user_defined_process("add1")
    assert udp._repr_html_() == dirty_equals.IsStr(
        regex=r'.*<openeo-process>.*"process":\s*{"id":\s*"add1".*', regex_flags=re.DOTALL
    )


def test_store_simple(con100, requests_mock):
    requests_mock.get(API_URL + "/processes", json={"processes": [{"id": "add"}]})

    two = {
        "add": {
            "process_id": "add",
            "arguments": {"x": 1, "y": 1, },
            "result": True,
        }
    }

    def check_body(request):
        body = request.json()
        assert body == {
            "process_graph": two,
            "public": False,
        }
        return True

    adapter = requests_mock.put(API_URL + "/process_graphs/two", additional_matcher=check_body)

    udp = con100.save_user_defined_process("two", two)
    assert isinstance(udp, RESTUserDefinedProcess)
    assert adapter.called


def test_store_full(con100, requests_mock):
    requests_mock.get(API_URL + "/processes", json={"processes": [{"id": "add"}]})

    two = {
        "add": {
            "process_id": "add",
            "arguments": {"x": 1, "y": 1, },
            "result": True,
        }
    }

    def check_body(request):
        body = request.json()
        assert body == {
            "process_graph": two,
            "public": False,
            "summary": "A summary",
            "description": "A description",
            "returns": {"schema": {"type": ["number", "null"]}},
            "categories": ["math", "simple"],
            "examples": [{"arguments": {"x": 5, "y": 2.5}, "returns": 7.5}, ],
            "links": [{"link1": "openeo.cloud", "link2": "openeo.vito.be"}],
        }
        return True

    adapter = requests_mock.put(API_URL + "/process_graphs/two", additional_matcher=check_body)

    udp = con100.save_user_defined_process(
        "two", two,
        summary="A summary",
        description="A description",
        returns={"schema": {"type": ["number", "null"]}},
        categories=["math", "simple"],
        examples=[{"arguments": {"x": 5, "y": 2.5}, "returns": 7.5}, ],
        links=[{"link1": "openeo.cloud", "link2": "openeo.vito.be"}],
    )
    assert isinstance(udp, RESTUserDefinedProcess)
    assert adapter.called


def test_store_collision(con100, requests_mock):
    subtract = {
        "minusy": {
            "process_id": "multiply",
            "arguments": {"x": {"from_parameter": "y"}, "y": -1}
        },
        "add": {
            "process_id": "add",
            "arguments": {"x": {"from_parameter": "x"}, "y": {"from_node": "minusy"}, },
            "result": True,
        }
    }

    def check_body(request):
        body = request.json()
        assert body == {
            "process_graph": subtract,
            "parameters": [
                {"name": "x", "description": "x", "schema": {"type": "number"}},
                {"name": "y", "description": "y", "schema": {"type": "number"}}
            ],
            "public": False,
        }
        return True

    requests_mock.get(API_URL + "/processes", json={"processes": [{"id": "add"}, {"id": "subtract"}]})
    adapter1 = requests_mock.put(API_URL + "/process_graphs/my_subtract", additional_matcher=check_body)
    adapter2 = requests_mock.put(API_URL + "/process_graphs/subtract", additional_matcher=check_body)

    parameters = [Parameter.number("x"), Parameter.number("y")]
    with warnings.catch_warnings():
        # Turn warnings into exceptions
        warnings.simplefilter("error")
        udp = con100.save_user_defined_process("my_subtract", subtract, parameters=parameters)
    assert isinstance(udp, RESTUserDefinedProcess)
    assert adapter1.call_count == 1

    with pytest.warns(UserWarning, match="same id as a pre-defined process") as recorder:
        udp = con100.save_user_defined_process("subtract", subtract, parameters=parameters)
    assert isinstance(udp, RESTUserDefinedProcess)
    assert adapter2.call_count == 1
    assert len(recorder) == 1


@pytest.mark.parametrize(["parameters", "expected_parameters"], [
    (
            [Parameter(name="data", description="A data cube.", schema="number")],
            {"parameters": [{"name": "data", "description": "A data cube.", "schema": {"type": "number"}}]}
    ),
    (
            [Parameter(name="data", description="A cube.", schema="number", default=42)],
            {"parameters": [
                {"name": "data", "description": "A cube.", "schema": {"type": "number"}, "optional": True,
                 "default": 42}
            ]}
    ),
    (
            [{"name": "data", "description": "A data cube.", "schema": {"type": "number"}}],
            {"parameters": [{"name": "data", "description": "A data cube.", "schema": {"type": "number"}}]}
    ),
    (
            [{"name": "data", "description": "A cube.", "schema": {"type": "number"}, "default": 42}],
            {"parameters": [
                {"name": "data", "description": "A cube.", "schema": {"type": "number"}, "optional": True,
                 "default": 42}
            ]}
    ),
    ([], {"parameters": []}),
    (None, {}),
])
def test_store_with_parameter(con100, requests_mock, parameters, expected_parameters):
    requests_mock.get(API_URL + "/processes", json={"processes": [{"id": "add"}]})

    increment = {
        "add1": {
            "process_id": "add",
            "arguments": {
                "x": {"from_parameter": "data"},
                "y": 1,
            },
            "result": True,
        }
    }

    def check_body(request):
        body = request.json()
        assert body == {
            "process_graph": increment,
            "public": False,
            **expected_parameters,
        }
        return True

    adapter = requests_mock.put(API_URL + "/process_graphs/increment", additional_matcher=check_body)

    con100.save_user_defined_process(
        "increment", increment, parameters=parameters
    )

    assert adapter.called


def test_store_with_validation(con100, requests_mock, caplog):
    requests_mock.get(API_URL + "/processes", json={"processes": [{"id": "add"}]})
    validation_mock = requests_mock.post(
        API_URL + "/validation", json={"errors": [{"code": "TooComplex", "message": "Nope"}]}
    )

    two = {
        "add": {
            "process_id": "add",
            "arguments": {
                "x": 1,
                "y": 1,
            },
            "result": True,
        }
    }

    def check_body(request):
        body = request.json()
        assert body == {
            "process_graph": two,
            "public": False,
        }
        return True

    udp_mock = requests_mock.put(API_URL + "/process_graphs/two", additional_matcher=check_body)

    udp = con100.save_user_defined_process("two", two)
    assert isinstance(udp, RESTUserDefinedProcess)

    assert udp_mock.called
    assert validation_mock.called
    assert [(r.levelname, r.getMessage()) for r in caplog.records] == [
        ("WARNING", "Preflight process graph validation raised: [TooComplex] Nope")
    ]


def test_update(con100, requests_mock, test_data):
    updated_udp = test_data.load_json("1.0.0/udp_details.json")

    def check_body(request):
        body = request.json()
        assert body['process_graph'] == updated_udp['process_graph']
        assert body['parameters'] == updated_udp['parameters']
        assert body['public'] is False
        return True

    adapter = requests_mock.put(API_URL + "/process_graphs/evi", additional_matcher=check_body)

    udp = con100.user_defined_process(user_defined_process_id='evi')

    udp.update(process_graph=updated_udp['process_graph'], parameters=updated_udp['parameters'])

    assert adapter.called


def test_make_public(con100, requests_mock, test_data):
    udp = con100.user_defined_process(user_defined_process_id='evi')

    def check_body(request):
        body = request.json()
        assert body['process_graph'] == updated_udp['process_graph']
        assert body['parameters'] == updated_udp['parameters']
        assert body['public'] is True
        return True

    adapter = requests_mock.put(API_URL + "/process_graphs/evi", additional_matcher=check_body)

    updated_udp = test_data.load_json("1.0.0/udp_details.json")

    udp.update(process_graph=updated_udp['process_graph'], parameters=updated_udp['parameters'], public=True)

    assert adapter.called


def test_delete(con100, requests_mock):
    adapter = requests_mock.delete(API_URL + "/process_graphs/evi", status_code=204)

    udp = con100.user_defined_process(user_defined_process_id='evi')
    udp.delete()

    assert adapter.called


@pytest.mark.parametrize(
    ["layer", "dates", "bbox"],
    [
        (
            Parameter.string("layer", description="Collection Id"),
            Parameter.string("dates", description="Temporal extent"),
            Parameter("bbox", schema="object", description="bbox"),
        ),
        (
            Parameter.string("layer", description="Collection Id"),
            Parameter.temporal_interval(name="dates"),
            Parameter.spatial_extent(name="bbox"),
        ),
        (
            Parameter.string("layer", description="Collection Id"),
            Parameter.temporal_interval(name="dates"),
            Parameter.bounding_box(name="bbox"),
        ),
    ],
)
def test_build_parameterized_cube_filters(con100, layer, dates, bbox, recwarn):
    cube = con100.load_collection(layer).filter_temporal(dates).filter_bbox(bbox)
    assert cube.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": {"from_parameter": "layer"}, "temporal_extent": None, "spatial_extent": None},
        },
        "filtertemporal1": {
            "process_id": "filter_temporal",
            "arguments": {"data": {"from_node": "loadcollection1"}, "extent": {"from_parameter": "dates"}},
        },
        "filterbbox1": {
            "process_id": "filter_bbox",
            "arguments": {"data": {"from_node": "filtertemporal1"}, "extent": {"from_parameter": "bbox"}},
            "result": True,
        }
    }
    assert recwarn.list == []


def test_build_parameterized_cube_single_date(con100):
    layer = Parameter.string("layer")
    date = Parameter.string("date")
    bbox = Parameter("bbox", schema="object")
    cube = con100.load_collection(layer).filter_temporal(date, date).filter_bbox(bbox)

    assert cube.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": {"from_parameter": "layer"}, "temporal_extent": None, "spatial_extent": None},
        },
        "filtertemporal1": {
            "process_id": "filter_temporal",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "extent": [{"from_parameter": "date"}, {"from_parameter": "date"}]
            },
        },
        "filterbbox1": {
            "process_id": "filter_bbox",
            "arguments": {"data": {"from_node": "filtertemporal1"}, "extent": {"from_parameter": "bbox"}},
            "result": True,
        }
    }


def test_build_parameterized_cube_start_date(con100, recwarn):
    layer = Parameter.string("layer", description="Collection id")
    start = Parameter.string("start", description="Start date")
    bbox = Parameter("bbox", schema="object", description="Bbox")
    cube = con100.load_collection(layer).filter_temporal(start, None).filter_bbox(bbox)

    assert cube.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": {"from_parameter": "layer"}, "temporal_extent": None, "spatial_extent": None},
        },
        "filtertemporal1": {
            "process_id": "filter_temporal",
            "arguments": {"data": {"from_node": "loadcollection1"}, "extent": [{"from_parameter": "start"}, None]},
        },
        "filterbbox1": {
            "process_id": "filter_bbox",
            "arguments": {"data": {"from_node": "filtertemporal1"}, "extent": {"from_parameter": "bbox"}},
            "result": True,
        }
    }
    assert recwarn.list == []


@pytest.mark.parametrize(
    ["layer", "dates", "bbox"],
    [
        (
            Parameter.string("layer", description="Collection Id"),
            Parameter.string("dates", description="Temporal extent"),
            Parameter("bbox", schema="object", description="bbox"),
        ),
        (
            Parameter.string("layer", description="Collection Id"),
            Parameter.temporal_interval(name="dates"),
            Parameter.spatial_extent(name="bbox"),
        ),
        (
            Parameter.string("layer", description="Collection Id"),
            Parameter.temporal_interval(name="dates"),
            Parameter.bounding_box(name="bbox"),
        ),
    ],
)
def test_build_parameterized_cube_load_collection(con100, recwarn, layer, dates, bbox):
    cube = con100.load_collection(layer, spatial_extent=bbox, temporal_extent=dates)
    assert cube.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {
                "id": {"from_parameter": "layer"},
                "temporal_extent": {"from_parameter": "dates"},
                "spatial_extent": {"from_parameter": "bbox"}
            },
            "result": True,
        }
    }
    assert recwarn.list == []


def test_build_parameterized_cube_load_collection_invalid_bbox_schema(con100):
    layer = Parameter.string("layer", description="Collection id")
    dates = Parameter.string("dates", description="Temporal extent")
    bbox = Parameter.string("bbox", description="Spatial extent")
    with pytest.warns(
        UserWarning,
        match="Schema mismatch with parameter given to `spatial_extent` in `load_collection`: expected a schema compatible with type 'object' but got {'type': 'string'}.",
    ):
        cube = con100.load_collection(layer, spatial_extent=bbox, temporal_extent=dates)

    assert cube.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {
                "id": {"from_parameter": "layer"},
                "temporal_extent": {"from_parameter": "dates"},
                "spatial_extent": {"from_parameter": "bbox"},
            },
            "result": True,
        }
    }


def test_build_parameterized_cube_filter_bbox_invalid_schema(con100):
    layer = Parameter.string("layer", description="Collection id")
    bbox = Parameter.string("bbox", description="Spatial extent")
    with pytest.warns(
        UserWarning,
        match="Unexpected parameterized `extent` in `filter_bbox`: expected schema compatible with type 'object' but got {'type': 'string'}.",
    ):
        cube = con100.load_collection(layer).filter_bbox(bbox)

    assert cube.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": {"from_parameter": "layer"}, "temporal_extent": None, "spatial_extent": None},
        },
        "filterbbox1": {
            "process_id": "filter_bbox",
            "arguments": {"data": {"from_node": "loadcollection1"}, "extent": {"from_parameter": "bbox"}},
            "result": True,
        },
    }


def test_build_parameterized_cube_load_collection_band(con100):
    layer = Parameter.string("layer")
    bands = [Parameter.string("band8"), Parameter.string("band12")]
    cube = con100.load_collection(layer, bands=bands)

    assert cube.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {
                "id": {"from_parameter": "layer"},
                "temporal_extent": None,
                "spatial_extent": None,
                "bands": [{"from_parameter": "band8"}, {"from_parameter": "band12"}]
            },
            "result": True,
        }
    }


def test_build_parameterized_cube_band_math(con100):
    layer = Parameter.string("layer")
    bands = [Parameter.string("band8"), Parameter.string("band12")]
    cube = con100.load_collection(layer, bands=bands)
    x = cube.band(0) * cube.band(1)
    assert x.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {
                "id": {"from_parameter": "layer"},
                "spatial_extent": None,
                "temporal_extent": None,
                "bands": [{"from_parameter": "band8"}, {"from_parameter": "band12"}],
            },
        },
        "reducedimension1": {
            "process_id": "reduce_dimension",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "dimension": "bands",
                "reducer": {"process_graph": {
                    "arrayelement1": {
                        "process_id": "array_element",
                        "arguments": {"data": {"from_parameter": "data"}, "index": 0},
                    },
                    "arrayelement2": {
                        "process_id": "array_element",
                        "arguments": {"data": {"from_parameter": "data"}, "index": 1},
                    },
                    "multiply1": {
                        "process_id": "multiply",
                        "arguments": {
                            "x": {"from_node": "arrayelement1"},
                            "y": {"from_node": "arrayelement2"}},
                        "result": True
                    }
                }}
            },
            "result": True
        }
    }


def test_build_process_dict_from_pg_dict():
    actual = build_process_dict(
        process_graph={
            "add": {"process_id": "add", "arguments": {"x": {"from_parameter": "data"}, "y": 1}, "result": True}
        },
        process_id="increment", summary="Increment value", description="Add 1 to input.",
        parameters=[Parameter.number(name="data")],
        returns={"schema": {"type": "number"}}
    )
    expected = {
        "id": "increment",
        "description": "Add 1 to input.",
        "summary": "Increment value",
        "process_graph": {
            "add": {"arguments": {"x": {"from_parameter": "data"}, "y": 1}, "process_id": "add", "result": True}
        },
        "parameters": [{"name": "data", "description": "data", "schema": {"type": "number"}}],
        "returns": {"schema": {"type": "number"}},
    }
    assert actual == expected


def test_build_process_dict_from_process(con100):
    from openeo.processes import add
    data = Parameter.number("data")
    proc = add(x=data, y=1)
    actual = build_process_dict(
        process_graph=proc,
        process_id="increment", summary="Increment value", description="Add 1 to input.",
        parameters=[data],
        returns={"schema": {"type": "number"}}
    )
    expected = {
        "id": "increment",
        "description": "Add 1 to input.",
        "summary": "Increment value",
        "process_graph": {
            "add1": {"arguments": {"x": {"from_parameter": "data"}, "y": 1}, "process_id": "add", "result": True}
        },
        "parameters": [{"name": "data", "description": "data", "schema": {"type": "number"}}],
        "returns": {"schema": {"type": "number"}},
    }
    assert actual == expected


def test_build_process_dict_from_datacube(con100):
    data = Parameter.number("data")
    cube = con100.datacube_from_process("add", x=data, y=1)
    actual = build_process_dict(
        process_graph=cube,
        process_id="increment", summary="Increment value", description="Add 1 to input.",
        parameters=[data],
        returns={"schema": {"type": "number"}}
    )
    expected = {
        "id": "increment",
        "description": "Add 1 to input.",
        "summary": "Increment value",
        "process_graph": {
            "add1": {"arguments": {"x": {"from_parameter": "data"}, "y": 1}, "process_id": "add", "result": True}
        },
        "parameters": [{"name": "data", "description": "data", "schema": {"type": "number"}}],
        "returns": {"schema": {"type": "number"}},
    }
    assert actual == expected


def test_build_process_dict_udf_context_param_field(con100):
    """https://github.com/Open-EO/openeo-python-client/issues/431"""
    udf_param = Parameter.number("udf_param")
    cube = con100.datacube_from_process("add", x=3, y=5)
    udf = openeo.UDF(code="print(123)", runtime="Python", context={"foo": udf_param})
    cube = cube.apply(process=udf)

    actual = build_process_dict(
        process_graph=cube,
        process_id="do_udf",
        summary="do_udf value",
        description="do_udf",
        parameters=[udf_param],
        returns={"schema": {"type": "number"}},
    )
    expected = {
        "id": "do_udf",
        "description": "do_udf",
        "parameters": [{"name": "udf_param", "description": "udf_param", "schema": {"type": "number"}}],
        "process_graph": {
            "add1": {"arguments": {"x": 3, "y": 5}, "process_id": "add"},
            "apply1": {
                "arguments": {
                    "data": {"from_node": "add1"},
                    "process": {
                        "process_graph": {
                            "runudf1": {
                                "process_id": "run_udf",
                                "arguments": {
                                    "data": {"from_parameter": "x"},
                                    "udf": "print(123)",
                                    "runtime": "Python",
                                    "context": {"foo": {"from_parameter": "udf_param"}},
                                },
                                "result": True,
                            }
                        }
                    },
                },
                "process_id": "apply",
                "result": True,
            },
        },
        "returns": {"schema": {"type": "number"}},
        "summary": "do_udf value",
    }
    assert actual == expected


def test_build_process_dict_udf_context_param_direct(con100):
    """https://github.com/Open-EO/openeo-python-client/issues/431"""
    udf_param = Parameter.number("udf_param")
    cube = con100.datacube_from_process("add", x=3, y=5)
    udf = openeo.UDF(code="print(123)", runtime="Python", context=udf_param)
    cube = cube.apply(process=udf)

    actual = build_process_dict(
        process_graph=cube,
        process_id="do_udf",
        summary="do_udf value",
        description="do_udf",
        parameters=[udf_param],
        returns={"schema": {"type": "number"}},
    )
    expected = {
        "id": "do_udf",
        "description": "do_udf",
        "parameters": [{"name": "udf_param", "description": "udf_param", "schema": {"type": "number"}}],
        "process_graph": {
            "add1": {"arguments": {"x": 3, "y": 5}, "process_id": "add"},
            "apply1": {
                "arguments": {
                    "data": {"from_node": "add1"},
                    "process": {
                        "process_graph": {
                            "runudf1": {
                                "process_id": "run_udf",
                                "arguments": {
                                    "data": {"from_parameter": "x"},
                                    "udf": "print(123)",
                                    "runtime": "Python",
                                    "context": {"from_parameter": "udf_param"},
                                },
                                "result": True,
                            }
                        }
                    },
                },
                "process_id": "apply",
                "result": True,
            },
        },
        "returns": {"schema": {"type": "number"}},
        "summary": "do_udf value",
    }
    assert actual == expected


def test_build_process_dict_processing_parameters(con100):
    actual = build_process_dict(
        process_graph={
            "add": {"process_id": "add", "arguments": {"x": {"from_parameter": "data"}, "y": 1}, "result": True}
        },
        process_id="increment",
        default_job_options={"memory": "4GB", "disk": "1TB"},
        default_synchronous_options={"timeout": 600},
    )
    expected = {
        "id": "increment",
        "process_graph": {
            "add": {"arguments": {"x": {"from_parameter": "data"}, "y": 1}, "process_id": "add", "result": True}
        },
        "default_job_options": {"disk": "1TB", "memory": "4GB"},
        "default_synchronous_options": {"timeout": 600},
    }
    assert actual == expected
