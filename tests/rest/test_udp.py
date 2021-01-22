import pytest

import openeo
from openeo.api.process import Parameter
from openeo.rest.udp import RESTUserDefinedProcess
from .. import load_json_resource

API_URL = "https://oeo.test"


@pytest.fixture
def con100(requests_mock):
    requests_mock.get(API_URL + "/", json={"api_version": "1.0.0"})
    con = openeo.connect(API_URL)
    return con


def test_describe(con100, requests_mock):
    expected_details = load_json_resource("data/1.0.0/udp_details.json")
    requests_mock.get(API_URL + "/process_graphs/evi", json=expected_details)

    udp = con100.user_defined_process(user_defined_process_id='evi')
    details = udp.describe()

    assert details == expected_details


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


def test_store_collision(con100, requests_mock):
    subtract = {
        "minusy": {
            "process_id": "multiply",
            "arguments": {"x": {"from_parameter": "y", "y": -1}}
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
            "public": False,
        }
        return True

    requests_mock.get(API_URL + "/processes", json={"processes": [{"id": "add"}, {"id": "subtract"}]})
    adapter1 = requests_mock.put(API_URL + "/process_graphs/my_subtract", additional_matcher=check_body)
    adapter2 = requests_mock.put(API_URL + "/process_graphs/subtract", additional_matcher=check_body)

    with pytest.warns(None) as recorder:
        udp = con100.save_user_defined_process("my_subtract", subtract)
    assert isinstance(udp, RESTUserDefinedProcess)
    assert adapter1.call_count == 1
    assert len(recorder) == 0

    with pytest.warns(UserWarning, match="same id as a pre-defined process") as recorder:
        udp = con100.save_user_defined_process("subtract", subtract)
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


def test_update(con100, requests_mock):
    updated_udp = load_json_resource("data/1.0.0/udp_details.json")

    def check_body(request):
        body = request.json()
        assert body['process_graph'] == updated_udp['process_graph']
        assert body['parameters'] == updated_udp['parameters']
        assert not body.get('public', False)
        return True

    adapter = requests_mock.put(API_URL + "/process_graphs/evi", additional_matcher=check_body)

    udp = con100.user_defined_process(user_defined_process_id='evi')

    udp.update(process_graph=updated_udp['process_graph'], parameters=updated_udp['parameters'])

    assert adapter.called


def test_make_public(con100, requests_mock):
    udp = con100.user_defined_process(user_defined_process_id='evi')

    def check_body(request):
        body = request.json()
        assert body['process_graph'] == updated_udp['process_graph']
        assert body['parameters'] == updated_udp['parameters']
        assert body['public']
        return True

    adapter = requests_mock.put(API_URL + "/process_graphs/evi", additional_matcher=check_body)

    updated_udp = load_json_resource("data/1.0.0/udp_details.json")

    udp.update(process_graph=updated_udp['process_graph'], parameters=updated_udp['parameters'], public=True)

    assert adapter.called


def test_delete(con100, requests_mock):
    adapter = requests_mock.delete(API_URL + "/process_graphs/evi", status_code=204)

    udp = con100.user_defined_process(user_defined_process_id='evi')
    udp.delete()

    assert adapter.called


def test_build_parameterized_cube_basic(con100):
    layer = Parameter.string("layer")
    dates = Parameter.string("dates")
    bbox = Parameter("bbox", schema="object")
    cube = con100.load_collection(layer).filter_temporal(dates).filter_bbox(bbox)

    assert cube.flatten() == {
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


def test_build_parameterized_cube_single_date(con100):
    layer = Parameter.string("layer")
    date = Parameter.string("date")
    bbox = Parameter("bbox", schema="object")
    cube = con100.load_collection(layer).filter_temporal(date, date).filter_bbox(bbox)

    assert cube.flatten() == {
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


def test_build_parameterized_cube_start_date(con100):
    layer = Parameter.string("layer")
    start = Parameter.string("start")
    bbox = Parameter("bbox", schema="object")
    cube = con100.load_collection(layer).filter_temporal(start, None).filter_bbox(bbox)

    assert cube.flatten() == {
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


def test_build_parameterized_cube_load_collection(con100):
    layer = Parameter.string("layer")
    dates = Parameter.string("dates")
    bbox = Parameter("bbox", schema="object")
    cube = con100.load_collection(layer, spatial_extent=bbox, temporal_extent=dates)

    assert cube.flatten() == {
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


def test_build_parameterized_cube_load_collection_band(con100):
    layer = Parameter.string("layer")
    bands = [Parameter.string("band8"), Parameter.string("band12")]
    cube = con100.load_collection(layer, bands=bands)

    assert cube.flatten() == {
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
    x = cube.band(0) * cube.band(Parameter.string("band12"))
    assert x.flatten() == {
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
