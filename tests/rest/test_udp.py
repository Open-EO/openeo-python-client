import openeo
import pytest

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
