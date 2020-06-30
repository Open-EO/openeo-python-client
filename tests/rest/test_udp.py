import openeo
import pytest
from .. import load_json_resource

API_URL = "https://oeo.net"


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
    adapter = requests_mock.delete(API_URL + "/process_graphs/evi")

    udp = con100.user_defined_process(user_defined_process_id='evi')
    udp.delete()

    assert adapter.called
