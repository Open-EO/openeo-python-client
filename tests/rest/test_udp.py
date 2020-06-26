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

    udp = con100.process_graph(process_graph_id='evi')
    details = udp.describe()

    assert details == expected_details


def test_update(con100, requests_mock):
    updated_udp = load_json_resource("data/1.0.0/udp_details.json")

    def check_body(request):
        assert request.json() == updated_udp
        return True

    adapter = requests_mock.put(API_URL + "/process_graphs/evi", additional_matcher=check_body)

    udp = con100.process_graph(process_graph_id='evi')
    process_graph_metadata = {k: v for k, v in updated_udp.items() if k != 'process_graph'}

    udp.update(process_graph=updated_udp['process_graph'], **process_graph_metadata)

    assert adapter.called
