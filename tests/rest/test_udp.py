import openeo
import pytest

API_URL = "https://oeo.net"


@pytest.fixture
def con100(requests_mock):
    requests_mock.get(API_URL + "/", json={"api_version": "1.0.0"})
    con = openeo.connect(API_URL)
    return con


def test_describe(con100, requests_mock):
    # TODO: read it from a file?
    requests_mock.get(API_URL + "/process_graphs/evi", json={
        "id": "evi",
        "summary": "Enhanced Vegetation Index",
        "description": "Computes the Enhanced Vegetation Index (EVI). It is computed with the following formula: `2.5 * (NIR - RED) / (1 + NIR + 6*RED + -7.5*BLUE)`.",
    })

    udp = con100.process_graph(process_graph_id='evi')
    details = udp.describe()

    assert details['summary'] == "Enhanced Vegetation Index"


def test_update(con100, requests_mock):
    updated_process_graph = {
        'description': "Computes the Enhanced Vegetation Index (EVI).",
        'parameters': [],
        'process_graph': {
            'sub': {
                'process_id': 'add'
            }
        }
    }

    def check_body(request):
        assert request.json() == updated_process_graph
        return True

    requests_mock.put(API_URL + "/process_graphs/evi", additional_matcher=check_body)

    udp = con100.process_graph(process_graph_id='evi')
    udp.update(
        process_graph={
            'sub': {
                'process_id': 'add'
            }
        },
        description="Computes the Enhanced Vegetation Index (EVI).",
        parameters=[]
    )
