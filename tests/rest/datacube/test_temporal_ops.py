import openeo
import pytest
from openeo.internal.graphbuilder_040 import GraphBuilder as GraphBuilder040

from ... import load_json_resource, get_download_graph

API_URL = "https://oeo.net"


@pytest.fixture(params=["0.4.0", "1.0.0"])
def api_version(request):
    return request.param


@pytest.fixture
def connection(api_version, requests_mock):
    """
    Fixture connection with given api_version to a backend
    with a some default image collections
    """
    requests_mock.get(API_URL + "/", json={"api_version": api_version})
    s2_properties = {
        "properties": {
            "cube:dimensions": {
                "bands": {"type": "bands", "values": ["B02", "B03", "B04", "B08"]}
            },
            "eo:bands": [
                {"name": "B02", "common_name": "blue", "center_wavelength": 0.4966},
                {"name": "B03", "common_name": "green", "center_wavelength": 0.560},
                {"name": "B04", "common_name": "red", "center_wavelength": 0.6645},
                {"name": "B08", "common_name": "nir", "center_wavelength": 0.8351},
            ]
        }
    }
    requests_mock.get(API_URL + "/collections/SENTINEL2_RADIOMETRY_10M", json=s2_properties)
    requests_mock.get(API_URL + "/collections/S2", json=s2_properties)
    requests_mock.get(API_URL + "/collections/MASK", json={})
    return openeo.connect(API_URL)


def test_apply_dimension_temporal_cumsum(connection, api_version):
    s2 = connection.load_collection("SENTINEL2_RADIOMETRY_10M")
    cumsum = s2.apply_dimension('cumsum')
    actual_graph = get_download_graph(cumsum)
    expected_graph = load_json_resource('data/{v}/apply_dimension_temporal_cumsum.json'.format(v=api_version))
    assert actual_graph == expected_graph


def test_min_time(connection, api_version):
    s2 = connection.load_collection("S2")
    min_time = s2.min_time()
    actual_graph = get_download_graph(min_time)
    expected_graph = load_json_resource('data/{v}/min_time.json'.format(v=api_version))
    assert actual_graph == expected_graph
