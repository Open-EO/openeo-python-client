import openeo
import pytest

API_URL = "https://oeo.net"


@pytest.fixture
def session040(requests_mock):
    requests_mock.get(API_URL + "/", json={"api_version": "0.4.0"})
    session = openeo.connect(API_URL)
    return session


@pytest.fixture
def con100(requests_mock):
    requests_mock.get(API_URL + "/", json={"api_version": "1.0.0"})
    con = openeo.connect(API_URL)
    return con


def test_load_json(con100, requests_mock):
    requests_mock.get(API_URL + '/jobs/1/results', json={'assets': {'out': {'href': API_URL + '/jobs/1/results/out'}}})
    requests_mock.get(API_URL + '/jobs/1/results/out', json={'2018-08-03T00:00:00Z': [[], [], [None, None]]})

    json = con100.job('1').get_result().load_json()

    assert json == {'2018-08-03T00:00:00Z': [[], [], [None, None]]}


def test_load_json_040(session040, requests_mock):
    requests_mock.get(API_URL + '/jobs/1/results', json={'links': [{'href': API_URL + '/jobs/1/results/out'}]})
    requests_mock.get(API_URL + '/jobs/1/results/out', json={'2018-08-03T00:00:00Z': [[], [], [None, None]]})

    json = session040.job('1').get_result().load_json()

    assert json == {'2018-08-03T00:00:00Z': [[], [], [None, None]]}


def test_load_bytes(con100, requests_mock):
    requests_mock.get(API_URL + '/jobs/1/results', json={'assets': {'out': {'href': API_URL + '/jobs/1/results/out'}}})
    requests_mock.get(API_URL + '/jobs/1/results/out', content=b'\x01\x02\x03\x04\x05')

    content = con100.job('1').get_result().load_bytes()

    assert content == b'\x01\x02\x03\x04\x05'


def test_load_bytes_040(session040, requests_mock):
    requests_mock.get(API_URL + '/jobs/1/results', json={'links': [{'href': API_URL + '/jobs/1/results/out'}]})
    requests_mock.get(API_URL + '/jobs/1/results/out', content=b'\x01\x02\x03\x04\x05')

    content = session040.job('1').get_result().load_bytes()

    assert content == b'\x01\x02\x03\x04\x05'

