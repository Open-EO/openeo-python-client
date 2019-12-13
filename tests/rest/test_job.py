import re

import openeo
import pytest

API_URL = "https://oeo.net"


@pytest.fixture
def session040(requests_mock):
    requests_mock.get(API_URL + "/", json={"api_version": "0.4.0"})
    session = openeo.connect(API_URL)
    return session


def test_execute_batch(session040, requests_mock, tmpdir):
    requests_mock.get(API_URL + "/collections/SENTINEL2", json={"foo": "bar"})
    requests_mock.post(API_URL + "/jobs", headers={"OpenEO-Identifier": "f00ba5"})
    requests_mock.post(API_URL + "/jobs/f00ba5/results")
    requests_mock.get(API_URL + "/jobs/f00ba5", [
        {'json': {"status": "submitted"}},
        {'json': {"status": "queued"}},
        {'json': {"status": "running", "progress": 15}},
        {'json': {"status": "running", "progress": 80}},
        {'json': {"status": "finished", "progress": 100}},
    ])
    requests_mock.get(API_URL + "/jobs/f00ba5/results", json={
        "links": [{"href": API_URL + "/jobs/f00ba5/files/output.tiff"}]
    })
    requests_mock.get(API_URL + "/jobs/f00ba5/files/output.tiff", text="tiffdata")

    path = tmpdir.join("tmp.tiff")
    log = []

    def print(msg):
        log.append(msg)

    session040.load_collection("SENTINEL2").execute_batch(
        outputfile=str(path), out_format="GTIFF",
        max_poll_interval=.1, print=print
    )

    assert re.match(r"0:00:00(.0\d*)? Job f00ba5: submitted \(progress N/A\)", log[0])
    assert re.match(r"0:00:00.1\d* Job f00ba5: queued \(progress N/A\)", log[1])
    assert re.match(r"0:00:00.2\d* Job f00ba5: running \(progress 15%\)", log[2])
    assert re.match(r"0:00:00.3\d* Job f00ba5: running \(progress 80%\)", log[3])
    assert re.match(r"0:00:00.4\d* Job f00ba5: finished \(progress 100%\)", log[4])

    assert path.read() == "tiffdata"
