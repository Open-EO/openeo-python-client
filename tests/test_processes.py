import pytest

import openeo.processes
from openeo.internal.graph_building import PGNode


EXPECTED_123_APPLY_ABSOLUTE = {
    "apply1": {
        "process_id": "apply",
        "arguments": {
            "data": [1, 2, 3],
            "process": {
                "process_graph": {
                    "absolute1": {
                        "process_id": "absolute",
                        "arguments": {"x": {"from_parameter": "x"}},
                        "result": True,
                    }
                }
            },
        },
        "result": True,
    }
}


def test_apply_str():
    res = openeo.processes.apply(data=[1, 2, 3], process="absolute")
    assert res.flat_graph() == EXPECTED_123_APPLY_ABSOLUTE


def test_apply_pgnode():
    res = openeo.processes.apply(
        data=[1, 2, 3],
        process=PGNode(process_id="absolute", arguments={"x": {"from_parameter": "x"}}),
    )
    assert res.flat_graph() == EXPECTED_123_APPLY_ABSOLUTE


@pytest.mark.parametrize(
    "callable",
    [
        lambda x: x.absolute(),
        lambda x: openeo.processes.absolute(x),
        openeo.processes.absolute,
    ],
)
def test_apply_callable(callable):
    res = openeo.processes.apply(data=[1, 2, 3], process=callable)
    assert res.flat_graph() == EXPECTED_123_APPLY_ABSOLUTE


def test_apply_udf():
    res = openeo.processes.apply(
        data=[1, 2, 3],
        process=openeo.UDF("def foo(): pass"),
    )
    assert res.flat_graph() == {
        "apply1": {
            "process_id": "apply",
            "arguments": {
                "data": [1, 2, 3],
                "process": {
                    "process_graph": {
                        "runudf1": {
                            "arguments": {
                                "data": {"from_parameter": "x"},
                                "runtime": "Python",
                                "udf": "def " "foo(): " "pass",
                            },
                            "process_id": "run_udf",
                            "result": True,
                        }
                    }
                },
            },
            "result": True,
        }
    }


def test_merge_cubes_no_overlap_resolver():
    res = openeo.processes.merge_cubes(cube1="dummy1", cube2="dummy2")
    assert res.flat_graph() == {
        "mergecubes1": {
            "process_id": "merge_cubes",
            "arguments": {"cube1": "dummy1", "cube2": "dummy2"},
            "result": True,
        }
    }


def test_merge_cubes_overlap_resolver_none():
    res = openeo.processes.merge_cubes(cube1="dummy1", cube2="dummy2", overlap_resolver=None)
    assert res.flat_graph() == {
        "mergecubes1": {
            "process_id": "merge_cubes",
            "arguments": {"cube1": "dummy1", "cube2": "dummy2", "overlap_resolver": None},
            "result": True,
        }
    }
