import dirty_equals
import pytest

from openeo.internal.graph_building import as_flat_graph
from openeo.rest.graph_building import CollectionProperty, collection_property


class TestCollectionProperty:
    @staticmethod
    def _as_flat_graph(prop: CollectionProperty) -> dict:
        return as_flat_graph(prop.from_node())

    def test_eq(self):
        prop = collection_property("color") == "blue"
        assert self._as_flat_graph(prop) == {
            "eq1": {
                "process_id": "eq",
                "arguments": {"x": {"from_parameter": "value"}, "y": "blue"},
                "result": True,
            }
        }

    def test_ne(self):
        prop = collection_property("color") != "blue"
        assert self._as_flat_graph(prop) == {
            "neq1": {
                "process_id": "neq",
                "arguments": {"x": {"from_parameter": "value"}, "y": "blue"},
                "result": True,
            }
        }

    @pytest.mark.parametrize(
        ["compare", "expected_process_id"],
        [
            (lambda v: v > 5, "gt"),
            (lambda v: v >= 5, "gte"),
            (lambda v: v < 5, "lt"),
            (lambda v: v <= 5, "lte"),
        ],
    )
    def test_compare(self, compare, expected_process_id):
        prop = compare(collection_property("size"))
        assert self._as_flat_graph(prop) == {
            f"{expected_process_id}1": {
                "process_id": expected_process_id,
                "arguments": {"x": {"from_parameter": "value"}, "y": 5},
                "result": True,
            }
        }

    @pytest.mark.parametrize(
        "args",
        [
            # Given as single arg list/tuple/set
            (["MGRS-32ULB", "MGRS-32UMB"],),
            (("MGRS-32ULB", "MGRS-32UMB"),),
            ({"MGRS-32ULB", "MGRS-32UMB"},),
            # Given as multiple args
            ("MGRS-32ULB", "MGRS-32UMB"),
        ],
    )
    def test_is_one_of(self, args):
        prop = collection_property("tile_id").is_one_of(*args)
        assert self._as_flat_graph(prop) == {
            f"arraycontains1": {
                "process_id": "array_contains",
                "arguments": {
                    "data": dirty_equals.IsOneOf(
                        ["MGRS-32ULB", "MGRS-32UMB"],
                        ["MGRS-32UMB", "MGRS-32ULB"],
                    ),
                    "value": {"from_parameter": "value"},
                },
                "result": True,
            }
        }
