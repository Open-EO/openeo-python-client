import io
import logging
import re
import textwrap

import pytest

import openeo.processes
from openeo.internal.graph_building import PGNode
from openeo.internal.processes.builder import (
    ProcessBuilderBase,
    convert_callable_to_pgnode,
    get_parameter_names,
)
from openeo.rest import OpenEoClientException


def test_process_builder_process_basic():
    builder = ProcessBuilderBase.process("foo", color="blue")
    assert builder.pgnode.flat_graph() == {
        "foo1": {"process_id": "foo", "arguments": {"color": "blue"}, "result": True}
    }


def test_process_builder_process_to_json():
    builder = ProcessBuilderBase.process("foo", color="blue")

    expected = '{"process_graph":{"foo1":{"process_id":"foo","arguments":{"color":"blue"},"result":true}}}'
    assert builder.to_json(indent=None, separators=(",", ":")) == expected

    expected = textwrap.dedent(
        """\
        {
          "process_graph": {
            "foo1": {
              "process_id": "foo",
              "arguments": {
                "color": "blue"
              },
              "result": true
            }
          }
        }"""
    )
    assert builder.to_json() == expected


def test_process_builder_process_print_json():
    builder = ProcessBuilderBase.process("foo", color="blue")
    out = io.StringIO()
    builder.print_json(file=out, indent=None)
    assert (
        out.getvalue()
        == '{"process_graph": {"foo1": {"process_id": "foo", "arguments": {"color": "blue"}, "result": true}}}\n'
    )


def test_process_builder_process_namespace():
    builder = ProcessBuilderBase.process("foo", namespace="bar", color="blue")
    assert builder.pgnode.flat_graph() == {
        "foo1": {"process_id": "foo", "namespace": "bar", "arguments": {"color": "blue"}, "result": True}
    }


def test_get_parameter_names():
    def add_stuff(foo, bar, *args, **kwargs):
        return foo + bar + args + kwargs

    assert get_parameter_names(add_stuff) == ["foo", "bar"]


class TestConvertCallableToPgnode:

    def test_simple_lambda(self):
        result = convert_callable_to_pgnode(lambda x: x + 5)
        assert isinstance(result, PGNode)
        assert result.flat_graph() == {
            "add1": {"process_id": "add", "arguments": {"x": {"from_parameter": "x"}, "y": 5}, "result": True}
        }

    def test_simple_def(self):
        def callback(data):
            return data.count()

        result = convert_callable_to_pgnode(callback)
        assert isinstance(result, PGNode)
        assert result.flat_graph() == {
            "count1": {"process_id": "count", "arguments": {"data": {"from_parameter": "data"}}, "result": True}
        }

    def test_simple_predefined(self):
        result = convert_callable_to_pgnode(openeo.processes.is_valid)
        assert isinstance(result, PGNode)
        assert result.flat_graph() == {
            "isvalid1": {
                "process_id": "is_valid",
                "arguments": {
                    "x": {"from_parameter": "x"},
                },
                "result": True,
            }
        }

    def test_no_parent_parameter_info(self, recwarn):

        def my_callback(data, condition=False, context=None):
            return data.count(condition=condition, context=context)

        result = convert_callable_to_pgnode(my_callback)
        assert isinstance(result, PGNode)
        assert result.flat_graph() == {
            "count1": {
                "process_id": "count",
                "arguments": {
                    "data": {"from_parameter": "data"},
                    "condition": {"from_parameter": "condition"},
                    "context": {"from_parameter": "context"},
                },
                "result": True}
        }

        assert re.search(
            r"Blindly using callback parameter names from.*my_callback.*argument names: \['data', 'condition', 'context'\]",
            "\n".join(str(w.message) for w in recwarn.list),
        )

    def test_with_parent_parameter_info(self):
        def callback(data, condition=False, context=None):
            return data.count(condition=condition, context=context)

        result = convert_callable_to_pgnode(callback, parent_parameters=["data"])
        assert isinstance(result, PGNode)
        assert result.flat_graph() == {
            "count1": {
                "process_id": "count",
                "arguments": {
                    "data": {"from_parameter": "data"},
                    "condition": False,
                    "context": None,
                },
                "result": True}
        }

        result = convert_callable_to_pgnode(callback, parent_parameters=["data", "context"])
        assert isinstance(result, PGNode)
        assert result.flat_graph() == {
            "count1": {
                "process_id": "count",
                "arguments": {
                    "data": {"from_parameter": "data"},
                    "condition": False,
                    "context": {"from_parameter": "context"},
                },
                "result": True}
        }

    def test_naming_mismatch_fallback(self):
        def increment(start, to_add=1):
            return start + to_add

        result = convert_callable_to_pgnode(increment, parent_parameters=["data"])
        assert isinstance(result, PGNode)
        assert result.flat_graph() == {
            "add1": {"process_id": "add", "arguments": {"x": {"from_parameter": "data"}, "y": 1}, "result": True}
        }

    @pytest.mark.parametrize(["callback", "parent_parameters", "expected"], [
        (lambda: openeo.processes.pi(), [], {"pi1": {"process_id": "pi", "arguments": {}, "result": True}}),
        (lambda: openeo.processes.pi(), ["data"], {"pi1": {"process_id": "pi", "arguments": {}, "result": True}}),
        (lambda x=0: openeo.processes.pi(), ["data"], {"pi1": {"process_id": "pi", "arguments": {}, "result": True}}),
        (lambda x=0: openeo.processes.pi(), [], {"pi1": {"process_id": "pi", "arguments": {}, "result": True}}),
    ])
    def test_zero_params(self, callback, parent_parameters, expected):
        result = convert_callable_to_pgnode(callback, parent_parameters=parent_parameters)
        assert isinstance(result, PGNode)
        assert result.flat_graph() == expected

    @pytest.mark.parametrize(["callback", "parent_parameters", "expected"], [
        (
                lambda x: x + 1,
                ["data"],
                {"add1": {"process_id": "add", "arguments": {"x": {"from_parameter": "data"}, "y": 1}, "result": True}},
        ),
        (
                lambda x, y=5: x + y,
                ["data"],
                {"add1": {"process_id": "add", "arguments": {"x": {"from_parameter": "data"}, "y": 5}, "result": True}},
        ),
        (
                lambda x: x + 1,
                ["data", "other", "context"],
                {"add1": {"process_id": "add", "arguments": {"x": {"from_parameter": "data"}, "y": 1}, "result": True}},
        ),
    ])
    def test_one_parameter(self, callback, parent_parameters, expected):
        result = convert_callable_to_pgnode(callback, parent_parameters=parent_parameters)
        assert isinstance(result, PGNode)
        assert result.flat_graph() == expected

    def test_parameter_mismatch(self):
        def callback(x, y=5):
            return x + y

        with pytest.raises(OpenEoClientException, match="Callback argument mismatch"):
            _ = convert_callable_to_pgnode(callback, parent_parameters=["data", "other"])

    def test_partial_parameter_match(self):
        def callback(data, y=5):
            return data + y

        result = convert_callable_to_pgnode(callback, parent_parameters=["data", "other"])
        assert isinstance(result, PGNode)
        assert result.flat_graph() == {
            "add1": {
                "process_id": "add",
                "arguments": {"x": {"from_parameter": "data"}, "y": 5},
                "result": True,
            }
        }

    @pytest.mark.parametrize(["callback", "expected"], [
        (lambda data: data.sum(), {
            "sum1": {
                "process_id": "sum",
                "arguments": {"data": [{"from_parameter": "x"}, {"from_parameter": "y"}]},
                "result": True,
            }
        }),
        (lambda values: values.sum(), {
            "sum1": {
                "process_id": "sum",
                "arguments": {"data": [{"from_parameter": "x"}, {"from_parameter": "y"}]},
                "result": True,
            }
        }),
        (lambda data, offset=1: data.sum() + offset, {
            "sum1": {"process_id": "sum", "arguments": {"data": [{"from_parameter": "x"}, {"from_parameter": "y"}]}},
            "add1": {"process_id": "add", "arguments": {"x": {"from_node": "sum1"}, "y": 1}, "result": True}
        }),
    ])
    def test_xy_adapter_simple(self, callback, expected):
        result = convert_callable_to_pgnode(callback, parent_parameters=["x", "y"])
        assert isinstance(result, PGNode)
        assert result.flat_graph() == expected
