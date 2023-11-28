import re
import shutil
from io import StringIO
from textwrap import dedent

import pytest

from openeo.internal.processes.generator import (
    PythonRenderer,
    collect_processes,
    generate_process_py,
)
from openeo.internal.processes.parse import Process
from tests import get_test_resource


def test_render_basic():
    process = Process.from_dict({
        "id": "incr",
        "description": "Increment a value",
        "summary": "Increment a value",
        "parameters": [{"name": "x", "description": "value", "schema": {"type": "integer"}}],
        "returns": {"description": "incremented value", "schema": {"type": "integer"}}
    })

    renderer = PythonRenderer()
    src = renderer.render_process(process)
    assert src == dedent('''\
        def incr(x):
            """
            Increment a value

            :param x: value

            :return: incremented value
            """
            return _process('incr', x=x)''')


def test_render_no_params():
    process = Process.from_dict({
        "id": "pi",
        "description": "Pi",
        "summary": "Pi",
        "parameters": [],
        "returns": {"description": "value of pi", "schema": {"type": "number"}}
    })

    renderer = PythonRenderer()
    src = renderer.render_process(process)
    assert src == dedent('''\
        def pi():
            """
            Pi

            :return: value of pi
            """
            return _process('pi', )''')


def test_render_with_default():
    process = Process.from_dict({
        "id": "incr",
        "description": "Increment a value",
        "summary": "Increment a value",
        "parameters": [
            {"name": "x", "description": "value", "schema": {"type": "integer"}},
            {"name": "i", "description": "increment", "schema": {"type": "integer"}, "default": 1},
        ],
        "returns": {"description": "incremented value", "schema": {"type": "integer"}}
    })

    renderer = PythonRenderer()
    src = renderer.render_process(process)
    assert src == dedent('''\
        def incr(x, i=1):
            """
            Increment a value

            :param x: value
            :param i: increment

            :return: incremented value
            """
            return _process('incr', x=x, i=i)''')


def test_render_with_optional():
    process = Process.from_dict({
        "id": "foo",
        "description": "Foo",
        "summary": "Foo",
        "parameters": [
            {"name": "x", "description": "value", "schema": {"type": "integer"}},
            {"name": "y", "description": "something", "schema": {"type": "integer"}, "optional": True, "default": 1},
        ],
        "returns": {"description": "new value", "schema": {"type": "integer"}}
    })

    renderer = PythonRenderer(optional_default="UNSET")
    src = renderer.render_process(process)
    assert src == dedent('''\
        def foo(x, y=UNSET):
            """
            Foo

            :param x: value
            :param y: something

            :return: new value
            """
            return _process('foo', x=x, y=y)''')


def test_render_return_type_hint():
    process = Process.from_dict({
        "id": "incr",
        "description": "Increment a value",
        "summary": "Increment a value",
        "parameters": [{"name": "x", "description": "value", "schema": {"type": "integer"}}],
        "returns": {"description": "incremented value", "schema": {"type": "integer"}}
    })

    renderer = PythonRenderer(return_type_hint="FooBar")
    src = renderer.render_process(process)
    assert src == dedent('''\
        def incr(x) -> FooBar:
            """
            Increment a value

            :param x: value

            :return: incremented value
            """
            return _process('incr', x=x)''')


def test_render_oo_no_params():
    process = Process.from_dict({
        "id": "pi",
        "description": "Pi",
        "summary": "Pi",
        "parameters": [],
        "returns": {"description": "value of pi", "schema": {"type": "number"}}
    })

    renderer = PythonRenderer(oo_mode=True)
    src = "class Consts:\n" + renderer.render_process(process)
    assert src == dedent('''\
        class Consts:
            def pi(self):
                """
                Pi

                :return: value of pi
                """
                return _process('pi', )''')


def test_render_keyword():
    process = Process.from_dict({
        "id": "or",
        "description": "Boolean and",
        "summary": "Boolean and",
        "parameters": [
            {"name": "x", "description": "value", "schema": {"type": ["boolean", "null"]}},
            {"name": "y", "description": "value", "schema": {"type": ["boolean", "null"]}}
        ],
        "returns": {"description": "result", "schema": {"type": ["boolean", "null"]}},
    })
    renderer = PythonRenderer()
    src = renderer.render_process(process)
    assert src == dedent('''\
        def or_(x, y):
            """
            Boolean and

            :param x: value
            :param y: value

            :return: result
            """
            return _process('or', x=x, y=y)''')

    oo_renderer = PythonRenderer(oo_mode=True, body_template="return {safe_name}({args})", )
    src = oo_renderer.render_process(process)
    assert dedent(src) == dedent('''\
        def or_(self, y):
            """
            Boolean and

            :param self: value
            :param y: value

            :return: result
            """
            return or_(x=self, y=y)''')


def test_render_process_graph_callback():
    process = Process.from_dict(
        {
            "id": "apply",
            "description": "Apply",
            "summary": "Apply",
            "parameters": [
                {
                    "name": "data",
                    "description": "Data cube",
                    "schema": {"type": "object", "subtype": "raster-cube"},
                },
                {
                    "name": "process",
                    "description": "Process",
                    "schema": {
                        "type": "object",
                        "subtype": "process-graph",
                        "parameters": [{"name": "data", "schema": {"type": "array"}}],
                    },
                },
            ],
            "returns": {"description": "Data cube", "schema": {"type": "object", "subtype": "raster-cube"}},
        }
    )

    renderer = PythonRenderer(optional_default="UNSET")
    src = renderer.render_process(process)
    assert src == dedent(
        '''\
        def apply(data, process):
            """
            Apply

            :param data: Data cube
            :param process: Process

            :return: Data cube
            """
            return _process('apply', data=data, process=build_child_callback(process, parent_parameters=['data']))'''
    )


def test_render_process_graph_callback_wrapping():
    process = Process.from_dict(
        {
            "id": "apply_dimension",
            "description": "Apply",
            "summary": "Apply",
            "parameters": [
                {
                    "name": "data",
                    "description": "Data cube",
                    "schema": {"type": "object", "subtype": "raster-cube"},
                },
                {
                    "name": "dimension",
                    "description": "Dimension",
                    "schema": {"type": "string"},
                },
                {
                    "name": "process",
                    "description": "Process",
                    "schema": {
                        "type": "object",
                        "subtype": "process-graph",
                        "parameters": [{"name": "data", "schema": {"type": "array"}}],
                    },
                },
            ],
            "returns": {"description": "Data cube", "schema": {"type": "object", "subtype": "raster-cube"}},
        }
    )

    renderer = PythonRenderer(optional_default="UNSET")
    src = renderer.render_process(process, width=80)
    assert src == dedent(
        '''\
        def apply_dimension(data, dimension, process):
            """
            Apply

            :param data: Data cube
            :param dimension: Dimension
            :param process: Process

            :return: Data cube
            """
            return _process('apply_dimension',
                data=data,
                dimension=dimension,
                process=build_child_callback(process, parent_parameters=['data'])
            )'''
    )


def test_collect_processes_basic(tmp_path):
    processes = collect_processes(sources=[get_test_resource("data/processes/1.0")])
    assert [p.id for p in processes] == ["add", "cos"]


def test_collect_processes_multiple_sources(tmp_path):
    processes = collect_processes(sources=[
        get_test_resource("data/processes/1.0/cos.json"),
        get_test_resource("data/processes/1.0/add.json"),
    ])
    assert [p.id for p in processes] == ["add", "cos"]


def test_collect_processes_duplicates(tmp_path):
    shutil.copy(get_test_resource("data/processes/1.0/cos.json"), tmp_path / "foo.json")
    shutil.copy(get_test_resource("data/processes/1.0/cos.json"), tmp_path / "bar.json")
    with pytest.raises(Exception, match="Duplicate source for process 'cos'"):
        _ = collect_processes(sources=[tmp_path])


def test_generate_process_py():
    processes = [
        Process.from_dict({
            "id": "incr",
            "description": "Increment a value",
            "summary": "Increment a value",
            "parameters": [{"name": "x", "description": "value", "schema": {"type": "integer"}}],
            "returns": {"description": "incremented value", "schema": {"type": "integer"}}
        }),
        Process.from_dict({
            "id": "add", "description": "add", "summary": "add",
            "parameters": [
                {"name": "x", "description": "value", "schema": {"type": "integer"}},
                {"name": "y", "description": "value", "schema": {"type": "integer"}},
            ],
            "returns": {"description": "x+y", "schema": {"type": "integer"}}
        }),
    ]

    output = StringIO()
    generate_process_py(processes, output=output)
    content = output.getvalue()
    assert "\nclass ProcessBuilder(ProcessBuilderBase):\n" in content
    assert re.search(
        '\n    def incr\\(self\\) -> ProcessBuilder:\n        """[^"]*"""\n        return incr\\(x=self\\)\n',
        content,
        flags=re.DOTALL,
    )
    assert re.search(
        '\n    def add\\(self, y\\) -> ProcessBuilder:\n        """[^"]*"""\n        return add\\(x=self, y=y\\)\n',
        content,
        flags=re.DOTALL,
    )
    assert re.search(
        '\ndef incr\\(x\\) -> ProcessBuilder:\n    """[^"]*"""\n    return _process\\(\'incr\', x=x\\)\n',
        content,
        flags=re.DOTALL,
    )
    assert re.search(
        '\ndef add\\(x, y\\) -> ProcessBuilder:\n    """[^"]*"""\n    return _process\\(\'add\', x=x, y=y\\)\n',
        content,
        flags=re.DOTALL,
    )
