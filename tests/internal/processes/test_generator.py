from io import StringIO
from textwrap import dedent

from openeo.internal.processes.generator import PythonRenderer, generate_process_py, collect_processes
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


def test_collect_processes_basic(tmp_path):
    processes = collect_processes(sources=[get_test_resource("data/processes/1.0")])
    assert [p.id for p in processes] == ["add", "cos"]


def test_collect_processes_multiple_sources(tmp_path):
    processes = collect_processes(sources=[
        get_test_resource("data/processes/1.0/cos.json"),
        get_test_resource("data/processes/1.0/add.json"),
    ])
    assert [p.id for p in processes] == ["add", "cos"]


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
    lines = output.getvalue().split("\n")
    assert "class ProcessBuilder(ProcessBuilderBase):" in lines
    assert "    def incr(self) -> 'ProcessBuilder':" in lines
    assert "    def add(self, y) -> 'ProcessBuilder':" in lines
    assert "def incr(x) -> ProcessBuilder:" in lines
    assert "def add(x, y) -> ProcessBuilder:" in lines
