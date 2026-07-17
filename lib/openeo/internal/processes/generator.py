import argparse
import datetime
import keyword
import sys
import textwrap
from pathlib import Path
from typing import Iterator, List, Optional, Union

from openeo.internal.processes.parse import Process, parse_all_from_dir


class PythonRenderer:
    """Generator of Python function source code for a given openEO process"""

    DEFAULT_WIDTH = 115

    def __init__(
        self,
        oo_mode: bool = False,
        indent: str = "    ",
        body_template: str = "return _process({id!r}, {args})",
        optional_default="None",
        return_type_hint: Optional[str] = None,
        decorator: Optional[str] = None,
    ):
        self.oo_mode = oo_mode
        self.indent = indent
        self.body_template = body_template
        self.optional_default = optional_default
        self.return_type_hint = return_type_hint
        self.decorator = decorator

    def render_process(self, process: Process, prefix: str = None, width: int = DEFAULT_WIDTH) -> str:
        if prefix is None:
            prefix = "    " if self.oo_mode else ""

        # TODO: add type hints
        # TODO: width limit?
        def_line = "def {id}({args}){th}:".format(
            id=self._safe_name(process.id),
            args=", ".join(self._def_arguments(process)),
            th=" -> {t}".format(t=self.return_type_hint) if self.return_type_hint else "",
        )

        call_args = ", ".join(self._call_args(process))
        if len(call_args) > width:
            # TODO: also include `id` placeholder in `self.body_format`
            call_args = (
                "\n" + ",\n".join(self.indent + self.indent + a for a in self._call_args(process)) + "\n" + self.indent
            )
        body = self.indent + self.body_template.format(
            id=process.id, safe_name=self._safe_name(process.id), args=call_args
        )

        lines = ([self.decorator] if self.decorator else []) + [
            def_line,
            self.render_docstring(process, width=width - len(prefix), prefix=self.indent),
            body,
        ]
        return textwrap.indent("\n".join(lines), prefix=prefix)

    def _safe_name(self, name: str) -> str:
        if keyword.iskeyword(name):
            name += "_"
        return name

    def _par_names(self, process: Process) -> List[str]:
        """Names of the openEO process parameters"""
        return [self._safe_name(p.name) for p in process.parameters]

    def _arg_names(self, process: Process) -> List[str]:
        """Names of the arguments in the python function"""
        arg_names = self._par_names(process)
        if self.oo_mode and arg_names:
            arg_names[0] = "self"
        return arg_names

    def _call_args(self, process: Process) -> Iterator[str]:
        for parameter, par_name, arg_name in zip(
            process.parameters, self._par_names(process), self._arg_names(process)
        ):
            arg_expression = arg_name
            if parameter.schema.is_process_graph():
                parent_parameters = [p["name"] for p in parameter.schema.schema["parameters"]]
                arg_expression = f"build_child_callback({arg_expression}, parent_parameters={parent_parameters})"
                if parameter.optional:
                    arg_expression = (
                        f"({arg_expression} if {arg_name} not in [None, {self.optional_default}] else {arg_name})"
                    )
            yield f"{par_name}={arg_expression}"

    def _def_arguments(self, process: Process) -> Iterator[str]:
        # TODO: add argument type hints?
        for arg, param in zip(self._arg_names(process), process.parameters):
            if param.optional:
                yield "{a}={d}".format(a=arg, d=self.optional_default)
            elif param.has_default():
                yield "{a}={d!r}".format(a=arg, d=param.default)
            else:
                yield arg
        if self.oo_mode and len(process.parameters) == 0:
            yield "self"

    def render_docstring(self, process: Process, prefix="", width: int = DEFAULT_WIDTH) -> str:
        w = width - len(prefix)
        # TODO: use description instead of summary?
        doc = "\n\n".join(textwrap.fill(d, width=w) for d in process.summary.split("\n\n"))
        params = "\n".join(
            self._hanging_indent(":param {n}: {d}".format(n=arg, d=param.description), width=w)
            for arg, param in zip(self._arg_names(process), process.parameters)
        )
        returns = self._hanging_indent(":return: {d}".format(d=process.returns.description), width=w)
        return textwrap.indent('"""\n' + doc + "\n\n" + (params + "\n\n" + returns).strip() + '\n"""', prefix=prefix)

    def _hanging_indent(self, paragraph: str, indent="    ", width: int = DEFAULT_WIDTH) -> str:
        return textwrap.indent(textwrap.fill(paragraph, width=width - len(indent)), prefix=indent).lstrip()


def collect_processes(sources: List[Union[Path, str]]) -> List[Process]:
    processes = {}
    for src in [Path(s) for s in sources]:
        if src.is_dir():
            to_add = parse_all_from_dir(src)
        else:
            to_add = [Process.from_json_file(src)]
        for p in to_add:
            if p.id in processes:
                raise Exception(f"Duplicate source for process {p.id!r}")
            processes[p.id] = p
    return sorted(processes.values(), key=lambda p: p.id)


def generate_process_py(processes: List[Process], output=sys.stdout, argv=None):
    oo_src = textwrap.dedent(
        """
        from __future__ import annotations

        import builtins

        from openeo.internal.documentation import openeo_process
        from openeo.internal.processes.builder import UNSET, ProcessBuilderBase
        from openeo.rest._datacube import build_child_callback


        class ProcessBuilder(ProcessBuilderBase):
            \"\"\"
            .. include:: api-processbuilder.rst
            \"\"\"

            _ITERATION_LIMIT = 100

            @openeo_process(process_id="add", mode="operator")
            def __add__(self, other) -> ProcessBuilder:
                return self.add(other)

            @openeo_process(process_id="add", mode="operator")
            def __radd__(self, other) -> ProcessBuilder:
                return add(other, self)

            @openeo_process(process_id="subtract", mode="operator")
            def __sub__(self, other) -> ProcessBuilder:
                return self.subtract(other)

            @openeo_process(process_id="subtract", mode="operator")
            def __rsub__(self, other) -> ProcessBuilder:
                return subtract(other, self)

            @openeo_process(process_id="multiply", mode="operator")
            def __mul__(self, other) -> ProcessBuilder:
                return self.multiply(other)

            @openeo_process(process_id="multiply", mode="operator")
            def __rmul__(self, other) -> ProcessBuilder:
                return multiply(other, self)

            @openeo_process(process_id="divide", mode="operator")
            def __truediv__(self, other) -> ProcessBuilder:
                return self.divide(other)

            @openeo_process(process_id="divide", mode="operator")
            def __rtruediv__(self, other) -> ProcessBuilder:
                return divide(other, self)

            @openeo_process(process_id="multiply", mode="operator")
            def __neg__(self) -> ProcessBuilder:
                return self.multiply(-1)

            @openeo_process(process_id="power", mode="operator")
            def __pow__(self, other) -> ProcessBuilder:
                return self.power(other)

            @openeo_process(process_id="array_element", mode="operator")
            def __getitem__(self, key) -> ProcessBuilder:
                if isinstance(key, builtins.int):
                    if key > self._ITERATION_LIMIT:
                        raise RuntimeError(
                            "Exceeded ProcessBuilder iteration limit. "
                            "Are you mistakenly using a Python builtin like `sum()` or `all()` in a callback "
                            "instead of the appropriate helpers from the `openeo.processes` module?"
                        )
                    return self.array_element(index=key)
                else:
                    return self.array_element(label=key)

            @openeo_process(process_id="eq", mode="operator")
            def __eq__(self, other) -> ProcessBuilder:
                return eq(self, other)

            @openeo_process(process_id="neq", mode="operator")
            def __ne__(self, other) -> ProcessBuilder:
                return neq(self, other)

            @openeo_process(process_id="lt", mode="operator")
            def __lt__(self, other) -> ProcessBuilder:
                return lt(self, other)

            @openeo_process(process_id="lte", mode="operator")
            def __le__(self, other) -> ProcessBuilder:
                return lte(self, other)

            @openeo_process(process_id="ge", mode="operator")
            def __ge__(self, other) -> ProcessBuilder:
                return gte(self, other)

            @openeo_process(process_id="gt", mode="operator")
            def __gt__(self, other) -> ProcessBuilder:
                return gt(self, other)

    """
    )
    fun_src = textwrap.dedent(
        """
        # Public shortcut
        process = ProcessBuilder.process
        # Private shortcut that has lower chance to collide with a process argument named `process`
        _process = ProcessBuilder.process


    """
    )
    fun_renderer = PythonRenderer(
        body_template="return _process({id!r}, {args})",
        optional_default="UNSET",
        return_type_hint="ProcessBuilder",
        decorator="@openeo_process",
    )
    oo_renderer = PythonRenderer(
        oo_mode=True,
        body_template="return {safe_name}({args})",
        optional_default="UNSET",
        return_type_hint="ProcessBuilder",
        decorator="@openeo_process",
    )
    for p in processes:
        fun_src += fun_renderer.render_process(p) + "\n\n\n"
        oo_src += oo_renderer.render_process(p) + "\n\n"
    output.write(
        textwrap.dedent(
            """
        # Do not edit this file directly.
        # It is automatically generated.
    """
        )
    )
    if argv:
        output.write(
            textwrap.dedent(
                """\
            # Used command line arguments:
            #    {cli}
        """.format(
                    cli=" ".join(argv)
                )
            )
        )
    output.write(f"# Generated on {datetime.date.today().isoformat()}\n")

    output.write(oo_src)
    output.write(fun_src.rstrip() + "\n")


def main():
    # Usage example (from project root):
    #   # Update subrepos (with process specs)
    #   python specs/update-subrepos.py
    #   python openeo/internal/processes/generator.py  specs/openeo-processes specs/openeo-processes/proposals --output openeo/processes.py

    argv = sys.argv
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "source", nargs="+", help="""Source directories or files containing openEO process definitions in JSON format"""
    )
    arg_parser.add_argument("--output", help="Path to output 'processes.py' file")

    arguments = arg_parser.parse_args(argv[1:])
    sources = arguments.source
    output = arguments.output

    processes = collect_processes(sources)
    with open(output, "w", encoding="utf-8") if output else sys.stdout as f:
        generate_process_py(processes, output=f, argv=argv)


if __name__ == "__main__":
    main()
