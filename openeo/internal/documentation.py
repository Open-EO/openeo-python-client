"""
Utilities to build/automate/extend documentation
"""

import collections
import inspect
import re
import textwrap
from functools import partial
from typing import Callable, Dict, List, Optional, Sequence, Tuple, TypeVar, Union

# TODO: give this a proper public API?
_process_registry = collections.defaultdict(list)


T = TypeVar("T", bound=Callable)


def openeo_process(f: Optional[T] = None, process_id: Optional[str] = None, mode: Optional[str] = None) -> T:
    """
    Decorator for function or method to associate it with a standard openEO process

    :param f: function or method
    :param process_id: openEO process_id (to be given when it can not be guessed from function name)
    :return:
    """
    # TODO: include openEO version?
    # TODO: support non-standard/proposed/experimental?
    # TODO: handling of `mode` (or something alike): apply/reduce_dimension/... callback, (band) math operator, ...?
    # TODO: documentation test that "seealso" urls are valid
    # TODO: inject more references/metadata in __doc__
    if f is None:
        # Parameterized decorator call
        return partial(openeo_process, process_id=process_id)

    process_id = process_id or f.__name__
    url = f"https://processes.openeo.org/#{process_id}"
    seealso = f'.. seealso::\n    openeo.org documentation on `process "{process_id}" <{url}>`_.'
    f.__doc__ = textwrap.dedent(f.__doc__ or "") + "\n\n" + seealso

    _process_registry[process_id].append((f, mode))
    return f


def openeo_endpoint(endpoint: str) -> Callable[[Callable], Callable]:
    """
    Parameterized decorator to annotate given function or method with the openEO endpoint it interacts with

    :param endpoint: REST endpoint (e.g. "GET /jobs", "POST /result", ...)
    :return:
    """
    # TODO: automatically parse/normalize endpoint (to method+path)
    # TODO: wrap this in some markup/directive to make this more a "small print" note.

    def decorate(f: Callable) -> Callable:
        is_method = list(inspect.signature(f).parameters.keys())[:1] == ["self"]
        seealso = f"This {'method' if is_method else 'function'} uses openEO endpoint ``{endpoint}``"
        f.__doc__ = textwrap.dedent(f.__doc__ or "") + "\n\n" + seealso + "\n"
        return f

    return decorate


def get_docstring(obj: Union[str, Callable]) -> str:
    """
    Get docstring of a method or function.
    """
    if isinstance(obj, str):
        doc = obj
    else:
        doc = obj.__doc__
    return textwrap.dedent(doc)


def extract_params(doc: Union[str, Callable]) -> Dict[str, str]:
    """
    Extract parameters (``:param name:`` format) from a docstring.
    """
    doc = get_docstring(doc)
    params_regex = re.compile(r"^:param\s+(?P<param>\w+)\s*:(?P<doc>.*(\n +.*)*)", re.MULTILINE)
    return {m.group("param"): m.group("doc").strip() for m in params_regex.finditer(doc)}


def extract_return(doc: Union[str, Callable]) -> Union[str, None]:
    """
    Extract return value description (``:return:`` format) from a docstring.
    """
    doc = get_docstring(doc)
    return_regex = re.compile(r"^:return\s*:(?P<doc>.*(\n +.*)*)", re.MULTILINE)
    matches = [m.group("doc").strip() for m in return_regex.finditer(doc)]
    assert 0 <= len(matches) <= 1
    return matches[0] if matches else None


def extract_main_description(doc: Union[str, Callable]) -> List[str]:
    """
    Extract main description from a docstring:
    paragraphs before the params/returns description.
    """
    paragraphs = []
    for part in re.split(r"\s*\n(?:\s*\n)+", get_docstring(doc)):
        if re.match(r"\s*:", part):
            break
        paragraphs.append(part.strip("\n"))
    assert len(paragraphs) > 0
    return paragraphs


def assert_same_param_docs(doc_a: Union[str, Callable], doc_b: Union[str, Callable], only_intersection: bool = False):
    """
    Compare parameters (``:param name:`` format) from two docstrings.
    """
    # TODO: option to also check order?
    params_a = extract_params(doc_a)
    params_b = extract_params(doc_b)

    if only_intersection:
        intersection = set(params_a.keys()).intersection(params_b.keys())
        params_a = {k: v for k, v in params_a.items() if k in intersection}
        params_b = {k: v for k, v in params_b.items() if k in intersection}

    assert params_a == params_b


def assert_same_return_docs(doc_a: Union[str, Callable], doc_b: Union[str, Callable]):
    """
    Compare return value descriptions from two docstrings.
    """
    assert extract_return(doc_a) == extract_return(doc_b)


def assert_same_main_description(doc_a: Union[str, Callable], doc_b: Union[str, Callable], ignore: Sequence[str] = ()):
    """
    Compare main description from two docstrings.
    """
    description_a = extract_main_description(doc_a)
    description_b = extract_main_description(doc_b)

    for s in ignore:
        description_a = [p.replace(s, "<IGNORED>") for p in description_a]
        description_b = [p.replace(s, "<IGNORED>") for p in description_b]

    assert description_a == description_b
