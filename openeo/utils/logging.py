import urllib.parse
from typing import Any, Callable, Optional, Union


def strip_url_query_params(data: Any, *, replacement: Optional[str] = "-redacted-") -> Any:
    """
    (Recursively) strip query parameters from URLs for logging purposes:
    better signal/noise ratio, and reduce risk on leaking signatures or tokens.
    """
    if isinstance(data, str):
        parsed = urllib.parse.urlsplit(data)
        stripped = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, None, None))
        if parsed.query and replacement is not None:
            stripped += f"?{replacement}"
        return stripped
    elif isinstance(data, dict):
        return {k: strip_url_query_params(v, replacement=replacement) for k, v in data.items()}
    elif isinstance(data, list):
        return [strip_url_query_params(v, replacement=replacement) for v in data]
    else:
        return data


def get_url_query_param_stripper(value: Union[bool, Callable, str]) -> Callable:
    if value is True:
        return strip_url_query_params
    elif value is False:
        return lambda x: x
    elif callable(value):
        return value
    elif isinstance(value, str):
        return lambda x: strip_url_query_params(x, replacement=value)
    else:
        raise ValueError(f"Invalid value for url_query_param_stripper: {value}")
