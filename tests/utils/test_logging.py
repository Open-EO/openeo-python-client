import pytest

from openeo.utils.logging import get_url_query_param_stripper, strip_url_query_params


@pytest.mark.parametrize(
    ["url", "expected"],
    [
        ("https://example.com/path", "https://example.com/path"),
        ("https://example.com/path?foo=bar", "https://example.com/path?-redacted-"),
        ("https://example.com/path?foo=bar&baz=qux", "https://example.com/path?-redacted-"),
        ("https://example.com/path?foo=bar&baz=qux#content", "https://example.com/path?-redacted-"),
        ("https://example.com/path#content", "https://example.com/path"),
        ("https://example.com/", "https://example.com/"),
        ("https://example.com", "https://example.com"),
        ("https://example.com?foo=bar", "https://example.com?-redacted-"),
        ("example.com/foo?ba=r", "example.com/foo?-redacted-"),
    ],
)
def test_strip_url_query_basic(url, expected):
    assert strip_url_query_params(url) == expected


@pytest.mark.parametrize(
    ["data", "expected"],
    [
        (None, None),
        (123, 123),
        ("hello world", "hello world"),
        ({"hello": "world"}, {"hello": "world"}),
        ({"url": "https://example.com/?foo=bar"}, {"url": "https://example.com/?-redacted-"}),
        (["foo", "https://example.com/?foo=bar"], ["foo", "https://example.com/?-redacted-"]),
        (
            {"nested": {"urls": ["https://example.com/?foo=bar"]}, "hello": {"name": "world", "size": 5}},
            {"nested": {"urls": ["https://example.com/?-redacted-"]}, "hello": {"name": "world", "size": 5}},
        ),
    ],
)
def test_strip_url_query_data_structs(data, expected):
    assert strip_url_query_params(data) == expected


def test_strip_url_query_replacement():
    assert strip_url_query_params("https://example.com/?foo=bar") == "https://example.com/?-redacted-"
    assert strip_url_query_params("https://example.com/?foo=bar", replacement="...") == "https://example.com/?..."
    assert strip_url_query_params("https://example.com/?foo=bar", replacement="") == "https://example.com/?"
    assert strip_url_query_params("https://example.com/?foo=bar", replacement=None) == "https://example.com/"

    # Test nesting too
    assert strip_url_query_params(
        {"links": [{"rel": "about", "href": "https://example.com/?foo=bar"}]}, replacement="..."
    ) == {"links": [{"rel": "about", "href": "https://example.com/?..."}]}


def test_get_url_query_param_stripper():
    assert get_url_query_param_stripper(True)("https://example.com/?foo=bar") == "https://example.com/?-redacted-"
    assert get_url_query_param_stripper(False)("https://example.com/?foo=bar") == "https://example.com/?foo=bar"
    assert get_url_query_param_stripper("...")("https://example.com/?foo=bar") == "https://example.com/?..."
    assert (
        get_url_query_param_stripper(lambda x: x.upper())("https://example.com/?foo=bar")
        == "HTTPS://EXAMPLE.COM/?FOO=BAR"
    )
