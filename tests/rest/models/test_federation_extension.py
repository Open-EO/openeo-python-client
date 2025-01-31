import pytest

from openeo.rest.models.federation_extension import (
    get_backend_details,
    get_federation_missing,
)


def test_get_backend_details():
    assert get_backend_details({}) is None
    assert get_backend_details(
        {
            "api_version": "1.2.0",
            "backend_version": "1.1.2",
            "stac_version": "1.0.0",
            "type": "Catalog",
            "id": "cool-eo-cloud",
            "endpoints": [
                {"path": "/collections", "methods": ["GET"]},
            ],
            "federation": {
                "eoa": {"title": "EO Answers", "url": "https://eoe.test/go"},
                "eob": {"title": "Beyond EO", "url": "https://eoeb.example.com"},
            },
        }
    ) == {
        "eoa": {"title": "EO Answers", "url": "https://eoe.test/go"},
        "eob": {"title": "Beyond EO", "url": "https://eoeb.example.com"},
    }


def test_get_federation_missing():
    assert get_federation_missing({}, resource_name="things") is None
    assert get_federation_missing(
        {
            "things": ["apple", "banana"],
            "federation:missing": ["veggies"],
        },
        resource_name="things",
    ) == ["veggies"]


@pytest.mark.parametrize(["auto_warn"], [[True], [False]])
def test_get_federation_missing_auto_warn(auto_warn, caplog):
    assert get_federation_missing(
        {
            "things": ["apple", "banana"],
            "federation:missing": ["veggies"],
        },
        resource_name="things",
        auto_warn=auto_warn,
    ) == ["veggies"]

    if auto_warn:
        assert "Partial things: missing federation components: ['veggies']." in caplog.text
    else:
        assert caplog.text == ""
