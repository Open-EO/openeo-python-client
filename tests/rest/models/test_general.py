import pytest

from openeo.rest.models.general import CollectionListingResponse, Link


class TestLink:
    def test_basic(self):
        link = Link(rel="about", href="https://example.com/about")
        assert link.rel == "about"
        assert link.href == "https://example.com/about"
        assert link.title is None
        assert link.type is None

    def test_full(self):
        link = Link(rel="about", href="https://example.com/about", type="text/html", title="About example")
        assert link.rel == "about"
        assert link.href == "https://example.com/about"
        assert link.title == "About example"
        assert link.type == "text/html"

    def test_repr(self):
        link = Link(rel="about", href="https://example.com/about")
        assert repr(link) == "Link(rel='about', href='https://example.com/about', type=None, title=None)"


class TestCollectionListingResponse:
    def test_basic(self):
        data = {"collections": [{"id": "S2"}, {"id": "S3"}]}
        collections = CollectionListingResponse(data)
        assert collections == [{"id": "S2"}, {"id": "S3"}]
        assert repr(collections) == "[{'id': 'S2'}, {'id': 'S3'}]"

    def test_links(self):
        data = {
            "collections": [{"id": "S2"}, {"id": "S3"}],
            "links": [
                {"rel": "self", "href": "https://openeo.test/collections"},
                {"rel": "next", "href": "https://openeo.test/collections?page=2"},
            ],
        }
        collections = CollectionListingResponse(data)
        assert collections.links == [
            Link(rel="self", href="https://openeo.test/collections"),
            Link(rel="next", href="https://openeo.test/collections?page=2"),
        ]

    @pytest.mark.parametrize(
        ["data", "expected"],
        [
            (
                {"collections": [{"id": "S2"}], "federation:missing": ["wwu"]},
                ["wwu"],
            ),
            (
                {"collections": [{"id": "S2"}]},
                None,
            ),
        ],
    )
    def test_federation_missing(self, data, expected):
        collections = CollectionListingResponse(data)
        assert collections.ext_federation.missing == expected
