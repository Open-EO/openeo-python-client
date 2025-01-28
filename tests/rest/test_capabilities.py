from openeo.rest.capabilities import OpenEoCapabilities
from openeo.utils.version import ComparableVersion


class TestOpenEoCapabilities:

    def test_api_version(self):
        capabilities = OpenEoCapabilities(
            {
                "api_version": "1.2.3",
            }
        )
        assert capabilities.api_version() == "1.2.3"

    def test_api_version_check(self):
        capabilities = OpenEoCapabilities(
            {
                "api_version": "1.2.3",
            }
        )
        checker = capabilities.api_version_check
        assert isinstance(checker, ComparableVersion)
        assert checker == "1.2.3"
        assert checker > "1.2"
        assert checker < "1.5"

    def test_supports_endpoint(self):
        capabilities = OpenEoCapabilities(
            {
                "api_version": "1.2.3",
                "endpoints": [
                    {"path": "/collections", "methods": ["GET"]},
                    {"path": "/jobs", "methods": ["GET", "POST"]},
                ],
            }
        )
        assert capabilities.supports_endpoint(path="/collections", method="GET")
        assert not capabilities.supports_endpoint(path="/collections", method="POST")
        assert capabilities.supports_endpoint(path="/jobs", method="GET")
        assert capabilities.supports_endpoint(path="/jobs", method="POST")

    def test_currency(self):
        assert OpenEoCapabilities({}).currency() is None
        assert OpenEoCapabilities({"billing": None}).currency() is None
        assert OpenEoCapabilities({"billing": {"currency": "EUR"}}).currency() == "EUR"

    def test_list_plans(self):
        assert OpenEoCapabilities({}).list_plans() == []
        assert OpenEoCapabilities({"billing": None}).list_plans() == []
        assert OpenEoCapabilities({"billing": {"plans": []}}).list_plans() == []
        assert OpenEoCapabilities({"billing": {"plans": [{"name": "free"}]}}).list_plans() == [{"name": "free"}]

    def test_federation_absent(self):
        assert OpenEoCapabilities({}).get_federation() is None

    def test_federation_present(self):
        data = {
            "api_version": "1.2.3",
            "federation": {
                "a": {"url": "https://a.test/openeo/v2", "title": "A backend"},
                "bb": {"url": "https://openeo.b.test/v9"},
            },
        }
        capabilities = OpenEoCapabilities(data)
        assert capabilities.get_federation() == {
            "a": {"url": "https://a.test/openeo/v2", "title": "A backend"},
            "bb": {"url": "https://openeo.b.test/v9"},
        }
