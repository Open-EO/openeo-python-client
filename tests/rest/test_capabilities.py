from openeo.rest.rest_capabilities import RESTCapabilities


class TestCapabilities:
    def test_currency(self):
        assert RESTCapabilities({}).currency() is None
        assert RESTCapabilities({"billing": None}).currency() is None
        assert RESTCapabilities({"billing": {"currency": "EUR"}}).currency() == "EUR"

    def test_list_plans(self):
        assert RESTCapabilities({}).list_plans() == []
        assert RESTCapabilities({"billing": None}).list_plans() == []
        assert RESTCapabilities({"billing": {"plans": []}}).list_plans() == []
        assert RESTCapabilities(
            {"billing": {"plans": [{"name": "free"}]}}
        ).list_plans() == [{"name": "free"}]
