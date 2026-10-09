from openeo.metadata import _EmptyExtent


class TestEmptyExtent:

    def test_equality(self):
        assert _EmptyExtent() == _EmptyExtent()

    def test_none(self):
        assert _EmptyExtent() != None
        assert None != _EmptyExtent()
