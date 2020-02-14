from openeo.internal.graphbuilder import GraphBuilder, FlatGraphKeyGenerator


def test_flat_graph_key_generate():
    g = FlatGraphKeyGenerator()
    assert g.generate("foo") == "foo1"
    assert g.generate("foo") == "foo2"
    assert g.generate("bar") == "bar1"
    assert g.generate("foo") == "foo3"
