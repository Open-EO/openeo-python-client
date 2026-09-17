from openeo.utils.datastructure import make_new_key


def test_make_new_key_on_dict():
    d = {"href": "https://example.com", "name": "example"}
    assert make_new_key(d, "description") == "description"
    assert make_new_key(d, "name") == "name-1"
    assert d == {"href": "https://example.com", "name": "example"}

    assert make_new_key(d, "description") == "description"
    assert make_new_key(d, "name") == "name-1"

    d[make_new_key(d, "name")] = "other"
    assert d == {"href": "https://example.com", "name": "example", "name-1": "other"}
