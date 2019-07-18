import pytest

from openeo.util import first_not_none, get_temporal_extent


@pytest.mark.parametrize(['input', 'expected'], [
    ((123,), 123),
    ((1, 2, 3), 1),
    ((0, 2, 3), 0),
    ((0.0, 2, 3), 0.0),
    ((None, 2, 3), 2),
    ((None, 'foo', 3), 'foo'),
    ((None, None, 3), 3),
    ((None, [], [1, 2]), []),
])
def test_first_not_none(input, expected):
    assert expected == first_not_none(*input)


def test_first_not_none_failures():
    with pytest.raises(ValueError):
        first_not_none()
    with pytest.raises(ValueError):
        first_not_none(None)
    with pytest.raises(ValueError):
        first_not_none(None, None)


def test_get_temporal_extent():
    assert get_temporal_extent("2019-03-15") == ("2019-03-15", None)
    assert get_temporal_extent("2019-03-15", "2019-10-11") == ("2019-03-15", "2019-10-11")
    assert get_temporal_extent(["2019-03-15", "2019-10-11"]) == ("2019-03-15", "2019-10-11")
    assert get_temporal_extent(("2019-03-15", "2019-10-11")) == ("2019-03-15", "2019-10-11")
    assert get_temporal_extent(extent=["2019-03-15", "2019-10-11"]) == ("2019-03-15", "2019-10-11")
    assert get_temporal_extent(extent=("2019-03-15", "2019-10-11")) == ("2019-03-15", "2019-10-11")
    assert get_temporal_extent(extent=(None, "2019-10-11")) == (None, "2019-10-11")
    assert get_temporal_extent(extent=("2019-03-15", None)) == ("2019-03-15", None)
    assert get_temporal_extent(start_date="2019-03-15", end_date="2019-10-11") == ("2019-03-15", "2019-10-11")
    assert get_temporal_extent(start_date="2019-03-15") == ("2019-03-15", None)
    assert get_temporal_extent(end_date="2019-10-11") == (None, "2019-10-11")
