from typing import Iterable

import pytest

from openeo.utils.normalize import normalize_resample_resolution, unique


@pytest.mark.parametrize(
    ["resolution", "expected"],
    [
        (1, (1, 1)),
        (1.23, (1.23, 1.23)),
        ([1, 2], (1, 2)),
        ((1.23, 2.34), (1.23, 2.34)),
    ],
)
def test_normalize_resample_resolution(resolution, expected):
    assert normalize_resample_resolution(resolution) == expected


@pytest.mark.parametrize(
    "resolution",
    [
        "0123",
        [1, 2, 3],
        {"x": 2, "y": 5},
    ],
)
def test_normalize_resample_resolution(resolution):
    with pytest.raises(ValueError, match="Invalid resolution"):
        normalize_resample_resolution(resolution)


@pytest.mark.parametrize(
    "input, expected",
    [
        ([], []),
        (["foo"], ["foo"]),
        ("foo", ["f", "o"]),
        ([1, 2, 2, 3, 1, 2, 3, 1, 4], [1, 2, 3, 4]),
        (["a", "b", "a", "c"], ["a", "b", "c"]),
        ([(1, 2), (1, 2), (2, 3)], [(1, 2), (2, 3)]),
        ([1, "a", "1", "a"], [1, "a", "1"]),
        ((x for x in [1, 2, 2, 3]), [1, 2, 3]),
        (range(5), [0, 1, 2, 3, 4]),
        (iter("hello"), ["h", "e", "l", "o"]),
    ],
)
def test_unique(input, expected):
    actual = unique(input)
    assert isinstance(actual, Iterable)
    assert list(actual) == expected


@pytest.mark.parametrize(
    ["input", "key", "expected"],
    [
        (["apple", "banana", "Apple", "Banana"], None, ["apple", "banana", "Apple", "Banana"]),
        (["apple", "banana", "Apple", "Banana"], lambda x: x.lower(), ["apple", "banana"]),
        ([(1, 2), (2, 1), (2, 3)], sum, [(1, 2), (2, 3)]),
        ([(1, 2), (2, 1), (2, 3)], lambda x: x[0], [(1, 2), (2, 1)]),
        ([(1, 2), (2, 1), (2, 3)], lambda x: x[1], [(1, 2), (2, 1), (2, 3)]),
    ],
)
def test_unique_with_key(input, key, expected):
    actual = unique(input, key=key)
    assert isinstance(actual, Iterable)
    assert list(actual) == expected
