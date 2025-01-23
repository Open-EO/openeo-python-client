import pytest

from openeo.utils.normalize import normalize_resample_resolution


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
