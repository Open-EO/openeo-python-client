import pytest

from openeo.metadata import NumericalInterval, _EmptyExtent


class TestEmptyExtent:

    def test_equality(self):
        assert _EmptyExtent() == _EmptyExtent()

    def test_none(self):
        assert _EmptyExtent() != None
        assert None != _EmptyExtent()


class TestNumericalInterval:
    def test_basic(self):
        interval = NumericalInterval(3, 5.5)
        assert interval.start == 3
        assert interval.end == 5.5

    @pytest.mark.parametrize(["start", "end"], [(None, 5), (3, None), (None, None)])
    def test_unbounded(self, start, end):
        interval = NumericalInterval(start, end)
        assert interval.start == start
        assert interval.end == end

    def test_equality(self):
        assert NumericalInterval(1, 2) == NumericalInterval(1, 2)
        assert NumericalInterval(1, 2) != NumericalInterval(1, 3)
        assert NumericalInterval(1, 2) != NumericalInterval(0, 2)
        assert NumericalInterval(None, 2) != NumericalInterval(0, 2)
        assert NumericalInterval(None, None) == NumericalInterval(None, None)

    def test_try_from(self):
        assert NumericalInterval.try_from([1, 2]) == NumericalInterval(1, 2)
        assert NumericalInterval.try_from((None, 5)) == NumericalInterval(None, 5)
        assert NumericalInterval.try_from([3.5, None]) == NumericalInterval(3.5, None)
        assert NumericalInterval.try_from([None, None]) == NumericalInterval(None, None)
        assert NumericalInterval.try_from([1]) is None
        assert NumericalInterval.try_from([1, 2, 3]) is None
        assert NumericalInterval.try_from("not a list") is None
        assert NumericalInterval.try_from([1, "not a number"]) is None

    @pytest.mark.parametrize(
        ["interval1", "interval2", "expected"],
        [
            # Closed interval cases
            ((3, 5), (1, 2), _EmptyExtent()),
            ((3, 5), (1, 3.5), NumericalInterval(3, 3.5)),
            ((3, 5), (3.5, 4.5), NumericalInterval(3.5, 4.5)),
            ((3, 5), (4.5, 8), NumericalInterval(4.5, 5)),
            ((3, 5), (6, 8), _EmptyExtent()),
            # Half-open interval cases
            ((None, 5), (1, 2), NumericalInterval(1, 2)),
            ((None, 5), (1, 7), NumericalInterval(1, 5)),
            ((None, 5), (6, 7), _EmptyExtent()),
            ((3, None), (1, 2), _EmptyExtent()),
            ((3, None), (1, 4), NumericalInterval(3, 4)),
            ((3, None), (5, 7), NumericalInterval(5, 7)),
            # Unbounded interval cases
            ((None, None), (1, 2), NumericalInterval(1, 2)),
            ((None, None), (None, 2), NumericalInterval(None, 2)),
            ((None, None), (3, None), NumericalInterval(3, None)),
            ((None, None), (None, None), NumericalInterval(None, None)),
        ],
    )
    def test_intersection(self, interval1, interval2, expected):
        interval1 = NumericalInterval(*interval1)
        interval2 = NumericalInterval(*interval2)
        assert interval1.intersection(interval2) == expected
        assert interval2.intersection(interval1) == expected
