import pytest

from openeo.capabilities import ComparableVersion


@pytest.mark.parametrize(["a", "b", "c"], [
    (ComparableVersion("1.2.3"), ComparableVersion("1.2.3"), ComparableVersion("2.3.4")),
    (ComparableVersion("1.2.3"), "1.2.3", "2.3.4"),
    ("1.2.3", ComparableVersion("1.2.3"), ComparableVersion("2.3.4")),
])
def test_comparable_version_equals(a, b, c):
    assert (a == b) is True
    assert (a == c) is False
    assert (a != b) is False
    assert (a != c) is True
    if isinstance(a, ComparableVersion):
        assert a.equals(b) is True
        assert a.equals(c) is False


def test_comparable_version_int_parsing():
    assert ComparableVersion("1.20") > "1.2"
    assert ComparableVersion("1.20") > "1.3"
    assert ComparableVersion("1.20") == "1.20"
    assert ComparableVersion("1.20") < "1.21"


def test_comparable_version_string_parsing():
    assert ComparableVersion("1b2") > "1"
    assert ComparableVersion("1b2") > "1.b1"
    assert ComparableVersion("1b2") == "1.b.2"
    assert ComparableVersion("1b2") < "1b3"


def test_comparable_version_to_str():
    assert str(ComparableVersion("1.2.3")) == "1.2.3"
    assert str(ComparableVersion("1.b.3")) == "1.b.3"


@pytest.mark.parametrize("b", [
    "0.9", "1", "1.2.2",
    ComparableVersion("0.9"), ComparableVersion("1.1"),
])
def test_comparable_version_operators(b):
    a = ComparableVersion("1.2.3")
    assert (a == a) is True
    assert (a != a) is False
    assert (a > b) is True
    assert (a >= b) is True
    assert (a < b) is False
    assert (a <= b) is False
    assert (b < a) is True
    assert (b <= a) is True
    assert (b > a) is False
    assert (b >= a) is False


def test_comparable_version_right_referencing():
    v = ComparableVersion('1.2.3')
    assert v.equals('1.2.3')
    assert v.above('0')
    assert v.above('0.1')
    assert v.above('0.1.2')
    assert v.above('1.2')
    assert v.above('1.2.2')
    assert v.above('1.2.2b')
    assert v.above('1.2.3') is False
    assert v.above('1.2.20') is False
    assert v.above('1.2.4') is False
    assert v.above('1.10.4') is False
    assert v.at_least('0')
    assert v.at_least('1')
    assert v.at_least('1.1')
    assert v.at_least('1.10') is False
    assert v.at_least('1.2')
    assert v.at_least('1.02')
    assert v.at_least('1.2.2')
    assert v.at_least('1.2.3')
    assert v.at_least('1.2.3a') is False
    assert v.at_least('1.2.4') is False
    assert v.at_least('1.3') is False
    assert v.at_least('2') is False
    assert v.below('2')
    assert v.below('1.3')
    assert v.below('1.2.4')
    assert v.below('1.2.3b')
    assert v.below('1.2.3') is False
    assert v.below('1.2') is False
    assert v.at_most('2')
    assert v.at_most('1.3')
    assert v.at_most('1.2.3c')
    assert v.at_most('1.2.3')
    assert v.at_most('1.02.03')
    assert v.at_most('1.2.2b') is False
    assert v.at_most('1.2') is False
    assert v.at_most('1.10')

    assert v.above(ComparableVersion('1.2'))
    assert v.at_least(ComparableVersion('1.2.3a')) is False
    assert v.at_most(ComparableVersion('1.02.03'))


def test_comparable_version_left_referencing():
    v = ComparableVersion("1.2.3")
    assert v.or_higher("1.2.2") is False
    assert v.or_higher("1.2.3") is True
    assert v.or_higher("1.2.4") is True
    assert v.or_lower("1.2.2") is True
    assert v.or_lower("1.2.3") is True
    assert v.or_lower("1.2.4") is False
    assert v.accept_higher("1.2.2") is False
    assert v.accept_higher("1.2.3") is False
    assert v.accept_higher("1.2.4") is True
    assert v.accept_lower("1.2.2") is True
    assert v.accept_lower("1.2.3") is False
    assert v.accept_lower("1.2.4") is False
