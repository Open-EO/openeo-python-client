from openeo.capabilities import ComparableVersion


def test_checkable_version():
    v = ComparableVersion('1.2.3')
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
