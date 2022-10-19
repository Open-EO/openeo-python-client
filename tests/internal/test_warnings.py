import re

import pytest

from openeo.internal.warnings import legacy_alias, UserDeprecationWarning, deprecated


def test_user_deprecation_warning(pytester):
    pytester.makepyfile(myscript="""
        from openeo.internal.warnings import test_warnings
        test_warnings()
        test_warnings(2)
    """)

    result = pytester.runpython("myscript.py")
    stderr = "\n".join(result.errlines)
    assert "This is a UserDeprecationWarning (stacklevel 1)" in stderr
    assert "myscript.py:3: UserDeprecationWarning: This is a UserDeprecationWarning (stacklevel 2)" in stderr


def test_legacy_alias_function(recwarn):
    def add(x, y):
        """Add x and y."""
        return x + y

    do_plus = legacy_alias(add, "do_plus")

    assert add.__doc__ == "Add x and y."
    assert do_plus.__doc__ == "Use of this legacy function is deprecated, use :py:func:`.add` instead."

    assert add(2, 3) == 5
    assert len(recwarn) == 0

    with pytest.warns(UserDeprecationWarning, match="Call to deprecated function `do_plus`, use `add` instead."):
        res = do_plus(2, 3)
    assert res == 5


def test_legacy_alias_method(recwarn):
    class Foo:
        def add(self, x, y):
            """Add x and y."""
            return x + y

        do_plus = legacy_alias(add, "do_plus")

    assert Foo.add.__doc__ == "Add x and y."
    assert Foo.do_plus.__doc__ == "Use of this legacy method is deprecated, use :py:meth:`.add` instead."

    assert Foo().add(2, 3) == 5
    assert len(recwarn) == 0

    with pytest.warns(UserDeprecationWarning, match="Call to deprecated method `do_plus`, use `add` instead."):
        res = Foo().do_plus(2, 3)
    assert res == 5


def test_legacy_alias_classmethod(recwarn):
    class Foo:
        @classmethod
        def add(cls, x, y):
            """Add x and y."""
            assert cls is Foo
            return x + y

        do_plus = legacy_alias(add, "do_plus")

    assert Foo.add.__doc__ == "Add x and y."
    assert Foo.do_plus.__doc__ == "Use of this legacy class method is deprecated, use :py:meth:`.add` instead."

    assert Foo().add(2, 3) == 5
    assert len(recwarn) == 0

    with pytest.warns(UserDeprecationWarning, match="Call to deprecated class method `do_plus`, use `add` instead."):
        res = Foo().do_plus(2, 3)
    assert res == 5


def test_legacy_alias_staticmethod(recwarn):
    class Foo:
        @staticmethod
        def add(x, y):
            """Add x and y."""
            return x + y

        do_plus = legacy_alias(add, "do_plus")

    assert Foo.add.__doc__ == "Add x and y."
    assert Foo.do_plus.__doc__ == "Use of this legacy static method is deprecated, use :py:meth:`.add` instead."

    assert Foo().add(2, 3) == 5
    assert len(recwarn) == 0

    with pytest.warns(UserDeprecationWarning, match="Call to deprecated static method `do_plus`, use `add` instead."):
        res = Foo().do_plus(2, 3)
    assert res == 5


def test_deprecated_decorator():
    class Foo:

        @deprecated("Use `add` instead", version="1.2.3")
        def plus1(self, x):
            return x + 1

    expected = "Call to deprecated method plus1. (Use `add` instead) -- Deprecated since version 1.2.3."
    with pytest.warns(UserDeprecationWarning, match=re.escape(expected)):
        assert Foo().plus1(2) == 3
