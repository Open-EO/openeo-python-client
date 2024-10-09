import re
import sys

import pytest

from openeo.internal.warnings import UserDeprecationWarning, deprecated, legacy_alias


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

    do_plus = legacy_alias(add, "do_plus", since="v1.2")

    assert add.__doc__ == "Add x and y."
    assert do_plus.__doc__ == (
        "\n"
        ".. deprecated:: v1.2\n"
        "   Usage of this legacy function is deprecated. Use :py:func:`.add`\n"
        "   instead.\n"
    )

    assert add(2, 3) == 5
    assert len(recwarn) == 0

    with pytest.warns(
        UserDeprecationWarning,
        match=re.escape(
            "Call to deprecated function (or staticmethod) do_plus."
            " (Usage of this legacy function is deprecated. Use `.add` instead.)"
            " -- Deprecated since version v1.2."
        ),
    ):
        res = do_plus(2, 3)
    assert res == 5


def test_legacy_alias_method(recwarn):
    class Foo:
        def add(self, x, y):
            """Add x and y."""
            return x + y

        do_plus = legacy_alias(add, "do_plus", since="v1.2")

    assert Foo.add.__doc__ == "Add x and y."
    assert Foo.do_plus.__doc__ == (
        "\n"
        ".. deprecated:: v1.2\n"
        "   Usage of this legacy method is deprecated. Use :py:meth:`.add`\n"
        "   instead.\n"
    )

    assert Foo().add(2, 3) == 5
    assert len(recwarn) == 0

    with pytest.warns(
        UserDeprecationWarning,
        match=re.escape(
            "Call to deprecated method do_plus."
            " (Usage of this legacy method is deprecated. Use `.add` instead.)"
            " -- Deprecated since version v1.2."
        ),
    ):
        res = Foo().do_plus(2, 3)
    assert res == 5


def test_legacy_alias_classmethod(recwarn):
    class Foo:
        @classmethod
        def add(cls, x, y):
            """Add x and y."""
            assert cls is Foo
            return x + y

        do_plus = legacy_alias(add, "do_plus", since="v1.2")

    assert Foo.add.__doc__ == "Add x and y."
    assert Foo.do_plus.__doc__ == (
        "\n"
        ".. deprecated:: v1.2\n"
        "   Usage of this legacy class method is deprecated. Use\n"
        "   :py:meth:`.add` instead.\n"
    )

    assert Foo().add(2, 3) == 5
    assert len(recwarn) == 0

    expected_warning = re.escape(
        # Workaround for bug in classmethod detection before Python 3.9 (see https://wrapt.readthedocs.io/en/latest/decorators.html#decorating-class-methods
        f"Call to deprecated {'class method' if sys.version_info >= (3, 9) else 'function (or staticmethod)'} do_plus."
        " (Usage of this legacy class method is deprecated. Use `.add` instead.)"
        " -- Deprecated since version v1.2."
    )

    with pytest.warns(UserDeprecationWarning, match=expected_warning):
        res = Foo().do_plus(2, 3)
    assert res == 5

    with pytest.warns(UserDeprecationWarning, match=expected_warning):
        res = Foo.do_plus(2, 3)
    assert res == 5


def test_legacy_alias_staticmethod(recwarn):
    class Foo:
        @staticmethod
        def add(x, y):
            """Add x and y."""
            return x + y

        do_plus = legacy_alias(add, "do_plus", since="v1.2")

    assert Foo.add.__doc__ == "Add x and y."
    assert Foo.do_plus.__doc__ == (
        "\n"
        ".. deprecated:: v1.2\n"
        "   Usage of this legacy static method is deprecated. Use\n"
        "   :py:meth:`.add` instead.\n"
    )

    assert Foo().add(2, 3) == 5
    assert len(recwarn) == 0

    expected_warning = re.escape(
        "Call to deprecated function (or staticmethod) do_plus."
        " (Usage of this legacy static method is deprecated. Use `.add` instead.)"
        " -- Deprecated since version v1.2."
    )
    with pytest.warns(UserDeprecationWarning, match=expected_warning):
        res = Foo().do_plus(2, 3)
    assert res == 5

    with pytest.warns(UserDeprecationWarning, match=expected_warning):
        res = Foo.do_plus(2, 3)
    assert res == 5


def test_legacy_alias_method_soft(recwarn):
    class Foo:
        def add(self, x, y):
            """Add x and y."""
            return x + y

        do_plus = legacy_alias(add, name="do_plus", since="v1.2", mode="soft")

    assert Foo.add.__doc__ == "Add x and y."
    assert Foo.do_plus.__doc__ == (
        "Add x and y.\n"
        "\n"
        ".. deprecated:: v1.2\n"
        "   Usage of this legacy method is deprecated. Use :py:meth:`.add` instead.\n"
    )

    assert Foo().add(2, 3) == 5
    assert len(recwarn) == 0

    res = Foo().do_plus(2, 3)
    assert len(recwarn) == 0
    assert res == 5


def test_deprecated_decorator():
    class Foo:

        @deprecated("Use `add` instead", version="1.2.3")
        def plus1(self, x):
            return x + 1

    expected = "Call to deprecated method plus1. (Use `add` instead) -- Deprecated since version 1.2.3."
    with pytest.warns(UserDeprecationWarning, match=re.escape(expected)):
        assert Foo().plus1(2) == 3
