import textwrap

import pytest

from openeo.udf._compat import FlimsyTomlParser


class TestFlimsyTomlLib:
    @pytest.mark.parametrize(
        ["value", "expected"],
        [
            # Numbers
            ("123", 123),
            ("12.5", 12.5),
            # Strings
            ('"Basic string"', "Basic string"),
            ("'Literal string'", "Literal string"),
            ('''"I'm a string"''', "I'm a string"),
            (r'''"You can \"quote\" me"''', 'You can "quote" me'),
            # Arrays (aka lists)
            ("[]", []),
            ("[1, 2, 3]", [1, 2, 3]),
            ("[1.5, 2.5, 3.5]", [1.5, 2.5, 3.5]),
            ("[1, 2, 3,]", [1, 2, 3]),
            ("[\n  1,\n  2,\n 3,\n]", [1, 2, 3]),
            ('["blue", "yellow"]', ["blue", "yellow"]),
            ("['blue', 'yellow']", ["blue", "yellow"]),
            (
                """
                [
                    "blue",
                    "yellow",
                ]
                """,
                ["blue", "yellow"],
            ),
            ("[1, 'two', 3.0, \"four\"]", [1, "two", 3.0, "four"]),
            (
                """
                [
                    'one',
                    [2, 3],
                ]
                """,
                ["one", [2, 3]],
            ),
        ],
    )
    def test_parse_toml_value_like_json(self, value, expected):
        assert FlimsyTomlParser._parse_toml_value_like_json(value) == expected

    def test_loads_empty(self):
        assert FlimsyTomlParser.loads("") == {}

    def test_loads_basic(self):
        data = textwrap.dedent(
            """
            title = "TOML Example"
            colors = ["blue", "yellow"]
            size = 132
            """
        )
        assert FlimsyTomlParser.loads(data) == {
            "title": "TOML Example",
            "colors": ["blue", "yellow"],
            "size": 132,
        }

    def test_loads_multiline_values(self):
        """Test multiline values, including trailing commas and comments."""
        data = textwrap.dedent(
            """
            # Some colors
            colors = [
                "blue",  # The sky is blue
                "yellow",  # Look ma a trailing comma
            ]
            sizes = [
                12,
                34,
                # This closing bracket is intentionally indented too
                ]
            shape = "round"
            """
        )
        assert FlimsyTomlParser.loads(data) == {
            "colors": ["blue", "yellow"],
            "sizes": [12, 34],
            "shape": "round",
        }

    def test_loads_special_keys(self):
        data = textwrap.dedent(
            """
            1234 = "one two three four"
            bare_key = "underscore"
            another-key = "dash"
            """
        )
        assert FlimsyTomlParser.loads(data) == {
            "1234": "one two three four",
            "another-key": "dash",
            "bare_key": "underscore",
        }

    def test_loads_tables(self):
        data = textwrap.dedent(
            """
            title = "Vroom"
            [car]
            brand = "HobbleBlob"
            """
        )
        with pytest.raises(FlimsyTomlParser.TomlParseError, match="Tables are not supported"):
            _ = FlimsyTomlParser.loads(data)

    def test_loads_dotted_keys(self):
        data = textwrap.dedent(
            """
            title = "Vroom"
            car.brand = "HobbleBlob"
            """
        )
        with pytest.raises(FlimsyTomlParser.TomlParseError, match="Dotted keys are not supported"):
            _ = FlimsyTomlParser.loads(data)
