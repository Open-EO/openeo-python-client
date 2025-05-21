import json
import re
from pathlib import Path

import pytest

from openeo.testing.io import TestDataLoader


class TestTestDataLoader:
    def test_get_path(self, tmp_path):
        path = tmp_path / "hello" / "world.txt"
        loader = TestDataLoader(root=tmp_path)
        assert loader.get_path("hello/world.txt") == path
        assert loader.get_path(Path("hello/world.txt")) == path

    def test_load_bytes(self, tmp_path):
        path = tmp_path / "hello" / "world.txt"
        path.parent.mkdir(parents=True)
        path.write_bytes(b"Hello W\x00rld")

        loader = TestDataLoader(root=tmp_path)
        assert loader.load_bytes("hello/world.txt") == b"Hello W\x00rld"

    @pytest.mark.parametrize(
        ["preprocess", "expected"],
        [
            (None, "Hello, World!"),
            (lambda s: s.lower(), "hello, world!"),
            (lambda s: s.replace("World", "Earth"), "Hello, Earth!"),
            ({"World": "Earth"}, "Hello, Earth!"),
            ({"Hello": "Greetings", "World": "Terra"}, "Greetings, Terra!"),
            ({re.compile("l+"): "|_"}, "He|_o, Wor|_d!"),
            ({re.compile("([A-Z])"): r"\1\1\1"}, "HHHello, WWWorld!"),
        ],
    )
    def test_load_text(self, tmp_path, preprocess, expected):
        (tmp_path / "hello.txt").write_text("Hello, World!", encoding="utf8")

        loader = TestDataLoader(root=tmp_path)
        assert loader.load_text("hello.txt", preprocess=preprocess) == expected

    @pytest.mark.parametrize(
        ["preprocess", "expected"],
        [
            (None, {"salutation": "Hello", "target": "World"}),
            (lambda s: s.upper(), {"SALUTATION": "HELLO", "TARGET": "WORLD"}),
            (
                lambda s: s.replace("World", "Terra"),
                {"salutation": "Hello", "target": "Terra"},
            ),
            (
                lambda s: s.replace('"World"', '["Terra","Earth"]'),
                {"salutation": "Hello", "target": ["Terra", "Earth"]},
            ),
            (
                {"World": "Earth", "salutation": "say", "target": "to"},
                {"say": "Hello", "to": "Earth"},
            ),
            (
                {"Hello": "Greetings", '"World"': '["Terra","Earth"]'},
                {"salutation": "Greetings", "target": ["Terra", "Earth"]},
            ),
            (
                {re.compile("([aeoiu])"): r"\1\1\1"},
                {"saaaluuutaaatiiiooon": "Heeellooo", "taaargeeet": "Wooorld"},
            ),
        ],
    )
    def test_load_json(self, tmp_path, preprocess, expected):
        with (tmp_path / "data.json").open("w", encoding="utf8") as f:
            json.dump({"salutation": "Hello", "target": "World"}, f)

        loader = TestDataLoader(root=tmp_path)
        assert loader.load_json("data.json", preprocess=preprocess) == expected
