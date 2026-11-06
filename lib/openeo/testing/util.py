from __future__ import annotations

import contextlib
import time
import typing
from unittest import mock

import time_machine


class Sleeper:
    """
    Helper to create a `fast_sleep` fixture that makes "time.sleep"
    calls (almost) immediate while updating the current time
    (e.g. `time.time()`) accordingly

    Usage: add this fixture definition to your conftest.py:

        @pytest.fixture
        def fast_sleep(time_machine) -> typing.Iterator[Sleeper]:
            with Sleeper().patch(time_machine=time_machine) as sleeper:
                yield sleeper
    """

    def __init__(self):
        self.history = []

    @contextlib.contextmanager
    def patch(self, time_machine: time_machine.TimeMachineFixture) -> typing.Iterator[Sleeper]:
        orig_sleep = time.sleep

        def sleep(seconds):
            time_machine.shift(seconds)
            self.history.append(seconds)
            # At least do some minimal sleeping to avoid messing up
            # Python internals or third party logic
            # that depend on actual sleeping
            orig_sleep(min(seconds, 0.1))

        with mock.patch("time.sleep", new=sleep):
            yield self

    def did_sleep(self) -> bool:
        return len(self.history) > 0
