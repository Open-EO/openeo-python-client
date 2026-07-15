from openeo.utils.events import EventBus


class TestEventBus:
    def test_basic(self):
        event_bus = EventBus()

        history = []

        def handler(event: str, **kwargs):
            history.append((event, kwargs))

        event_bus.on("event1", handler)

        event_bus.emit("event1")
        event_bus.emit("event2")
        assert history == [
            ("event1", {}),
        ]

        event_bus.emit("event1", extra="cheese")
        assert history == [
            ("event1", {}),
            ("event1", {"extra": "cheese"}),
        ]

    def test_decorator(self):
        event_bus = EventBus()

        history = []

        @event_bus.on("event1")
        def handler(event: str, **kwargs):
            history.append((event, kwargs))

        event_bus.emit("event1")
        event_bus.emit("event2")
        assert history == [
            ("event1", {}),
        ]

        event_bus.emit("event1", extra="cheese")
        assert history == [
            ("event1", {}),
            ("event1", {"extra": "cheese"}),
        ]

    def test_resilience(self, caplog):
        event_bus = EventBus()

        speeds = []

        @event_bus.on("event1")
        def handler(event: str, distance: float, time: float):
            speeds.append(distance / time)

        event_bus.emit("event1", distance=100, time=5)
        assert speeds == [20.0]

        # Trigger ZeroDivisionError
        event_bus.emit("event1", distance=100, time=0)
        assert speeds == [20.0]
        assert "Error while handling event='event1'" in caplog.text
        assert "ZeroDivisionError" in caplog.text

        caplog.clear()

        # Argument mismatch
        event_bus.emit("event1", farness=10, waitingness=2)
        assert speeds == [20.0]
        assert "Error while handling event='event1'" in caplog.text
        assert "unexpected keyword argument" in caplog.text

        # No worries, still working
        event_bus.emit("event1", distance=10, time=2)
        assert speeds == [20.0, 5.0]
