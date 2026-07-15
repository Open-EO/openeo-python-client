import collections
import logging
from typing import Callable, Dict, List, Optional, Union

_log = logging.getLogger(__name__)

# type annotation aliases to make things self-documenting
EventName = str
EventHandler = Callable[..., None]


class EventBus:
    """
    Simple event bus implementation:
    allowing users to subscribe for events
    emitted from internals in some (sub)system.
    """

    def __init__(self):
        self._handlers: Dict[EventName, List[EventHandler]] = collections.defaultdict(list)

    def on(
        self, event: EventName, handler: Optional[EventHandler] = None
    ) -> Union[EventHandler, Callable[[EventHandler], EventHandler]]:
        """
        Register an event handler for a given event.
        Can also be used as decorator.
        """

        def register(handler: EventHandler):
            self._handlers[event].append(handler)
            return handler

        return register(handler=handler) if handler else register

    def emit(self, event: EventName, **kwargs) -> None:
        """
        Trigger an event and pass it to all registered handlers

        :param event: name of the event
        :param kwargs: payload to include in the event
        """
        for handler in self._handlers.get(event, []):
            try:
                handler(event=event, **kwargs)
            except Exception as exc:
                _log.exception(f"Error while handling {event=} with {handler=}: {exc=}")
