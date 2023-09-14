from typing import Callable, Awaitable, Type

from event_bus.abstract.event_bus import AbstractEvent


class EventNotSubscribed(Exception):
    def __init__(self, event_type: Type[AbstractEvent]):
        super().__init__(f"Event {event_type} not subscribed")


class EventHandlerNotFound(Exception):
    def __init__(
        self,
        event_type: Type[AbstractEvent],
        handler: Callable[[AbstractEvent], Awaitable[None]]
    ):
        super().__init__(f"Event {event_type} doesn't have handler {handler}")
