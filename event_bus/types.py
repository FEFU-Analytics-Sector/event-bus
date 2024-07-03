from typing import Awaitable, Callable

from event_bus.event import Event

EventHandler = Callable[[Event], Awaitable[None] | None]
