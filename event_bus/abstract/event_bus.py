from abc import ABC, abstractmethod
from functools import wraps
from typing import Callable, Awaitable, Type

from event_bus.abstract.event import AbstractEvent


EventHandler = Callable[[AbstractEvent], Awaitable[None] | None]

class AbstractEventBus(ABC):
    def on(self, event_name: Type[AbstractEvent]):
        def wrapper(handler):
            self.subscribe(event_name, handler)
            @wraps(handler)
            def wrapped(*args, **kwargs):
                return handler(*args, **kwargs)
            return wrapped
        return wrapper

    @abstractmethod
    async def publish(self, event: AbstractEvent) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def subscribe(
        self,
        event_type: Type[AbstractEvent],
        handler: EventHandler
    ) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def unsubscribe(
        self,
        event_type: Type[AbstractEvent],
        handler: EventHandler
    ) -> None:
        raise NotImplementedError()
