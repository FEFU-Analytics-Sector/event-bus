import asyncio
import inspect
from asyncio import AbstractEventLoop, Task
from collections import defaultdict
from dataclasses import field, dataclass
from typing import Type

from event_bus.abstract.event import AbstractEvent
from ..abstract.event_bus import AbstractEventBus, EventHandler
from ..abstract.exceptions import EventNotSubscribed, EventHandlerNotFound


@dataclass
class InternalEventBus(AbstractEventBus):
    event_handlers: dict[
        Type[AbstractEvent],
        list[EventHandler]
    ] = field(
        default_factory=lambda: defaultdict(list)
    )
    event_loop: AbstractEventLoop = field(
        default_factory=asyncio.get_running_loop
    )
    active_handlers: list[Task] = field(
        default_factory=list
    )

    def publish(self, event: AbstractEvent) -> None:
        if type(event) not in self.event_handlers:
            return

        for handler in self.event_handlers[type(event)]:
            if inspect.iscoroutinefunction(handler):
                task = self.event_loop.create_task(handler(event))
            else:
                loop = asyncio.to_thread(handler, event)
                task = self.event_loop.create_task(loop)
            task.add_done_callback(self.active_handlers.remove)
            self.active_handlers.append(task)

    def subscribe(
        self,
        event_type: Type[AbstractEvent],
        handler: EventHandler
    ) -> None:
        self.event_handlers[event_type].append(handler)

    def unsubscribe(
        self,
        event_type: Type[AbstractEvent],
        handler: EventHandler
    ) -> None:
        if event_type not in self.event_handlers:
            raise EventNotSubscribed(event_type)

        handlers = self.event_handlers[event_type]

        if handler not in handlers:
            raise EventHandlerNotFound(event_type, handler)

        handlers.remove(handler)
        if len(handlers) == 0:
            self.event_handlers.pop(event_type)

    async def wait_for_handlers(self, timeout: int | None = None):
        if len(self.active_handlers) == 0:
            return

        await asyncio.wait(
            self.active_handlers,
            timeout=timeout,
            return_when=asyncio.ALL_COMPLETED
        )
