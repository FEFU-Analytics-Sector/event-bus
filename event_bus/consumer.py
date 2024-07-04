import asyncio
import json
import logging
from dataclasses import field, dataclass
from typing import Type

import aio_pika
from aio_pika.abc import (
    AbstractRobustChannel,
    AbstractIncomingMessage,
    AbstractRobustConnection,
    AbstractRobustQueue,
)

from event_bus.event_infrastructure_declarator import EventInfrastructureDeclarator
from event_bus.retry_policy import RetryPolicy
from event_bus.event import Event
from event_bus.types import EventHandler

@dataclass
class RabbitMQEventConsumer:
    amqp_connection_url: str = field(
        init=True
    )
    event_bus_exchange: str = field(
        init=True
    )
    name: str = field(
        init=True
    )

    connection: AbstractRobustConnection = field(
        init=False
    )
    channel: AbstractRobustChannel = field(
        init=False
    )
    event_handlers: dict[Type[Event], EventHandler] = field(
        default_factory=dict,
        init=False
    )
    queues: dict[Type[Event], AbstractRobustQueue] = field(
        init=False,
        default_factory=dict
    )

    def __post_init__(self):
        self.logger = logging.getLogger(f"event_bus.consumer.{self.name}")

    async def connect(
        self,
        *,
        prefetch_count: int = 1
    ):
        self.connection = await aio_pika.connect_robust(
            self.amqp_connection_url
        )
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=prefetch_count)

    async def subscribe(
        self,
        event_type: Type[Event],
        handler: EventHandler,
        *,
        retry_policy: RetryPolicy | None = None
    ):
        self.event_handlers[event_type] = handler
        declarator = EventInfrastructureDeclarator(
            channel=self.channel,
            event_type=event_type,
            consumer_name=self.name,
            retry_policy=retry_policy
        )
        event_bus_exchange = await declarator.declare_event_bus_exchange(
            event_bus_exchange_name=self.event_bus_exchange
        )
        self.queues[event_type] = await declarator.declare_all(event_bus_exchange)

    async def start_consuming(self):
        for event_type in self.event_handlers:
            await self._start_event_consuming(event_type)
        await asyncio.Future()

    async def close(self):
        await self.channel.close()
        await self.connection.close()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def _start_event_consuming(
        self,
        event_type: Type[Event]
    ):
        queue = self.queues[event_type]
        event_handler = self.event_handlers[event_type]
        async def message_handler(
            message: AbstractIncomingMessage
        ):
            deserialized_event = json.loads(message.body)
            event = event_type(**deserialized_event)
            try:
                await event_handler(event)
                await message.ack()
                self.logger.debug(
                    f"handled event {event.name()} (id={event.id})"
                )
            except Exception:
                self.logger.debug(message.properties.headers)
                self.logger.error(
                    f"exception while handling event {event.name()} (id={event.id})",
                    exc_info=True
                )
                await message.reject(requeue=False)
        await queue.consume(message_handler)
