import asyncio
import json
from dataclasses import field, dataclass
from typing import Type

import aio_pika
from aio_pika.abc import (
    AbstractRobustConnection, AbstractRobustChannel,
    AbstractRobustExchange, ExchangeType,
    AbstractIncomingMessage
)

from ..abstract.event_bus import AbstractEventBus, AbstractEvent, EventHandler
from ..abstract.exceptions import EventNotSubscribed


@dataclass
class RabbitMQEventBus(AbstractEventBus):
    ampq_connection_url: str = field()
    exchange_name: str = field()

    connection: AbstractRobustConnection = field(init=False)
    channel: AbstractRobustChannel = field(init=False)
    exchange: AbstractRobustExchange = field(init=False)
    event_handlers: dict[Type[AbstractEvent], EventHandler] = field(
        default_factory=dict, init=False
    )

    async def connect(self):
        self.connection = await aio_pika.connect_robust(
            self.ampq_connection_url
        )
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=1)
        self.exchange = await self.channel.declare_exchange(
            self.exchange_name,
            type=ExchangeType.DIRECT
        )

    async def publish(self, event: AbstractEvent) -> None:
        event_name = type(event).__name__
        await self.exchange.publish(
            message=self._create_message_from_event(event),
            routing_key=event_name
        )

    def subscribe(
        self,
        event_type: Type[AbstractEvent],
        handler: EventHandler
    ):
        self.event_handlers[event_type] = handler

    def unsubscribe(
        self,
        event_type: Type[AbstractEvent],
        handler: EventHandler
    ) -> None:
        if event_type not in self.event_handlers:
            raise EventNotSubscribed(event_type)
        self.event_handlers.pop(event_type)

    async def start_consuming(self, consumer_name: str):
        for event_type in self.event_handlers:
            await self._start_event_consuming(consumer_name, event_type)
        await asyncio.Future()

    async def close(self):
        await self.channel.close()
        await self.connection.close()

    @staticmethod
    def _create_message_from_event(event: AbstractEvent) -> aio_pika.Message:
        serialized_event = event.json()
        return aio_pika.Message(
            body=serialized_event.encode(),
            content_type="application/json",
            content_encoding="utf-8",
            message_id=event.id,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        )

    async def _bind_event_to_queue(
        self,
        *,
        event_type: Type[AbstractEvent],
        consumer_name: str
    ):
        event_name = event_type.__name__
        queue_name = f"{consumer_name}_{event_name}_queue"
        queue = await self.channel.declare_queue(
            name=queue_name,
            durable=True
        )
        await queue.bind(
            self.exchange,
            routing_key=event_name,
        )
        return queue

    async def _start_event_consuming(
        self,
        consumer_name: str,
        event_type: Type[AbstractEvent]
    ):
        queue = await self._bind_event_to_queue(
            event_type=event_type,
            consumer_name=consumer_name
        )
        event_handler = self.event_handlers[event_type]

        async def message_handler(message: AbstractIncomingMessage):
            async with message.process(requeue=True):
                deserialized_event = json.loads(message.body)
                event = event_type(**deserialized_event)
                await event_handler(event)

        await queue.consume(message_handler)
