from dataclasses import dataclass, field

import aio_pika
from aio_pika.abc import (
    AbstractRobustChannel,
    AbstractRobustExchange,
    ExchangeType, AbstractRobustConnection
)

from event_bus.event import Event


@dataclass
class RabbitMQEventPublisher:
    amqp_connection_url: str = field(
        init=True
    )
    event_bus_exchange: str = field(
        init=True
    )

    connection: AbstractRobustConnection = field(init=False)
    channel: AbstractRobustChannel = field(
        init=False
    )

    async def connect(self):
        self.connection = await aio_pika.connect_robust(
            self.amqp_connection_url
        )
        self.channel = await self.connection.channel()

    async def publish(self, event: Event) -> None:
        exchange = await self._declare_exchange()
        await exchange.publish(
            message=self._create_message_from_event(event),
            routing_key=f"{event.name()}.rk"
        )

    async def close(self):
        await self.channel.close()
        await self.connection.close()

    async def __aenter__(
        self
    ):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def _declare_exchange(
        self
    ) -> AbstractRobustExchange:
        return await self.channel.declare_exchange(
            name=self.event_bus_exchange,
            type=ExchangeType.DIRECT,
            durable=True
        )

    @staticmethod
    def _create_message_from_event(
        event: Event
    ) -> aio_pika.Message:
        serialized_event = event.json()
        return aio_pika.Message(
            body=serialized_event.encode(),
            content_type="application/json",
            content_encoding="utf-8",
            message_id=event.id,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        )
