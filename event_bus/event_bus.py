from dataclasses import field, dataclass
from typing import Type

from event_bus.consumer import RabbitMQEventConsumer
from event_bus.publisher import RabbitMQEventPublisher
from event_bus.event import Event
from event_bus.retry_policy import RetryPolicy
from event_bus.types import EventHandler


@dataclass
class RabbitMQEventBus:
    consumer: RabbitMQEventConsumer = field(
        init=True
    )
    publisher: RabbitMQEventPublisher = field(
        init=True
    )

    async def start_consuming(
        self
    ):
        await self.consumer.start_consuming()

    async def publish(
        self,
        event: Event
    ) -> None:
        await self.publisher.publish(event)

    async def subscribe(
        self,
        event_type: Type[Event],
        handler: EventHandler,
        *,
        retry_policy: RetryPolicy | None = None
    ):
        await self.consumer.subscribe(
            event_type,
            handler,
            retry_policy=retry_policy
        )
