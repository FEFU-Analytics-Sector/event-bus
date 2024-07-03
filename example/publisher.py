import asyncio

import aio_pika
from aio_pika import ExchangeType

from event_bus import RabbitMQEventPublisher
from example.events import Event1, Event2


async def publish(exchange, text: str, *, rk: str) -> None:
    await exchange.publish(
        message=aio_pika.Message(
            body=text.encode('utf-8'),
            content_type="application/json",
            content_encoding="utf-8",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        ),
        routing_key=rk
    )

async def main():
    rabbitmq_connection_url = "ampq://rmuser:rmpassword@localhost:5672/"
    publisher_1 = RabbitMQEventPublisher(
        amqp_connection_url=rabbitmq_connection_url,
        event_bus_exchange="event_bus_example.exchange",
    )
    await publisher_1.connect()
    await publisher_1.publish(Event1())
    await publisher_1.publish(Event2())

if __name__ == "__main__":
    asyncio.run(main())

