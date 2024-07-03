import asyncio
import sys
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta

from event_bus import (
    RabbitMQEventConsumer, RabbitMQEventPublisher, Event,
    RetryPolicy
)
import aio_pika
from aio_pika.abc import (
    AbstractRobustChannel,
    ExchangeType,
    AbstractIncomingMessage, AbstractRobustConnection, AbstractRobustQueue,
    AbstractRobustExchange
)

from event_bus.event_infrastructure_declarator import EventInfrastructureDeclarator

# from utils import retry_checker


# @retry_checker(retry_delay=timedelta(seconds=5))
# async def sru():
#     print("sru")


logger = logging.getLogger("")
stdout_handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    '%(asctime)s | %(name)-35s | %(levelname)-6s | %(message)s'
)
stdout_handler.setFormatter(formatter)
logger.addHandler(stdout_handler)
logger.setLevel(logging.DEBUG)

logging.getLogger("aiormq").setLevel(logging.INFO)
logging.getLogger("aio_pika.connection").setLevel(logging.INFO)
logging.getLogger("aio_pika.robust_connection").setLevel(logging.INFO)

logger = logging.getLogger("event_bus.main")

# async def main():
#     rabbitmq_connection_url = "ampq://rmuser:rmpassword@localhost:5672/"
#
#     class KekEvent(Event):
#         data: int
#
#     retry_delay = timedelta(seconds=1)
#
#     @retry_checker(
#         retry_delay=retry_delay
#     )
#     async def handler(
#         event: KekEvent
#     ):
#         logger.debug(event.id)
#         raise Exception()
#
#     print("123")
#     async with (
#         RabbitMQEventConsumer(
#             amqp_connection_url=rabbitmq_connection_url,
#             name="test_consumer",
#             prefetch_count=1
#         ) as consumer,
#         RabbitMQEventPublisher(
#             amqp_connection_url=rabbitmq_connection_url
#         ) as publisher
#     ):
#         await consumer.subscribe(
#             KekEvent,
#             handler=handler,
#             retry_policy=RetryPolicy(
#                 retry_delays=[
#                     timedelta(seconds=3),
#                     timedelta(seconds=5),
#                     timedelta(seconds=3),
#                 ]
#             )
#         )
#         await publisher.publish(
#             KekEvent(
#                 data=1
#             )
#         )
#
#         await consumer.start_consuming()


@dataclass
class RetryChecker:
    max_retries: int
    current_retry: int


max_retries_map: dict[str, int] = {
    "1": 3,
    "2": 1,
    "3": 0,
    "4": 3,
    "5": 4,
    "6": 0
}


current_retries_map: dict[(str, str), int] = defaultdict(lambda: 0)

def message_handler(name: str):
    async def handler(
        message: AbstractIncomingMessage
    ):
        logger = logging.getLogger(f"event_bus.main.{name}")
        text = message.body.decode('utf-8')
        try:
            max_retries = max_retries_map[text]
            current_retry = current_retries_map[(text, name)]
            print(current_retry)
            if current_retry != max_retries:
                current_retries_map[(text, name)] += 1
                # raise Exception()
            await message.ack()
            logger.debug(
                f"message {text} handled"
            )
        except Exception as e:
            await message.reject(requeue=False)
            logger.debug(message.properties.headers)
            logger.error(
                f"exception while handling message {text}",
                # exc_info=True
            )
        # logger.debug(message.properties.headers)
    return handler

async def declare_infra(
    consumer_name: str,
    event_name: str,
    common_exchange: AbstractRobustExchange
) -> AbstractRobustQueue:
    rabbitmq_connection_url = "ampq://rmuser:rmpassword@localhost:5672/"
    connection = await aio_pika.connect_robust(
        rabbitmq_connection_url
    )
    channel = await connection.channel()
    exchange = await channel.declare_exchange(
        name=f"{consumer_name}.{event_name}.exchange",
        type=ExchangeType.DIRECT,
        durable=True
    )
    await exchange.bind(
        common_exchange,
        routing_key=event_name
    )
    dlx = await channel.declare_exchange(
        name=f"{consumer_name}.{event_name}.dlx",
        type=ExchangeType.DIRECT,
        durable=True
    )
    queue = await channel.declare_queue(
        name=f"{consumer_name}.{event_name}.queue",
        durable=True,
        arguments={
            "x-dead-letter-exchange": dlx.name
        }
    )
    retry_queue_1 = await channel.declare_queue(
        name=f"{consumer_name}.{event_name}.queue.retry_1",
        durable=True,
        arguments={
            "x-dead-letter-exchange": exchange.name,
            "x-dead-letter-routing-key": f"{event_name}.retry_2",
            "x-message-ttl": 2000
        }
    )
    retry_queue_2 = await channel.declare_queue(
        name=f"{consumer_name}.{event_name}.queue.retry_2",
        durable=True,
        arguments={
            "x-dead-letter-exchange": exchange.name,
            "x-dead-letter-routing-key": f"{event_name}.fail",
            "x-message-ttl": 3000
        }
    )
    fail_queue = await channel.declare_queue(
        name=f"{consumer_name}.{event_name}.queue.fail",
        durable=True,
        arguments={
            "x-dead-letter-exchange": exchange.name,
            "x-dead-letter-routing-key": f"{event_name}"
        }
    )
    await queue.bind(
        exchange,
        routing_key=f"{event_name}"
    )
    await queue.bind(
        exchange,
        routing_key=f"{event_name}.retry_2"
    )
    await queue.bind(
        exchange,
        routing_key=f"{event_name}.fail"
    )
    await retry_queue_1.bind(
        dlx,
        routing_key=f"{event_name}"
    )
    await retry_queue_2.bind(
        dlx,
        routing_key=f"{event_name}.retry_2"
    )
    await fail_queue.bind(
        dlx,
        routing_key=f"{event_name}.fail"
    )
    return queue

async def main():
    rabbitmq_connection_url = "ampq://rmuser:rmpassword@localhost:5672/"
    connection = await aio_pika.connect_robust(
        rabbitmq_connection_url
    )
    channel = await connection.channel()
    common_exchange = await channel.declare_exchange(
        name="common.exchange",
        type=ExchangeType.DIRECT,
        durable=True
    )
    kek_queue1 = await declare_infra("kek", "event_1", common_exchange)
    kek_queue2 = await declare_infra("kek", "event_2", common_exchange)
    qwerty_queue = await declare_infra("qwerty", "event_1", common_exchange)
    abc_queue = await declare_infra("abc", "event_2", common_exchange)
    # await kek_queue1.consume(message_handler("kek_1"))
    # await kek_queue2.consume(message_handler("kek_2"))
    # await qwerty_queue.consume(message_handler("qwerty"))
    # await abc_queue.consume(message_handler("abc"))

    class Event3(Event):

        @classmethod
        def name(cls) -> str:
            return "event_3"

    declarator = EventInfrastructureDeclarator(
        channel=channel,
        event_type=Event3,
        consumer_name="kek",
        retry_policy=RetryPolicy(
            retry_delays=[
                timedelta(seconds=2),
                timedelta(seconds=3),
                timedelta(seconds=2),
            ]
        )
    )
    kek_queue3 = await declarator.declare_all(event_bus_exchange=common_exchange)
    await kek_queue3.consume(message_handler("kek_3"))


    await asyncio.sleep(1000)


if __name__ == "__main__":
    asyncio.run(main())
