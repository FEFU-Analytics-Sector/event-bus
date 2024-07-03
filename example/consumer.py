import asyncio
import sys
import logging
from collections import defaultdict
from datetime import timedelta
from typing import Type

from event_bus import (
    RabbitMQEventConsumer, Event, RetryPolicy
)
from event_bus.types import EventHandler
from example.events import Event1, Event2


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


max_retries_map: dict[str, int] = {
    "event_1": 0,
    "event_2": 4,
    "event_3": 0,
}
current_retries_map: dict[str, int] = defaultdict(lambda: 0)


def make_handler_for_event(event_type: Type[Event]) -> EventHandler:
    async def event_handler(event: event_type):
        """
        simple simulation of errors to demonstrate retries mechanism
        """
        max_retries = max_retries_map[event.name()]
        current_retry = current_retries_map[event.name()]
        if current_retry != max_retries:
            current_retries_map[event.name()] += 1
            print("oops")
            raise Exception()
        print("success")
    return event_handler

async def main():
    rabbitmq_connection_url = "ampq://rmuser:rmpassword@localhost:5672/"

    """
    declare consumer with name consumer_1
    name must be unique for each consumer
    """
    consumer_1 = RabbitMQEventConsumer(
        amqp_connection_url=rabbitmq_connection_url,
        event_bus_exchange="event_bus_example.exchange",
        name="consumer_1"
    )
    await consumer_1.connect()

    """
    subscribe to event without retries
    will be instant requeued if exception is raised in handler
    """
    await consumer_1.subscribe(
        Event1,
        make_handler_for_event(Event1)
    )


    """
    subscribe to event with 3 retries
    retry happens when exception is raised in handler
    event will be routed in consumer_name.event_name.fail_queue when exceed retry limit
    """
    await consumer_1.subscribe(
        Event2,
        make_handler_for_event(Event2),
        retry_policy=RetryPolicy(
            retry_delays=[
                timedelta(seconds=2),
                timedelta(seconds=5),
                timedelta(seconds=10)
            ]
        )
    )

    await consumer_1.start_consuming()


if __name__ == "__main__":
    asyncio.run(main())
