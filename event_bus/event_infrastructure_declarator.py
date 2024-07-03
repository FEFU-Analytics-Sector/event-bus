from dataclasses import dataclass, field
from datetime import timedelta
from typing import Type

from event_bus.event import Event
from event_bus.retry_policy import RetryPolicy
from event_bus.naming_helper import NamingHelper

from aio_pika.abc import (
    AbstractRobustChannel,
    ExchangeType,
    AbstractRobustQueue,
    AbstractRobustExchange
)

@dataclass
class EventInfrastructureDeclarator:
    channel: AbstractRobustChannel
    event_type: Type[Event]
    consumer_name: str
    retry_policy: RetryPolicy
    naming_helper: NamingHelper = field(
        init=False
    )

    def __post_init__(self):
        self.naming_helper = NamingHelper(
            event_type=self.event_type,
            consumer_name=self.consumer_name
        )

    async def declare_exchange(
        self
    ) -> AbstractRobustExchange:
        return await self.channel.declare_exchange(
            name=self.naming_helper.exchange_name(),
            type=ExchangeType.DIRECT,
            durable=True
        )

    async def declare_dlx(
        self
    ) -> AbstractRobustExchange:
        return await self.channel.declare_exchange(
            name=self.naming_helper.dlx_name(),
            type=ExchangeType.DIRECT,
            durable=True
        )

    async def declare_queue(
        self,
        *,
        dlx: AbstractRobustExchange
    ) -> AbstractRobustQueue:
        return await self.channel.declare_queue(
            name=self.naming_helper.queue_name(),
            durable=True,
            arguments={
                "x-dead-letter-exchange": dlx.name
            }
        )

    async def declare_retry_queue(
        self,
        *,
        retry_number: int,
        retry_delay: timedelta,
        exchange: AbstractRobustExchange
    ) -> AbstractRobustQueue:
        next_rk = self.naming_helper.retry_routing_key_name(retry_number + 1)

        max_retries = len(self.retry_policy.retry_delays)
        if retry_number == max_retries:
            next_rk = self.naming_helper.fail_routing_key_name()

        retry_delay_in_milliseconds = (
            retry_delay // timedelta(milliseconds=1)
        )

        return await self.channel.declare_queue(
            name=self.naming_helper.retry_queue_name(retry_number),
            durable=True,
            arguments={
                "x-dead-letter-exchange": exchange.name,
                "x-dead-letter-routing-key": next_rk,
                "x-message-ttl": retry_delay_in_milliseconds
            }
        )

    async def declare_fail_queue(
        self,
        *,
        exchange: AbstractRobustExchange
    ) -> AbstractRobustQueue:
        return await self.channel.declare_queue(
            name=self.naming_helper.fail_queue_name(),
            durable=True,
            arguments={
                "x-dead-letter-exchange": exchange.name,
                "x-dead-letter-routing-key": self.naming_helper.routing_key_name(),
            }
        )

    async def declare_and_bind_retry_queues(
        self,
        *,
        exchange: AbstractRobustExchange,
        dlx: AbstractRobustExchange,
        queue: AbstractRobustQueue
    ) -> None:
        for retry_number, retry_delay in enumerate(
            self.retry_policy.retry_delays,
            start=1
        ):
            retry_queue = await self.declare_retry_queue(
                retry_number=retry_number,
                retry_delay=retry_delay,
                exchange=exchange,
            )

            rk = self.naming_helper.routing_key_name()
            if retry_number != 1:
                rk = self.naming_helper.retry_routing_key_name(retry_number)

            await retry_queue.bind(
                dlx,
                routing_key=rk
            )

            await queue.bind(
                exchange,
                routing_key=rk
            )

    async def declare_and_bind_fail_queue(
        self,
        *,
        exchange: AbstractRobustExchange,
        dlx: AbstractRobustExchange,
        queue: AbstractRobustQueue
    ) -> None:
        fail_queue = await self.declare_fail_queue(exchange=exchange)
        fail_rk = self.naming_helper.fail_routing_key_name()
        if self.retry_policy is None:
            fail_rk = self.naming_helper.routing_key_name()

        await fail_queue.bind(
            dlx,
            routing_key=fail_rk
        )

        await queue.bind(
            exchange,
            routing_key=fail_rk
        )

    async def declare_event_bus_exchange(
        self,
        event_bus_exchange_name: str
    ):
         return await self.channel.declare_exchange(
            name=event_bus_exchange_name,
            type=ExchangeType.DIRECT,
            durable=True
        )

    async def declare_all(
        self,
        event_bus_exchange: AbstractRobustExchange
    ) -> AbstractRobustQueue:
        exchange = await self.declare_exchange()
        await exchange.bind(
            event_bus_exchange,
            routing_key=self.naming_helper.routing_key_name()
        )

        dlx = await self.declare_dlx()
        queue = await self.declare_queue(dlx=dlx)

        if self.retry_policy is not None:
            await self.declare_and_bind_retry_queues(
                exchange=exchange,
                dlx=dlx,
                queue=queue
            )

        await self.declare_and_bind_fail_queue(
            exchange=exchange,
            dlx=dlx,
            queue=queue
        )

        return queue
