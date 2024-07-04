from dataclasses import dataclass
from typing import Type

from event_bus.event import Event


@dataclass(frozen=True)
class NamingHelper:
    event_type: Type[Event]
    consumer_name: str

    def exchange_name(self) -> str:
        return f"{self.consumer_name}.{self.event_type.name()}.x"

    def dlx_name(self) -> str:
        return f"{self.consumer_name}.{self.event_type.name()}.dlx"

    def queue_name(self) -> str:
        return f"{self.consumer_name}.{self.event_type.name()}.q"

    def retry_queue_name(
        self,
        retry_number: int
    ) -> str:
        return f"{self.consumer_name}.{self.event_type.name()}.rq_{retry_number}"

    def fail_queue_name(self) -> str:
        return f"{self.consumer_name}.{self.event_type.name()}.fq"

    def routing_key_name(self) -> str:
        return f"{self.event_type.name()}.rk"

    def retry_routing_key_name(
        self,
        retry_number: int
    ) -> str:
        return f"{self.consumer_name}.{self.event_type.name()}.rrk_{retry_number}"

    def fail_routing_key_name(self) -> str:
        return f"{self.consumer_name}.{self.event_type.name()}.frk"
