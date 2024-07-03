from dataclasses import dataclass
from typing import Type

from event_bus.event import Event


@dataclass(frozen=True)
class NamingHelper:
    event_type: Type[Event]
    consumer_name: str

    def exchange_name(self) -> str:
        return f"{self.consumer_name}.{self.event_type.name()}.exchange"

    def dlx_name(self) -> str:
        return f"{self.consumer_name}.{self.event_type.name()}.dlx"

    def queue_name(self) -> str:
        return f"{self.consumer_name}.{self.event_type.name()}.queue"

    def retry_queue_name(
        self,
        retry_number: int
    ) -> str:
        return f"{self.consumer_name}.{self.event_type.name()}.retry_queue_{retry_number}"

    def fail_queue_name(self) -> str:
        return f"{self.consumer_name}.{self.event_type.name()}.fail_queue"

    def routing_key_name(self) -> str:
        return f"{self.event_type.name()}.rk"

    def retry_routing_key_name(
        self,
        retry_number: int
    ) -> str:
        return f"{self.consumer_name}.{self.event_type.name()}.retry_{retry_number}_rk"

    def fail_routing_key_name(self) -> str:
        return f"{self.consumer_name}.{self.event_type.name()}.fail_rk"
