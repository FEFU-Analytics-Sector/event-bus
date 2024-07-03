import datetime
from dataclasses import dataclass, field


@dataclass(frozen=True)
class RetryPolicy:
    retry_delays: list[datetime.timedelta] = field(init=True)
