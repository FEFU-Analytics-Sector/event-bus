import datetime
import uuid
from pydantic import BaseModel, Field


class Event(BaseModel):
    id: str = Field(
        init=False,
        default_factory=lambda: str(uuid.uuid4())
    )
    created_at: datetime.datetime = Field(
        init=False,
        default_factory=lambda: datetime.datetime.now()
    )


    @classmethod
    def name(cls) -> str:
        return cls.__name__
