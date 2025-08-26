import datetime
import uuid
import re

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

    def __post_init__(self):
        self._name_converter = re.compile(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")

    @classmethod
    def name(cls) -> str:
        return cls._name_converter.sub('_', cls.__name__).lower()
