from event_bus.event import Event


class Event1(Event):
    @classmethod
    def name(
        cls
    ) -> str:
        return "event_1"


class Event2(Event):
    @classmethod
    def name(
        cls
    ) -> str:
        return "event_2"


class Event3(Event):
    @classmethod
    def name(
        cls
    ) -> str:
        return "event_3"
