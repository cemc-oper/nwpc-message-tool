from enum import Enum
import typing

import pandas as pd


class EventStatus(Enum):
    Unknown = 0
    Complete = 1
    Queued = 2
    Aborted = 3
    Submitted = 4
    Active = 5
    Suspended = 6


class EventMessage(object):
    def __init__(
            self,
            message_type: str = None,
            time: pd.Timestamp = None,
            **kwargs,
    ):
        self.message_type: str = message_type
        self.time: pd.Timestamp = time


class ProductionEventMessage(EventMessage):
    def __init__(
            self,
            system: str = None,
            stream: str = None,
            production_type: str = None,
            production_name: str = None,
            event: str = None,
            status: EventStatus = EventStatus.Unknown,
            start_time: pd.Timestamp = None,
            forecast_time: pd.Timedelta = None,
            **kwargs,
    ):
        super(ProductionEventMessage, self).__init__(**kwargs)
        self.message_type = "production"
        self.system: str = system
        self.stream: str = stream
        self.production_type: str = production_type
        self.production_name: str = production_name
        self.event: str = event
        self.status: EventStatus = status
        self.start_time: pd.Timestamp = start_time
        self.forecast_time: pd.Timedelta = forecast_time


class ProductionStandardTimeMessage(EventMessage):
    def __init__(
            self,
            system: str = None,
            stream: str = None,
            production_type: str = None,
            production_name: str = None,
            start_hours: typing.List= None,
            **kwargs,
    ):
        super(ProductionStandardTimeMessage, self).__init__(**kwargs)
        self.message_type = "production"
        self.system: str = system
        self.stream: str = stream
        self.production_type: str = production_type
        self.production_name: str = production_name
        self.start_hours = start_hours
