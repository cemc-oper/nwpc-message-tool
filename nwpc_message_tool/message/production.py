import typing

import pandas as pd

from nwpc_message_tool.message.event import (
    EventStatus,
    EventMessage,
)


class ProductionEventMessage(EventMessage):
    """
    Message for production event.
    """
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
    """
    Message of standard time for production.

    Attributes
    ----------
    start_hour : typing.Dict
    """
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
        self.message_type = "production-standard"
        self.system: str = system
        self.stream: str = stream
        self.production_type: str = production_type
        self.production_name: str = production_name
        self.start_hours = start_hours
