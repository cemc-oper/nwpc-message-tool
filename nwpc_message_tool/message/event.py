from enum import Enum

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
    """
    Base message for event.
    """
    def __init__(
            self,
            message_type: str = None,
            time: pd.Timestamp = None,
            **kwargs,
    ):
        self.message_type: str = message_type
        self.time: pd.Timestamp = time
