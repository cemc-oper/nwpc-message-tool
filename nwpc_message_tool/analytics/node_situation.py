from enum import Enum
from typing import List
import datetime

from .status_change_type import StatusChangeType

from .situation_type import (
    TaskSituationType,
)


class TimePoint(object):
    """
    Time point, indicating the state (NodeStatus) at a certain time point (time)
    """
    def __init__(
            self,
            status: StatusChangeType = StatusChangeType.Unknown,
            time: datetime.datetime = None
    ):
        self.status = status
        self.time = time

    def __eq__(self, other):
        return self.status == other.status and self.time == self.time


class TimePeriodType(Enum):
    """
    Time period type
    """
    InSubmitted = "in_submitted"
    InActive = "in_active"
    InAll = "in_all"


class TimePeriod(object):
    """
    Time period
    """
    def __init__(
            self,
            period_type: TimePeriodType = None,
            start_time: datetime.datetime = None,
            end_time: datetime.datetime = None,
    ):
        self.period_type = period_type
        self.start_time = start_time
        self.end_time = end_time


class NodeSituation(object):
    """
    The running situation of the node which can be obtained by analyzing the NodeState sequence.
    """
    def __init__(
            self,
            situation: TaskSituationType = TaskSituationType.Initial,
            time_points: List[TimePoint] = None,
            time_periods: List[TimePeriod] = None,
    ):
        self.situation = situation

        if time_points is None:
            time_points = list()
        self.time_points = time_points

        if time_periods is None:
            time_periods = list()
        self.time_periods = time_periods
