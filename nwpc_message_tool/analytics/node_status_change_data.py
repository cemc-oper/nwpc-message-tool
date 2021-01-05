import datetime
from abc import ABC, abstractmethod


from .status_change_type import StatusChangeType


class NodeStatusChangeData(ABC):
    """
    An interface of node status change data.
    indicating that the node changes to a new state (status: NodeStatus) at a certain time point (date_time)
    """
    @property
    @abstractmethod
    def date_time(self) -> datetime.datetime:
        pass

    @property
    @abstractmethod
    def status(self) -> StatusChangeType:
        pass
