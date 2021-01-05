import datetime

from .node_status_change_data import NodeStatusChangeData, StatusChangeType
from nwpc_message_tool.message.ecflow_client import EcflowClientMessage


@NodeStatusChangeData.register
class StatusChangeEntry(object):
    def __init__(self, record: EcflowClientMessage):
        self._record = record

    @property
    def status(self) -> StatusChangeType:
        return convert_command_toStatus_change_type(self._record.command)

    @property
    def date_time(self) -> datetime.datetime:
        return self._record.time.ceil("S").to_pydatetime()


def convert_command_toStatus_change_type(command: str) -> StatusChangeType:
    status_map = {
        "submit": StatusChangeType.Submit,
        "init": StatusChangeType.Initial,
        "complete": StatusChangeType.Complete,
        "abort": StatusChangeType.Abort,
    }

    return status_map.get(command, StatusChangeType.Unknown)
