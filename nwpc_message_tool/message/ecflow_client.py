import typing
import datetime
import pandas as pd

from .event import EventMessage


class EcflowClientMessage(EventMessage):
    def __init__(
            self,
            command: str = None,
            arguments: typing.List[str] = None,
            envs: typing.Mapping = None,
            ecflow_host: str = None,
            ecflow_port: str = None,
            node_name: str = None,
            node_rid: str = None,
            try_no: str or int = None,
            ecf_date: str or datetime.datetime or pd.Timestamp = None,
            **kwargs
    ):
        super(EcflowClientMessage, self).__init__(
            **kwargs
        )
        self.command = command
        self.arguments = arguments
        self.envs = envs
        self.ecflow_host = ecflow_host
        self.ecflow_port = ecflow_port
        self.node_name = node_name
        self.node_rid = node_rid
        self.try_no = int(try_no)
        self.ecf_date = pd.to_datetime(ecf_date)
