import typing
import datetime
import pandas as pd

from .event import EventMessage


class EcflowClientMessage(EventMessage):
    """
    ecFlow client command message.
    """
    def __init__(
            self,
            command: str = None,
            arguments: typing.List[str] = None,
            envs: typing.Mapping = None,
            ecflow_host: str = None,
            ecflow_port: str = None,
            node_name: str = None,
            node_rid: str = None,
            try_no: typing.Union[str, int] = None,
            ecf_date: typing.Union[str, datetime.datetime] = None,
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
        if try_no.isdigit():
            self.try_no = int(try_no)
        else:
            pass
        self.ecf_date = pd.to_datetime(ecf_date, format="%Y%m%d").tz_localize('UTC')
