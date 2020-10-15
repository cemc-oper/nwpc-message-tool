import typing
import datetime

import numpy as np
import pandas as pd

from nwpc_message_tool.message import EcflowClientMessage
from nwpc_message_tool._type import StartTimeType


def load_message(doc: dict) -> EcflowClientMessage:
    data = doc["data"]
    message = EcflowClientMessage(
        message_type=doc["type"],
        time=pd.Timestamp(doc["time"]),
        command=data["command"],
        arguments=data["args"],
        envs=data["envs"],
        ecflow_host=data["ecf_host"],
        ecflow_port=data["ecf_port"],
        node_name=data["ecf_name"],
        node_rid=data["ecf_rid"],
        try_no=data["ecf_tryno"],
        ecf_date=data["ecf_date"],
    )
    return message


def get_query_body(
        node_name: str,
        ecflow_host: str = None,
        ecflow_port: str = None,
        ecf_date: StartTimeType = None
) -> typing.Dict:
    conditions = [{
        "term": {
            "data.ecf_name.keyword": node_name
        }
    }]
    if ecflow_host is not None:
        conditions.append({
            "term": {
                "data.ecf_host": ecflow_host
            }
        })
    if ecflow_port is not None:
        conditions.append({
            "term": {
                "data.ecf_port": ecflow_port
            }
        })
    if ecf_date is not None:
        if isinstance(ecf_date, datetime.datetime):
            conditions.append({
                "term": {
                    "data.ecf_date": ecf_date.strftime("%Y%m%d")
                }
            })
        elif isinstance(ecf_date, typing.List):
            conditions.append({
                "terms": {
                    "data.ecf_date": [d.strftime("%Y%m%d") for d in ecf_date]
                }
            })
        elif isinstance(ecf_date, typing.Tuple):
            conditions.append({
                "range": {
                    "data.ecf_date": {
                        "gte": ecf_date[0].strftime("%Y%m%d"),
                        "lte": ecf_date[1].strftime("%Y%m%d"),
                    }
                }
            })

    query_body = {
        "query": {
            "bool": {
                "filter": conditions
            },
        },
    }
    return query_body


def get_index(
        ecf_date: typing.Union[StartTimeType, np.ndarray, pd.DatetimeIndex] = None
) -> typing.List[str]:
    if isinstance(ecf_date, typing.Tuple):
        time_series = pd.date_range(
            start=pd.Timestamp(ecf_date[0]),
            end=pd.Timestamp(ecf_date[1]),
            freq="D"
        )
        return [h.strftime("ecflow-client-%Y-%m-%d") for h in time_series]
    elif (
            isinstance(ecf_date, typing.List)
            or isinstance(ecf_date, np.ndarray)
            or isinstance(ecf_date, pd.DatetimeIndex)
    ):
        return [h.strftime("ecflow-client-%Y-%m-%d") for h in ecf_date]
    return [f'ecflow-client-{ecf_date.strftime("%Y-%m-%d")}']
