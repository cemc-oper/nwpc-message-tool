import typing
import datetime

import pandas as pd

from nwpc_message_tool.message import ProductionEventMessage, EventStatus


def fix_system_name(system: str):
    return system


def load_message(doc: dict) -> ProductionEventMessage:
    data = doc["data"]
    message = ProductionEventMessage(
        message_type=doc["type"],
        time=pd.Timestamp(doc["time"]),
        system=data["system"],
        stream=data["stream"],
        production_type=data["type"],
        production_name=data["name"],
        event=data["event"],
        status=EventStatus(data["status"]),
        start_time=pd.Timestamp(data["start_time"]),
        forecast_time=pd.Timedelta(data["forecast_time"]),
    )
    return message


def get_index(start_time: datetime.datetime = None) -> typing.List[str]:
    return [start_time.strftime("%Y-%m")]


def get_production_query_body(
        system: str,
        production_type: str = None,
        production_stream: str = None,
        production_name: str = None,
        start_time: datetime.datetime = None,
        forecast_time: str = None,
) -> dict:
    conditions = [{
        "match": {"data.system": system}
    }]
    if production_type is not None:
        conditions.append({"match": {"data.type": production_type}})
    if production_stream is not None:
        conditions.append({"match": {"data.stream": production_stream}})
    if production_name is not None:
        conditions.append({"match": {"data.name": production_name}})
    if start_time is not None:
        conditions.append({"match": {"data.start_time": start_time.isoformat()}})

    query_body = {
        "query": {
            "bool": {
                "must": conditions
            },
        },
    }
    return query_body
