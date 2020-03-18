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


def get_index(start_time: datetime.datetime or typing.Tuple or typing.List = None) -> typing.List[str]:
    if isinstance(start_time, typing.Tuple):
        time_series = pd.date_range(start=pd.Timestamp(start_time[0]), end=pd.Timestamp(start_time[1]), freq="D")
        return [h.strftime("%Y-%m") for h in time_series]
    elif isinstance(start_time, typing.List):
        return [h.strftime("%Y-%m") for h in start_time]
    return [start_time.strftime("%Y-%m")]


def get_production_query_body(
        system: str,
        production_type: str = None,
        production_stream: str = None,
        production_name: str = None,
        start_time: datetime.datetime or typing.Tuple or typing.List = None,
        forecast_time: str = None,
) -> dict:
    conditions = [{
        "term": {"data.system": system}
    }]
    if production_type is not None:
        conditions.append({"term": {"data.type": production_type}})
    if production_stream is not None:
        conditions.append({"term": {"data.stream": production_stream}})
    if production_name is not None:
        conditions.append({"term": {"data.name": production_name}})
    if type(start_time) == datetime.datetime:
        conditions.append({"term": {"data.start_time": start_time.isoformat()}})
    elif isinstance(start_time, typing.Tuple):
        conditions.append({
            "range": {
                "data.start_time": {
                    "gte": start_time[0].isoformat(),
                    "lte": start_time[1].isoformat(),
                }
            }
        })
    elif isinstance(start_time, typing.List):
        conditions.append({
            "terms": {
                "data.start_time": [s.isoformat() for s in start_time]
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
