import datetime
import typing

import numpy as np
import pandas as pd

from nwpc_message_tool.message import ProductionEventMessage, EventStatus
from nwpc_message_tool._type import StartTimeType


def load_message(doc: dict) -> ProductionEventMessage:
    status = doc["status"]
    if status == "0":
        status = EventStatus.Complete
    system = doc["source"]

    message = ProductionEventMessage(
        message_type="production",
        time=pd.Timestamp(doc["datetime"]),
        system=system,
        stream="oper",
        production_type="grib2",
        production_name="orig",
        event="before_upload",
        status=status,
        start_time=pd.Timestamp(doc["startTime"]),
        forecast_time=pd.Timedelta(f"{doc['forecastTime']}h"),
    )
    return message


def get_index(
        start_time: StartTimeType = None
) -> typing.List[str]:
    if isinstance(start_time, typing.Tuple):
        time_series = pd.date_range(
            start=pd.Timestamp(start_time[0]),
            end=pd.Timestamp(start_time[1]),
            freq="D"
        )
        return [h.strftime("nmc-prod-%Y-%m") for h in time_series]
    elif isinstance(start_time, typing.List) or isinstance(start_time, np.ndarray) or isinstance(start_time, pd.DatetimeIndex):
        return [h.strftime("nmc-prod-%Y-%m") for h in start_time]
    return [start_time.strftime("nmc-prod-%Y-%m")]


def get_query_body(
        system: str,
        start_time: StartTimeType = None,
        # production_type: str = None,
        # production_stream: str = None,
        # production_name: str = None,
        # forecast_time: str = None,
        **kwargs
) -> dict:
    conditions = [{
        "term": {"source": system}
    }]
    if isinstance(start_time, datetime.datetime):
        conditions.append({
            "term": {
                "startTime": start_time.isoformat()
            }
        })
    elif isinstance(start_time, typing.Tuple):
        conditions.append({
            "range": {
                "startTime": {
                    "gte": start_time[0].isoformat(),
                    "lte": start_time[1].isoformat(),
                }
            }
        })
    elif isinstance(start_time, typing.List) or isinstance(start_time, pd.DatetimeIndex):
        conditions.append({
            "terms": {
                "startTime": [s.isoformat() for s in start_time]
            }
        })

    query_body = {
        "query": {
            "bool": {
                "filter": conditions
            },
        },
        "sort": [
            {
                "datetime": "asc"
            }
        ]
    }
    return query_body
