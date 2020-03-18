import datetime
import typing

import pandas as pd

from nwpc_message_tool.message import ProductionEventMessage, EventStatus


def fix_system_name(system: str):
    mapping = {
        "grapes_gfs_gmf": "nwpc_grapes_gfs",
        "grapes_meso_10km": "nwpc_grapes_meso_10km",
        "grapes_meso_3km": "nwpc_grapes_meso_3km",
    }
    return mapping.get(system, system)


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


def get_index(start_time: datetime.datetime = None) -> typing.List[str]:
    return [start_time.strftime("nmc-prod-%Y-%m")]


def get_production_query_body(
        system: str,
        production_type: str = None,
        production_stream: str = None,
        production_name: str = None,
        start_time: datetime.datetime = None,
        forecast_time: str = None,
) -> dict:
    conditions = [{
        "match": {"source": system}
    }]
    if start_time is not None:
        conditions.append({"match": {"startTime": start_time.isoformat()}})

    query_body = {
        "query": {
            "bool": {
                "must": conditions
            },
        },
    }
    return query_body
