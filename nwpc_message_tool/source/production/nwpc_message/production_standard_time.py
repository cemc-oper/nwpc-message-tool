import typing

import pandas as pd

from nwpc_message_tool.message import ProductionStandardTimeMessage


def load_message(doc: dict) -> ProductionStandardTimeMessage:
    data = doc["data"]
    message = ProductionStandardTimeMessage(
        message_type=doc["type"],
        time=pd.Timestamp(doc["time"]),
        system=data["system"],
        stream=data["stream"],
        production_type=data["type"],
        production_name=data["name"],
        start_hours=data["start_hours"]
    )
    return message


def get_query_body(
        system: str,
        production_type: str = None,
        production_stream: str = None,
        production_name: str = None,
) -> typing.Dict:
    conditions = [{
        "term": {"data.system": system}
    }]
    if production_type is not None:
        conditions.append({"term": {"data.type": production_type}})
    if production_stream is not None:
        conditions.append({"term": {"data.stream": production_stream}})
    if production_name is not None:
        conditions.append({"term": {"data.name": production_name}})

    query_body = {
        "query": {
            "bool": {
                "filter": conditions
            },
        },
    }
    return query_body


def get_index(**kwargs):
    return "prod-standard-time"
