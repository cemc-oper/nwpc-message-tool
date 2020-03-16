import datetime
import typing

from elasticsearch import Elasticsearch
import pandas as pd

from .message import ProductionEventMessage, EventStatus


class MessageStorage(object):
    def __init__(self):
        pass

    def get_production_messages(
            self,
            system,
            production_type,
            production_stream,
            production_name,
            start_time,
            forecast_time,
    ):
        pass


class EsMessageStorage(MessageStorage):
    def __init__(self, hosts: list):
        super(EsMessageStorage, self).__init__()
        self.client = Elasticsearch(hosts=hosts)

    def get_production_messages(
            self,
            system,
            production_type: str = None,
            production_stream: str = None,
            production_name: str = None,
            start_time: datetime.datetime = None,
            forecast_time: str = None,
            size: int = 100,
    ) -> typing.List[ProductionEventMessage]:
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
        search_body = {
            "size": size
        }
        search_body.update(**query_body)
        print(search_body)
        res = self.client.search(index="2020-03", body=search_body)
        total_value = res['hits']['total']['value']
        messages = []
        for hit in res['hits']['hits']:
            messages.append(load_message(hit["_source"]))
        return messages


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
