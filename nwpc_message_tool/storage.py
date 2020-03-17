import datetime
import typing

from elasticsearch import Elasticsearch
import pandas as pd
import numpy as np
from loguru import logger

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
    ) -> typing.Iterable[ProductionEventMessage]:
        raise NotImplemented()


class EsMessageStorage(MessageStorage):
    def __init__(self, hosts: list):
        super(EsMessageStorage, self).__init__()
        self.client = Elasticsearch(hosts=hosts)

    def get_production_messages(
            self,
            system: str,
            production_type: str = None,
            production_stream: str = None,
            production_name: str = None,
            start_time: datetime.datetime = None,
            forecast_time: str = None,
            size: int = 20,
    ) -> typing.Iterable[ProductionEventMessage]:
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

        search_from = 0
        total = np.iinfo(np.int16).max

        while search_from < total:
            res = self._get_result(
                index="2020-03",
                query_body=query_body,
                search_from=search_from,
                search_size=size,
            )

            total = res['hits']['total']['value']
            logger.info(f"total: {total}")
            search_from += len(res['hits']['hits'])
            for hit in res['hits']['hits']:
                yield load_message(hit["_source"])

    def _get_result(self, index: str, query_body: dict, search_from: int, search_size: int):
        search_body = {
            "size": search_size,
            "from": search_from,
        }
        search_body.update(**query_body)
        print(search_body)
        res = self.client.search(index=index, body=search_body)
        return res


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
