import datetime
import typing

from elasticsearch import Elasticsearch
import numpy as np
from loguru import logger

from nwpc_message_tool.message import ProductionEventMessage
from . import nwpc_message


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
    def __init__(self, hosts: list, engine=nwpc_message):
        super(EsMessageStorage, self).__init__()
        self.client = Elasticsearch(hosts=hosts)
        self._engine = engine

    def get_production_messages(
            self,
            system: str,
            production_type: str = None,
            production_stream: str = None,
            production_name: str = None,
            start_time: datetime.datetime or typing.Tuple = None,
            forecast_time: str = None,
            size: int = 20,
    ) -> typing.Iterable[ProductionEventMessage]:
        query_body = self._engine.get_production_query_body(
            system=system,
            production_stream=production_stream,
            production_type=production_type,
            production_name=production_name,
            start_time=start_time,
            forecast_time=forecast_time,
        )

        search_from = 0
        total = np.iinfo(np.int16).max

        indexes = self._engine.get_index(start_time)
        for index in indexes:
            while search_from < total:
                res = self._get_result(
                    index=index,
                    query_body=query_body,
                    search_from=search_from,
                    search_size=size,
                )

                total = res['hits']['total']['value']
                logger.info(f"total: {total}")
                search_from += len(res['hits']['hits'])
                logger.info("result count: {}", len(res["hits"]["hits"]))
                for hit in res['hits']['hits']:
                    yield self._engine.load_message(hit["_source"])

    def _get_result(self, index: str, query_body: dict, search_from: int, search_size: int):
        logger.info(f"searching from {search_from} with size {search_size}...")
        search_body = {
            "size": search_size,
            "from": search_from,
        }
        search_body.update(**query_body)
        logger.debug(f"search body: {search_body}")
        res = self.client.search(index=index, body=search_body)
        return res
