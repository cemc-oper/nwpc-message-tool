import datetime
import typing
from abc import ABC, abstractmethod

from elasticsearch import Elasticsearch
import numpy as np
from loguru import logger
from tqdm.auto import tqdm

import nwpc_message_tool.source.production.nwpc_message
import nwpc_message_tool.source.ecflow_client
from nwpc_message_tool._type import StartTimeType
from nwpc_message_tool._config import load_config

from nwpc_message_tool.message import (
    ProductionEventMessage,
    ProductionStandardTimeMessage,
    EcflowClientMessage
)


class MessageStorage(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def get_production_messages(
            self,
            system,
            production_type,
            production_stream,
            production_name,
            start_time,
            forecast_time,
            engine,
    ) -> typing.Iterable[ProductionEventMessage]:
        pass

    @abstractmethod
    def get_ecflow_client_messages(
            self,
            node_name,
            ecflow_host,
            ecflow_port,
            ecf_date,
            engine,
    ) -> typing.Iterable[EcflowClientMessage]:
        pass

    @abstractmethod
    def get_production_standard_time_message(
            self,
            system,
            production_type,
            production_stream,
            production_name,
            engine,
    ) -> typing.Iterable[ProductionStandardTimeMessage]:
        pass


class EsMessageStorage(MessageStorage):
    def __init__(
            self,
            hosts: typing.List,
            debug: bool = True,
            show_progress: bool = False,
    ):
        super(EsMessageStorage, self).__init__()
        self.client = Elasticsearch(hosts=hosts)
        self.debug: bool = debug
        self.show_progress: bool = show_progress

    def get_production_messages(
            self,
            system: str,
            production_type: str = None,
            production_stream: str = None,
            production_name: str = None,
            start_time: StartTimeType = None,
            forecast_time: str = None,
            engine = None,
            size: int = 20,
    ) -> typing.Iterable[ProductionEventMessage]:
        """
        Get production messages from ElasticSearch.

        Examples
        --------

        >>> import pandas as pd
        >>> from nwpc_message_tool import EsMessageStorage
        >>> storage = EsMessageStorage(
        ...    hosts=["localhost:9200"]
        ... )
        >>> results = storage.get_production_messages(
        ...     system="grapes_gfs_gmf",
        ...     production_stream="oper",
        ...     production_type="grib2",
        ...     production_name="orig",
        ...     start_time=pd.to_datetime("2021-04-01 00:00"),
        ...     forecast_time="240h"
        ... )
        >>> for m in results:
        ...     print(m.time)
        2021-04-01 05:21:04.833865600+00:00

        Parameters
        ----------
        system :
            system which generates the product, such as

            - grapes_gfs_gmf
            - grapes_meso_3km
            - grapes_meso_10km
            - grapes_tym

        production_type :
            type of production, such as "grib2"
        production_stream :
            stream of production, such as "oper"
        production_name :
            name of production, such as "orig"
        start_time :
            start time of cycle

            - ``TimeType1``: time point
            - ``typing.Tuple[TimeType, TimeType]``: time range of [t1, t2]
            - ``typing.List[TimeType]``: time list of [t1, t2, t3, .... tn]

        forecast_time :
            forecast time for production
        engine :
            source engine
        size :
            messages count for one search request to ElasticSearch.

        Returns
        -------
        typing.Iterable[ProductionEventMessage]
            production event messages
        """
        if engine is None:
            engine = nwpc_message_tool.source.production.nwpc_message.production

        query_body = engine.get_query_body(
            system=system,
            production_stream=production_stream,
            production_type=production_type,
            production_name=production_name,
            start_time=start_time,
            forecast_time=forecast_time,
        )

        pbar = None
        scroll = "1m"
        scroll_id = None

        indexes = engine.get_index(start_time)
        indexes = set(indexes)

        for index in indexes:
            search_from = 0
            total = np.iinfo(np.int16).max

            while search_from < total:
                if search_from == 0:
                    res = self._get_result(
                        index=index,
                        query_body=query_body,
                        search_from=search_from,
                        search_size=size,
                        scroll=scroll,
                    )
                    current_total = res['hits']['total']['value']
                    scroll_id = res["_scroll_id"]
                    total = current_total
                    logger.info(f"[{system}] found results: {total}")
                    if self.show_progress:
                        pbar = tqdm(total=total)
                else:
                    res = self._get_result_scroll(
                        scroll=scroll,
                        scroll_id=scroll_id,
                    )

                search_from += len(res['hits']['hits'])
                current_count = len(res["hits"]["hits"])
                if pbar is not None:
                    pbar.update(current_count)
                for hit in res['hits']['hits']:
                    yield engine.load_message(hit["_source"])

        if pbar is not None:
            pbar.close()

        if scroll_id is not None:
            self.client.clear_scroll(scroll_id=scroll_id)

    def get_ecflow_client_messages(
            self,
            node_name: str,
            ecflow_host: str = None,
            ecflow_port: str = None,
            ecf_date: StartTimeType = None,
            index_ecf_date: StartTimeType = None,
            engine = None,
            size: int = 20,
    ) -> typing.Iterable[EcflowClientMessage]:
        """
        Get ecflow client command messages from ElasticSearch.

        Examples
        --------

        >>> import pandas as pd
        >>> from nwpc_message_tool import EsMessageStorage
        >>> storage = EsMessageStorage(
        ...    hosts=["localhost:9200"]
        ... )
        >>> results = storage.get_ecflow_client_messages(
        ...     node_name="/globalchartos/00/deterministic/base/006/wmc_hgt_500_sep_006",
        ...    ecflow_host="login_b01",
        ...    ecflow_port="31071",
        ...    ecf_date=pd.to_datetime("2021-04-01")
        ... )
        >>> len(list(results))
        10

        Parameters
        ----------
        node_name:
            node path, variable ``ECF_NAME`` in ecFlow. such as "/grapes_meso_3km_v5_0/cold/00/model/fcst"
        ecflow_host:
            ecflow server host, such as "login_b01"
        ecflow_port:
            ecflow server port, such as "31071"
        ecf_date:
            search date
        index_ecf_date:
            index date, same as ``ecf_date`` in default.
            Used when index is not generated from ``ECF_DATE``.
        engine:
            source engine:
        size:
             messages count for one search request to ElasticSearch.

        Returns
        -------
        typing.Iterable[EcflowClientMessage]
            ecflow client command messages.
        """
        if engine is None:
            engine = nwpc_message_tool.source.ecflow_client

        if index_ecf_date is None:
            index_ecf_date = ecf_date

        query_body = engine.get_query_body(
            node_name=node_name,
            ecflow_host=ecflow_host,
            ecflow_port=ecflow_port,
            ecf_date=ecf_date,
        )

        search_from = 0
        total = np.iinfo(np.int16).max
        pbar = None

        indexes = engine.get_index(index_ecf_date)
        index = ",".join(indexes)

        search_from = 0
        total = np.iinfo(np.int16).max
        while search_from < total:
            res = self._get_result(
                index=index,
                query_body=query_body,
                search_from=search_from,
                search_size=size,
            )
            current_total = res['hits']['total']['value']
            if current_total < total:
                total = current_total
                if self.debug:
                    logger.info(f"found results: {total}")
                if self.show_progress:
                    pbar = tqdm(total=total)
            search_from += len(res['hits']['hits'])
            current_count = len(res["hits"]["hits"])
            if pbar is not None:
                pbar.update(current_count)
            for hit in res['hits']['hits']:
                yield engine.load_message(hit["_source"])

        if pbar is not None:
            pbar.close()

    def _get_result(
            self,
            index: str,
            query_body: typing.Dict,
            search_from: int,
            search_size: int,
            scroll = None
    ) -> typing.Dict:
        # logger.debug(f"searching from {search_from} with size {search_size}...")
        search_body = {
            "size": search_size,
            "from": search_from,
        }
        search_body.update(**query_body)
        # logger.debug(f"search body: {search_body}")
        res = self.client.search(
            index=index,
            body=search_body,
            scroll=scroll,
        )
        return res

    def _get_result_scroll(
            self,
            scroll: str,
            scroll_id: str,
    ) -> typing.Dict:
        res = self.client.scroll(
            scroll=scroll,
            scroll_id=scroll_id
        )
        return res

    def save_production_standard_time_message(
            self,
            system: str,
            production_type: str,
            production_stream: str,
            production_name: str,
            start_hours: typing.List,
    ):
        index = "prod-standard-time"
        message_id = f"{system}.{production_stream}.{production_type}.{production_name}"
        self.client.index(
            index,
            body={
                "app": "nwpc-message-tool",
                "type": "prduction-standard-time",
                "time": datetime.datetime.now(),
                "data": {
                    "system": system,
                    "stream": production_stream,
                    "type": production_type,
                    "name": production_name,
                    "start_hours": start_hours,
                }
            },
            id=message_id,
        )

    def get_production_standard_time_message(
            self,
            system: str,
            production_type: str = None,
            production_stream: str = None,
            production_name: str = None,
            engine = nwpc_message_tool.source.production.nwpc_message.production_standard_time
    ) -> typing.Iterable[ProductionStandardTimeMessage]:
        """
        Get standard time message for production from ElasticSearch

        Examples
        --------

        >>> import pandas as pd
        >>> from nwpc_message_tool import EsMessageStorage
        >>> storage = EsMessageStorage(
        ...    hosts=["localhost:9200"]
        ... )
        >>> results = storage.get_production_standard_time_message(
        ...     system="grapes_gfs_gmf",
        ...     production_stream="oper",
        ...     production_type="grib2",
        ...     production_name="orig"
        ... )
        >>> next(results)
        <nwpc_message_tool.message.production.ProductionStandardTimeMessage at 0x1c8a26fffd0>

        Parameters
        ----------
        system:
            system which generates the product, such as "grapes_gfs_gmf"
        production_type:
            type of production, such as "grib2"
        production_stream:
            stream of production, such as "oper"
        production_name:
            name of production, such as "orig"
        engine:
            source engine

        Returns
        -------
        typing.Iterable[ProductionStandardTimeMessage]
            production stardard time messages
        """
        query_body = engine.get_query_body(
            system=system,
            production_stream=production_stream,
            production_type=production_type,
            production_name=production_name,
        )

        search_from = 0
        total = np.iinfo(np.int16).max

        index = engine.get_index()
        search_from = 0
        total = np.iinfo(np.int16).max
        while search_from < total:
            res = self._get_result(
                index=index,
                query_body=query_body,
                search_from=search_from,
                search_size=10,
            )
            current_total = res['hits']['total']['value']
            if current_total < total:
                total = current_total
                logger.info(f"[{system}] found results: {total}")
            search_from += len(res['hits']['hits'])
            current_count = len(res["hits"]["hits"])
            for hit in res['hits']['hits']:
                yield engine.load_message(hit["_source"])


def get_es_message_storage(
        storage_name: str=None,
        config_file=None,
        **kwargs
) -> EsMessageStorage:
    """
    Get EsMessageStorage from config file.

    Default config locates in ``${USER}/.config/nwpc-oper/nwpc-message-tool.yaml``

    Parameters
    ----------
    storage_name :
        storage name, kye in "storage".
    config_file :
        config file path
    kwargs

    Returns
    -------
    EsMessageStorage
    """
    config = load_config(config_file)
    storage_map = config["storage"]
    if storage_name is None:
        storage_name = config["default_storage"]
    storage_record = storage_map[storage_name]

    hosts = storage_record["auth"]["hosts"]
    return EsMessageStorage(
        hosts=hosts,
        **kwargs
    )
