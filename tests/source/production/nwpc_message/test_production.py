import pandas as pd

from nwpc_message_tool.source.production.nwpc_message.production import (
    load_message,
    get_query_body,
    get_index
)
from nwpc_message_tool.message import (
    EventStatus,
)

def test_load_message():
    doc = {
        "app": "nwpc-message-client",
        "type": "production",
        "time": "2021-04-22T05:52:38.591782905Z",
        "data": {
            "event": "storage",
            "forecast_time": "036h",
            "name": "orig",
            "start_time": "2021-04-22T00:00:00Z",
            "status": 1,
            "stream": "oper",
            "system": "grapes_meso_3km",
            "type": "grib2"
        }
    }

    message = load_message(doc)

    assert message.message_type == "production"
    assert message.time == pd.to_datetime("2021-04-22T05:52:38.591782905Z")
    assert message.event == "storage"
    assert message.stream == "oper"
    assert message.production_name == "orig"
    assert message.production_type == "grib2"
    assert message.system == "grapes_meso_3km"

    assert message.start_time == pd.to_datetime("2021-04-22T00:00:00Z")
    assert message.forecast_time == pd.Timedelta(hours=36)

    assert message.status == EventStatus.Complete


def test_get_index():
    start_time = pd.to_datetime("2021-04-23 00:00")
    index_list = get_index(start_time)
    assert index_list == [
        "2021-04"
    ]

    start_time = pd.date_range(
        pd.to_datetime("2021-01-01"),
        pd.to_datetime("2021-04-23"),
        freq="D"
    )
    index_list = get_index(start_time)
    assert set(index_list) == {
        "2021-01",
        "2021-02",
        "2021-03",
        "2021-04"
    }

    start_time = (
        pd.to_datetime("2021-01-01"),
        pd.to_datetime("2021-04-23")
    )
    index_list = get_index(start_time)
    assert set(index_list) == {
        "2021-01",
        "2021-02",
        "2021-03",
        "2021-04"
    }


def test_get_query_body():
    system = "grapes_gfs_gmf"
    production_type = "grib2"
    production_stream = "oper"
    production_name = "orig"
    start_time = pd.to_datetime("2021-04-23 00:00:00")
    forecast_time = "012h"

    body = get_query_body(
        system=system,
        production_type=production_type,
        production_stream=production_stream,
        production_name=production_name,
        start_time=start_time,
        forecast_time=forecast_time
    )

    assert body == {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"data.system": system}},
                    {"term": {"data.type": production_type}},
                    {"term": {"data.stream": production_stream}},
                    {"term": {"data.name": production_name}},
                    {"term": {"data.start_time": start_time.isoformat()}},
                    {"term": {"data.forecast_time": forecast_time}}
                ]
            },
        },
        "sort": [{"time": "asc"}]
    }

    body = get_query_body(
        system=system,
        production_type=production_type,
        production_stream=production_stream,
        production_name=production_name,
        start_time=start_time,
    )

    assert body == {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"data.system": system}},
                    {"term": {"data.type": production_type}},
                    {"term": {"data.stream": production_stream}},
                    {"term": {"data.name": production_name}},
                    {"term": {"data.start_time": start_time.isoformat()}},
                ]
            },
        },
        "sort": [{"time": "asc"}]
    }

    body = get_query_body(
        system=system,
    )

    assert body == {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"data.system": system}},
                ]
            },
        },
        "sort": [{"time": "asc"}]
    }


def test_get_query_body_start_time_list():
    system = "grapes_gfs_gmf"
    production_type = "grib2"
    production_stream = "oper"
    production_name = "orig"
    forecast_time = "012h"

    start_time = pd.date_range(
        pd.to_datetime("2021-04-20"),
        pd.to_datetime("2021-04-23"),
        freq="D"
    )

    body = get_query_body(
        system=system,
        production_type=production_type,
        production_stream=production_stream,
        production_name=production_name,
        start_time=start_time,
        forecast_time=forecast_time
    )

    assert body == {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"data.system": system}},
                    {"term": {"data.type": production_type}},
                    {"term": {"data.stream": production_stream}},
                    {"term": {"data.name": production_name}},
                    {"terms": {"data.start_time": [
                        pd.to_datetime("2021-04-20").isoformat(),
                        pd.to_datetime("2021-04-21").isoformat(),
                        pd.to_datetime("2021-04-22").isoformat(),
                        pd.to_datetime("2021-04-23").isoformat(),
                    ]}},
                    {"term": {"data.forecast_time": forecast_time}}
                ]
            },
        },
        "sort": [{"time": "asc"}]
    }


def test_get_query_body_start_time_tuple():
    system = "grapes_gfs_gmf"
    production_type = "grib2"
    production_stream = "oper"
    production_name = "orig"
    forecast_time = "012h"

    start_time = (
        pd.to_datetime("2021-04-20"),
        pd.to_datetime("2021-04-23")
    )

    body = get_query_body(
        system=system,
        production_type=production_type,
        production_stream=production_stream,
        production_name=production_name,
        start_time=start_time,
        forecast_time=forecast_time
    )

    assert body == {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"data.system": system}},
                    {"term": {"data.type": production_type}},
                    {"term": {"data.stream": production_stream}},
                    {"term": {"data.name": production_name}},
                    {"range": {"data.start_time": {
                        "gte": pd.to_datetime("2021-04-20").isoformat(),
                        "lte": pd.to_datetime("2021-04-23").isoformat(),
                    }}},
                    {"term": {"data.forecast_time": forecast_time}}
                ]
            },
        },
        "sort": [{"time": "asc"}]
    }
