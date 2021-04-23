import pandas as pd

from nwpc_message_tool.source.production.nmc_monitor.production import (
    load_message,
    get_index,
    get_query_body
)
from nwpc_message_tool.message import EventStatus


def test_load_message():
    doc = {
        "source": "nwpc_grapes_meso_10km",
        "type": "prod_grib",
        "status": "0",
        "datetime": "2020-03-04T11:37:04+08:00",
        "fileName": "rmf.gra.2020030400001.grb2",
        "startTime": "2020-03-04T00:00:00Z",
        "forecastTime": "001"
    }

    message = load_message(doc)

    assert message.message_type == "production"
    assert message.system == "nwpc_grapes_meso_10km"
    assert message.stream == "oper"
    assert message.production_type == "grib2"
    assert message.production_name == "orig"
    assert message.event == "before_upload"
    assert message.status == EventStatus.Complete
    assert message.time == pd.Timestamp(
        year=2020,
        month=3,
        day=4,
        hour=11,
        minute=37,
        second=4,
        tz='Asia/Shanghai',
    )
    assert message.start_time == pd.Timestamp(
        year=2020,
        month=3,
        day=4,
        hour=0,
        minute=0,
        second=0,
        tz='GMT',
    )
    assert message.forecast_time == pd.Timedelta(
        hours=1,
    )

def test_get_index():
    start_time = pd.to_datetime("2021-04-23 00:00")
    index_list = get_index(start_time)
    assert index_list == [
        "nmc-prod-2021-04"
    ]

    start_time = pd.date_range(
        pd.to_datetime("2021-01-01"),
        pd.to_datetime("2021-04-23"),
        freq="D"
    )
    index_list = get_index(start_time)
    assert set(index_list) == {
        "nmc-prod-2021-01",
        "nmc-prod-2021-02",
        "nmc-prod-2021-03",
        "nmc-prod-2021-04"
    }

    start_time = (
        pd.to_datetime("2021-01-01"),
        pd.to_datetime("2021-04-23")
    )
    index_list = get_index(start_time)
    assert set(index_list) == {
        "nmc-prod-2021-01",
        "nmc-prod-2021-02",
        "nmc-prod-2021-03",
        "nmc-prod-2021-04"
    }


def test_get_query_body():
    system = "grapes_gfs_gmf"
    start_time = pd.to_datetime("2021-04-23 00:00:00")

    body = get_query_body(
        system=system,
        start_time=start_time,
    )

    assert body == {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"source": system}},
                    {"term": {"startTime": start_time.isoformat()}},
                ]
            },
        },
        "sort": [{"datetime": "asc"}]
    }

    body = get_query_body(
        system=system,
    )

    assert body == {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"source": system}},
                ]
            },
        },
        "sort": [{"datetime": "asc"}]
    }


def test_get_query_body_start_time_list():
    system = "grapes_gfs_gmf"
    start_time = pd.date_range(
        pd.to_datetime("2021-04-20"),
        pd.to_datetime("2021-04-23"),
        freq="D"
    )

    body = get_query_body(
        system=system,
        start_time=start_time,
    )

    assert body == {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"source": system}},
                    {"terms": {"startTime": [
                        pd.to_datetime("2021-04-20").isoformat(),
                        pd.to_datetime("2021-04-21").isoformat(),
                        pd.to_datetime("2021-04-22").isoformat(),
                        pd.to_datetime("2021-04-23").isoformat(),
                    ]}},
                ]
            },
        },
        "sort": [{"datetime": "asc"}]
    }


def test_get_query_body_start_time_tuple():
    system = "grapes_gfs_gmf"
    start_time = (
        pd.to_datetime("2021-04-20"),
        pd.to_datetime("2021-04-23")
    )


    body = get_query_body(
        system=system,
        start_time=start_time,
    )

    assert body == {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"source": system}},
                    {"range": {"startTime": {
                        "gte": pd.to_datetime("2021-04-20").isoformat(),
                        "lte": pd.to_datetime("2021-04-23").isoformat(),
                    }}},
                ]
            },
        },
        "sort": [{"datetime": "asc"}]
    }
