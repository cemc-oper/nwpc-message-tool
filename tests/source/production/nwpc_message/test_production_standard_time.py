import pandas as pd

from nwpc_message_tool.source.production.nwpc_message.production_standard_time import (
    load_message,
    get_query_body,
    get_index
)


def test_load_message():
    doc = {
        "app": "nwpc-message-tool",
        "type": "prduction-standard-time",
        "time": "2020-12-28T16:31:52.448865",
        "data": {
            "system": "grapes_gfs_gmf",
            "stream": "oper",
            "type": "grib2",
            "name": "orig",
            "start_hours": [
                {
                    "start_hour": "00",
                    "times": [
                        {
                            "forecast_hour": 0,
                            "upper_duration": "P0DT4H53M54S",
                            "lower_duration": "P0DT4H34M57S"
                        },
                        {
                            "forecast_hour": 3,
                            "upper_duration": "P0DT4H56M20S",
                            "lower_duration": "P0DT4H36M3S"
                        }
                    ]
                },
                {
                    "start_hour": "12",
                    "times": [
                        {
                            "forecast_hour": 0,
                            "upper_duration": "P0DT4H42M40S",
                            "lower_duration": "P0DT4H32M1S"
                        },
                        {
                            "forecast_hour": 3,
                            "upper_duration": "P0DT4H43M34S",
                            "lower_duration": "P0DT4H32M47S"
                        },
                    ]
                },
            ]
        }
    }

    message = load_message(doc)

    assert message.message_type == "production-standard"
    assert message.time == pd.to_datetime("2020-12-28T16:31:52.448865")
    assert message.stream == "oper"
    assert message.production_name == "orig"
    assert message.production_type == "grib2"
    assert message.system == "grapes_gfs_gmf"

    assert message.start_hours == [
        {
            "start_hour": "00",
            "times": [
                {
                    "forecast_hour": 0,
                    "upper_duration": "P0DT4H53M54S",
                    "lower_duration": "P0DT4H34M57S"
                },
                {
                    "forecast_hour": 3,
                    "upper_duration": "P0DT4H56M20S",
                    "lower_duration": "P0DT4H36M3S"
                }
            ]
        },
        {
            "start_hour": "12",
            "times": [
                {
                    "forecast_hour": 0,
                    "upper_duration": "P0DT4H42M40S",
                    "lower_duration": "P0DT4H32M1S"
                },
                {
                    "forecast_hour": 3,
                    "upper_duration": "P0DT4H43M34S",
                    "lower_duration": "P0DT4H32M47S"
                },
            ]
        },
    ]


def test_get_index():
    index = get_index()
    assert index == "prod-standard-time"


def test_get_query_body():
    system = "grapes_gfs_gmf"
    production_type = "grib2"
    production_stream = "oper"
    production_name = "orig"

    body = get_query_body(
        system=system,
        production_type=production_type,
        production_stream=production_stream,
        production_name=production_name
    )

    assert body == {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"data.system": system}},
                    {"term": {"data.type": production_type}},
                    {"term": {"data.stream": production_stream}},
                    {"term": {"data.name": production_name}},
                ]
            },
        }
    }

    body = get_query_body(
        system=system,
        production_type=production_type,
        production_stream=production_stream,
    )

    assert body == {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"data.system": system}},
                    {"term": {"data.type": production_type}},
                    {"term": {"data.stream": production_stream}},
                ]
            },
        }
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
        }
    }
