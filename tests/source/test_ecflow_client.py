import pandas as pd

from nwpc_message_tool.source.ecflow_client import (
    load_message,
    get_query_body,
    get_index
)


def test_load_message():
    doc = {
        "app": "nwpc-message-client",
        "type": "ecflow-client",
        "time": "2021-04-22T08:36:01.292320923Z",
        "data": {
            "args": None,
            "command": "complete",
            "ecf_date": "20210422",
            "ecf_host": "login_b06",
            "ecf_name": "/service_checker/ecflow/check_watchman",
            "ecf_port": "31071",
            "ecf_rid": "0.0",
            "ecf_tryno": "1",
            "envs": None
        }
    }

    message = load_message(doc)
    assert message.message_type == "ecflow-client"
    assert message.time == pd.to_datetime("2021-04-22T08:36:01.292320923Z")
    assert message.command == "complete"
    assert message.arguments is None
    assert message.envs is None
    assert message.ecflow_host == "login_b06"
    assert message.ecflow_port == "31071"
    assert message.node_name == "/service_checker/ecflow/check_watchman"
    assert message.node_rid == "0.0"
    assert message.try_no == 1
    assert message.ecf_date == pd.to_datetime("2021-04-22").tz_localize("UTC")


def test_get_query_body():
    node_name = "/s1/f1/t1"
    ecflow_host = "login_b01"
    ecflow_port = "31071"
    ecf_date = pd.to_datetime("2021-04-23")

    body = get_query_body(
        node_name,
        ecflow_host=ecflow_host,
        ecflow_port=ecflow_port,
        ecf_date=ecf_date
    )

    assert body == {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"data.ecf_name.keyword": node_name}},
                    {"term": {"data.ecf_host": ecflow_host}},
                    {"term": {"data.ecf_port": ecflow_port}},
                    {"term": {"data.ecf_date": "20210423"}},
                ]
            },
        },
        "sort": [{"time": "asc"}]
    }

    body = get_query_body(
        node_name,
        ecf_date=ecf_date
    )

    assert body == {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"data.ecf_name.keyword": node_name}},
                    {"term": {"data.ecf_date": "20210423"}},
                ]
            },
        },
        "sort": [{"time": "asc"}]
    }


def test_get_query_body_ecf_date_list():
    node_name = "/s1/f1/t1"
    ecflow_host = "login_b01"
    ecflow_port = "31071"

    ecf_date = pd.date_range(
        pd.to_datetime("2021-04-20"),
        pd.to_datetime("2021-04-23"),
        freq="D"
    )

    body = get_query_body(
        node_name,
        ecflow_host=ecflow_host,
        ecflow_port=ecflow_port,
        ecf_date=ecf_date
    )

    assert body == {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"data.ecf_name.keyword": node_name}},
                    {"term": {"data.ecf_host": ecflow_host}},
                    {"term": {"data.ecf_port": ecflow_port}},
                    {"terms": {"data.ecf_date": [
                        "20210420",
                        "20210421",
                        "20210422",
                        "20210423",
                    ]}},
                ]
            },
        },
        "sort": [{"time": "asc"}]
    }


def test_get_query_body_ecf_date_tuple():
    node_name = "/s1/f1/t1"
    ecflow_host = "login_b01"
    ecflow_port = "31071"
    ecf_date = (
        pd.to_datetime("2021-04-20"),
        pd.to_datetime("2021-04-23")
    )

    body = get_query_body(
        node_name,
        ecflow_host=ecflow_host,
        ecflow_port=ecflow_port,
        ecf_date=ecf_date
    )

    assert body == {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"data.ecf_name.keyword": node_name}},
                    {"term": {"data.ecf_host": ecflow_host}},
                    {"term": {"data.ecf_port": ecflow_port}},
                    {"range": {"data.ecf_date": {
                        "gte": "20210420",
                        "lte": "20210423"
                    }}},
                ]
            },
        },
        "sort": [{"time": "asc"}]
    }


def test_get_index():
    ecf_date = pd.to_datetime("2021-04-23")
    index_list = get_index(ecf_date)
    assert index_list == [
        "ecflow-client-2021-04-23"
    ]

    ecf_date = pd.date_range(
        pd.to_datetime("2021-04-20"),
        pd.to_datetime("2021-04-23"),
        freq="D"
    )
    index_list = get_index(ecf_date)
    assert index_list == [
        "ecflow-client-2021-04-20",
        "ecflow-client-2021-04-21",
        "ecflow-client-2021-04-22",
        "ecflow-client-2021-04-23"
    ]

    ecf_date = (
        pd.to_datetime("2021-04-20"),
        pd.to_datetime("2021-04-23")
    )
    index_list = get_index(ecf_date)
    assert index_list == [
        "ecflow-client-2021-04-20",
        "ecflow-client-2021-04-21",
        "ecflow-client-2021-04-22",
        "ecflow-client-2021-04-23"
    ]
