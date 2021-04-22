import pandas as pd

from nwpc_message_tool.source.ecflow_client import (
    load_message
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
