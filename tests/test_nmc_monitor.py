import pandas as pd

from nwpc_message_tool.nmc_monitor import load_message
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