import pandas as pd

from nwpc_message_tool.message import ProductionEventMessage, EventStatus


def load_message(doc: dict) -> ProductionEventMessage:
    status = doc["status"]
    if status == "0":
        status = EventStatus.Complete
    system = doc["source"]

    message = ProductionEventMessage(
        message_type="production",
        time=pd.Timestamp(doc["datetime"]),
        system=system,
        stream="oper",
        production_type="grib2",
        production_name="orig",
        event="before_upload",
        status=status,
        start_time=pd.Timestamp(doc["startTime"]),
        forecast_time=pd.Timedelta(f"{doc['forecastTime']}h"),
    )
    return message
