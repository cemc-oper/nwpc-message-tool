import typing

import pandas as pd
from loguru import logger

from nwpc_message_tool._util import get_hour
from nwpc_message_tool.message import ProductionEventMessage


class TableProcessor(object):
    def __init__(self):
        pass

    def process_messages(self, messages: typing.Iterable[ProductionEventMessage]) -> pd.DataFrame:
        df = pd.DataFrame(columns=["start_time", "forecast_hour", "time"])
        for result in messages:
            hours = get_hour(result)
            message_time = result.time.ceil("S")
            current_df = pd.DataFrame(
                {
                    "start_time": [f"{result.start_time.strftime('%Y%m%d%H')}"],
                    "forecast_hour": [hours],
                    "time": [message_time]
                },
                columns=["start_time", "forecast_hour", "time"],
                index=[f"{result.start_time.strftime('%Y%m%d%H')}+{hours:03}"]
            )
            df = df.append(current_df)
        df["time"] = pd.to_datetime(df["time"])
        df["forecast_hour"] = pd.to_numeric(df["forecast_hour"])
        df["start_time"] = df["start_time"].astype(str)
        logger.info(f"searching...done")

        logger.info(f"get {len(df)} results")
        df = df.sort_index()
        return df
