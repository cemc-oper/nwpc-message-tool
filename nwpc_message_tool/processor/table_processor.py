import typing

import pandas as pd
from loguru import logger

from nwpc_message_tool._util import get_hour
from nwpc_message_tool.message import ProductionEventMessage


class TableProcessor(object):
    def __init__(
            self,
            columns: typing.List[str] or None = None,
            keep_duplicates: bool or str = "first",
    ):
        self.columns = [
            "system",
            "stream",
            "type",
            "name",
            "start_time",
            "forecast_hour",
            "time",
            "event",
            "status",
        ]
        if columns is not None:
            self.columns = columns
        if keep_duplicates is False:
            self.drop_duplicates = False
        elif keep_duplicates is True:
            self.drop_duplicates = True
            self.keep_duplicates = "first"
        elif isinstance(keep_duplicates, str):
            self.drop_duplicates = True
            self.keep_duplicates = keep_duplicates

    def process_messages(self, messages: typing.Iterable[ProductionEventMessage]) -> pd.DataFrame:
        df = pd.DataFrame(columns=self.columns)
        for result in messages:
            hours = get_hour(result)
            message_time = result.time.ceil("S")
            current_df = pd.DataFrame(
                {
                    "system": [result.system],
                    "stream": [result.stream],
                    "type": [result.production_type],
                    "name": [result.production_name],
                    "start_time": [pd.to_datetime(result.start_time)],
                    "forecast_hour": [hours],
                    "time": [message_time],
                    "event": [result.event],
                    "status": [result.status.name],
                },
                columns=self.columns,
                index=[f"{result.start_time.strftime('%Y%m%d%H')}+{hours:03}"]
            )
            df = df.append(current_df)

        if "time" in self.columns:
            df["time"] = pd.to_datetime(df["time"], utc=True)
        if "forecast_hour" in self.columns:
            df["forecast_hour"] = pd.to_numeric(df["forecast_hour"])
        if "start_time" in self.columns:
            df["start_time"] = pd.to_datetime(df["start_time"], utc=True)

        logger.info(f"searching...done")

        logger.info(f"get {len(df)} results")
        df = df.sort_index()

        if self.drop_duplicates:
            df = df[~df.index.duplicated(keep=self.keep_duplicates)]

        return df
