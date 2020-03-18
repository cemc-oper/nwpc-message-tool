import typing

import pandas as pd
from loguru import logger

from .message import ProductionEventMessage


class Presenter(object):
    def __init__(self):
        pass

    def process_messages(self, messages: typing.Iterable[ProductionEventMessage]):
        pass


class PrintPresenter(Presenter):
    def __init__(self):
        super(PrintPresenter, self).__init__()

    def process_messages(self, messages: typing.Iterable[ProductionEventMessage]):
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
        logger.info(f"searching...done")

        logger.info(f"get {len(df)} results")
        df = df.sort_index()
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print(df)
        print(f"Latest time: {df.time.max()}")


def get_hour(message: ProductionEventMessage) -> int:
    return int(message.forecast_time.seconds/3600) + message.forecast_time.days * 24
