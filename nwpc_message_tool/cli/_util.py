import datetime
import typing
import pandas as pd


def parse_start_time(start_time: str, start_time_freq: str = "") -> (
        datetime.datetime or
        typing.Tuple[datetime.datetime] or
        typing.List[datetime.datetime]
):
    if "/" in start_time:
        token = start_time.split("/")
        start_time = tuple(datetime.datetime.strptime(t, "%Y%m%d%H") for t in token)
        if start_time_freq == "":
            return start_time
        start_time = list(pd.date_range(start=start_time[0], end=start_time[1], freq=start_time_freq))
    elif "," in start_time:
        token = start_time.split(",")
        start_time = list(datetime.datetime.strptime(t, "%Y%m%d%H") for t in token)
    else:
        start_time = datetime.datetime.strptime(start_time, "%Y%m%d%H")
    return start_time
