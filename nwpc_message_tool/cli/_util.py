import datetime
import typing


def parse_start_time(start_time: str) -> (
        datetime.datetime or
        typing.Tuple[datetime.datetime] or
        typing.List[datetime.datetime]
):
    if "/" in start_time:
        token = start_time.split("/")
        start_time = tuple(datetime.datetime.strptime(t, "%Y%m%d%H") for t in token)
    elif "," in start_time:
        token = start_time.split(",")
        start_time = list(datetime.datetime.strptime(t, "%Y%m%d%H") for t in token)
    else:
        start_time = datetime.datetime.strptime(start_time, "%Y%m%d%H")
    return start_time
