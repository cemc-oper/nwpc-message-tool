import datetime
import typing

import pandas as pd

TimeType = typing.Union[
    datetime.datetime,
    pd.Timestamp
]

StartTimeType = typing.Union[
    TimeType,
    typing.Tuple[TimeType, TimeType],
    typing.List[TimeType],
]