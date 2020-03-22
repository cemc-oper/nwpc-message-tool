import typing

import pandas as pd

from nwpc_message_tool.presenter.presenter import Presenter


class PrintPresenter(Presenter):
    def __init__(self):
        super(PrintPresenter, self).__init__()

    def show(self, df: pd.DataFrame):
        df.start_time = df.start_time.apply(lambda x: x.strftime("%Y%m%d%H"))
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print(df)
        print(f"Latest time: {df.time.max()}")
