import typing
import pathlib

import pandas as pd
from bokeh.io import (
    output_file,
    output_notebook,
    show,
)
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure, Figure
from bokeh.models.formatters import DatetimeTickFormatter

from nwpc_message_tool.presenter.presenter import Presenter


class PeriodBarPlotPresenter(Presenter):
    """
    Bar plot for production time period for each cycle.

    Attributes
    ----------
    system : str
        system name which is shown in title.
    output_type : typing.Tuple[str]
        output types, supported types:

        - ``file``: save to a file
        - ``notebook``: shown in Jupyter Notebook
    output_path :
        output file path, used when ``file`` is in ``output_type``
    """
    def __init__(
            self,
            system: str="",
            output_type: typing.Tuple[str]=("file",),
            output_path: typing.Optional[typing.Union[pathlib.Path, str]]=None,
    ):
        super(PeriodBarPlotPresenter, self).__init__()
        self.system = system
        self.output_type = output_type
        self.output_path = output_path
        if "file" in output_type:
            output_file(self.output_path)
        elif "notebook" in output_type:
            output_notebook()

    def show(
            self,
            table_data: pd.DataFrame
    ):
        p = self.generate_plot(table_data)
        show(p)

    def generate_plot(self, table_data: pd.DataFrame) -> Figure:
        table_data = self._process_table(table_data)
        grouped = table_data["time_length"].groupby(table_data["st"])
        grouped_table = grouped.agg(["min", "max"])

        source = ColumnDataSource(grouped_table)

        p = figure(
            y_range=grouped,
            plot_width=1600,
            plot_height=50 * len(grouped_table),
            x_axis_type="datetime",
            title=f"Cycle period of production for {self.system}"
        )
        p.xaxis.formatter = DatetimeTickFormatter(
            minsec=['%H:%M:%S'],
            minutes=['%H:%M:%S'],
            hourmin=['%H:%M:%S'],
            hours=['%H:%M:%S']
        )
        p.hbar(
            y="st",
            left='min',
            right='max',
            height=0.4,
            source=source
        )

        p.xaxis.axis_label = "Time Clock"
        return p

    def _process_table(self, table_data: pd.DataFrame):
        table_data["st"] = table_data.start_time.apply(
            lambda x: x.strftime("%Y%m%d%H")
        )
        table_data["time_length"] = table_data["time"].apply(
            lambda x: (x + pd.Timedelta(hours=8)).time()
        )
        return table_data
