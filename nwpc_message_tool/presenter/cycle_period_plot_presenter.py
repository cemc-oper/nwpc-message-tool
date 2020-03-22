import pandas as pd
from bokeh.io import output_file, output_notebook, show
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure
from bokeh.models.formatters import DatetimeTickFormatter

from .presenter import Presenter


class CyclePeriodPlotPresenter(Presenter):
    def __init__(
            self,
            system="",
            output_type=("file",),
            output_path=None,
    ):
        super(CyclePeriodPlotPresenter, self).__init__()
        self.system = system
        self.output_type = output_type
        self.output_path = output_path
        if "file" in output_type:
            output_file(self.output_path)
        elif "notebook" in output_type:
            output_notebook()

    def show(self, table_data: pd.DataFrame):
        table_data = self._append_column(table_data)
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
        p.hbar(y="st", left='min', right='max', height=0.4, source=source)

        p.xaxis.axis_label = "Time Clock"

        show(p)

    def _append_column(self, table_data: pd.DataFrame):
        table_data["st"] = table_data.start_time.apply(lambda x: x.strftime("%Y%m%d%H"))
        table_data["time_length"] = table_data["time"].apply(lambda x: (x + pd.Timedelta(hours=8)).time())
        return table_data
