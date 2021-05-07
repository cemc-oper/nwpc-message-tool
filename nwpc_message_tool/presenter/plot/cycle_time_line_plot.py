import typing
import pathlib

import pandas as pd

from bokeh.models import (
    ColumnDataSource,
    HoverTool,
    DatetimeTickFormatter,
)
from bokeh.io import output_file, output_notebook, show
from bokeh.plotting import figure, Figure

from nwpc_message_tool.presenter.presenter import Presenter
from nwpc_message_tool._type import StartTimeType


class CycleTimeLinePlotPresenter(Presenter):
    def __init__(
            self,
            system: str,
            start_time: StartTimeType,
            output_type=("file",),
            output_path: typing.Optional[typing.Union[str, pathlib.Path]]=None,
    ):
        super(CycleTimeLinePlotPresenter, self).__init__()
        self.system = system
        self.start_time = start_time
        self.output_type = output_type
        self.output_path = output_path
        if output_type is not None:
            if "file" in output_type:
                output_file(self.output_path)
            elif "notebook" in output_type:
                output_notebook()

    def show(
            self,
            production_time_table: pd.DataFrame,
            production_standard_time_table: pd.DataFrame
    ):
        p = self.generate_plot(production_time_table, production_standard_time_table)
        show(p)

    def generate_plot(
            self,
            production_time_table: pd.DataFrame,
            production_standard_time_table: pd.DataFrame
    ) -> Figure:
        source_table = self._process_time_table(production_time_table)
        standard_table = self._process_standard_time_table(production_standard_time_table)

        p = self._get_figure(source_table, standard_table)
        return p

    def _process_time_table(self, df):
        df["time_str"] = df["time"].map(lambda x: x.isoformat())
        return df

    def _process_standard_time_table(self, df):
        standard_table = df[df["start_hour"] == f"{self.start_time.hour:02}"].copy()
        standard_table["upper_time"] = standard_table["upper_duration"].map(lambda x: pd.Timedelta(x)) + self.start_time
        standard_table["lower_time"] = standard_table["lower_duration"].map(lambda x: pd.Timedelta(x)) + self.start_time
        standard_table["upper_time_str"] = standard_table["upper_time"].map(lambda x: x.isoformat())
        standard_table["lower_time_str"] = standard_table["lower_time"].map(lambda x: x.isoformat())
        return standard_table

    def _get_figure(self, production_table: pd.DataFrame, standard_table: pd.DataFrame) -> Figure:
        production_source = ColumnDataSource(production_table)
        standard_source = ColumnDataSource(standard_table)

        tools = "pan,wheel_zoom,box_zoom,reset,save"

        hover_tool = HoverTool(
            tooltips=[
                ("index", "$index"),
                ("(x,y)", "($x, $y)"),
                ("forecast hour", "@forecast_hour"),
                ("time", "@time_str"),
                ("upper_time", "@upper_time_str"),
                ("lower_time", "@lower_time_str")
            ],
            formatters={
                "@time": "datetime"
            },
        )

        p = figure(
            plot_width=1000,
            plot_height=500,
            y_axis_type="datetime",
            title=f"Production time for {self.system} ({self.start_time})",
            tools=tools,
        )

        p.add_tools(hover_tool)

        p.yaxis.formatter = DatetimeTickFormatter(
            minsec=['%H:%M:%S'],
            minutes=['%H:%M:%S'],
            hourmin=['%H:%M:%S'],
            hours=['%H:%M:%S']
        )

        p.varea(
            y2="upper_time",
            y1="lower_time",
            x="forecast_hour",
            source=standard_source,
            color="green",
            alpha=0.2,
        )

        p.line(
            y="time",
            x="forecast_hour",
            source=production_source,
            color="blue",
        )

        p.xaxis.axis_label = "Forecast hour"
        p.yaxis.axis_label = "Clock (UTC)"

        p.title.text_font_size = '16pt'
        p.xaxis.axis_label_text_font_size = '14pt'
        p.yaxis.axis_label_text_font_size = '14pt'
        p.xaxis.major_label_text_font_size = "14pt"
        p.yaxis.major_label_text_font_size = "14pt"

        return p
