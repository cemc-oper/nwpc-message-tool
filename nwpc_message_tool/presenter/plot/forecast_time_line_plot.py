import typing
import pathlib

import pandas as pd

from bokeh.models import (
    ColumnDataSource,
    HoverTool,
    DatetimeTickFormatter,
    BoxAnnotation
)
from bokeh.io import output_file, output_notebook, show
from bokeh.plotting import figure, Figure

from nwpc_message_tool.presenter.presenter import Presenter


class ForecastTimeLinePlotPresenter(Presenter):
    def __init__(
            self,
            system: str,
            start_hour: int,
            forecast_hour: int,
            output_type=("file",),
            output_path: typing.Optional[typing.Union[str, pathlib.Path]]=None,
    ):
        super(ForecastTimeLinePlotPresenter, self).__init__()
        self.system = system
        self.start_hour = start_hour
        self.forecast_hour = forecast_hour
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
            production_time_table,
            production_standard_time_table
    ) -> Figure:
        source_table = self._process_time_table(production_time_table)
        standard_forecast_time = self._get_standard_forecast_time(production_standard_time_table)

        p = self._get_figure(source_table, standard_forecast_time)
        return p

    def _process_time_table(self, df):
        df["time_str"] = df["time"].map(lambda x: x.isoformat())
        df["clock"] = (df["time"] - df[
            "start_time"]) + pd.Timedelta(hours=self.start_hour)
        df["start_hour"] = df["start_time"].map(lambda x: x.hour)
        source_table = df[df["start_hour"] == self.start_hour]

        return source_table

    def _get_standard_forecast_time(self, df):
        return pd.to_timedelta(
            df[
                (df["forecast_hour"] == self.forecast_hour) &
                (df["start_hour"] == f"{self.start_hour:02}")
                ]["upper_duration"]
        ).item()

    def _get_figure(self, source_table, standard_forecast_time) -> Figure:
        current_source = ColumnDataSource(source_table)

        tools = "pan,wheel_zoom,box_zoom,reset,save"
        hover_tool = HoverTool(
            tooltips=[
                ("start_time", "@time_str"),
                ("clock", "@clock"),
            ],
            formatters={
                "@start_time": "datetime",
                "@clock": "datetime",
            },
        )

        p = figure(
            plot_width=1000,
            plot_height=500,
            x_axis_type="datetime",
            title=f"Production time for {self.system} ({self.forecast_hour:03}h)",
            tools=tools,
        )

        upper_box = BoxAnnotation(
            top=standard_forecast_time + pd.Timedelta(hours=self.start_hour),
            fill_alpha=0.1,
            fill_color='green',
        )
        p.add_layout(upper_box)

        p.add_tools(hover_tool)

        p.xaxis.formatter = DatetimeTickFormatter(
            minsec=['%Y-%m-%d'],
            minutes=['%Y-%m-%d'],
            hourmin=['%Y-%m-%d'],
            hours=['%Y-%m-%d']
        )

        p.yaxis.formatter = DatetimeTickFormatter(
            minsec=['%H:%M:%S'],
            minutes=['%H:%M:%S'],
            hourmin=['%H:%M:%S'],
            hours=['%H:%M:%S'],
            days=['%H:%M:%S'],
        )

        p.circle(
            y="clock",
            x="time",
            source=current_source,
            color="blue",
        )

        p.xaxis.axis_label = "Start Time"
        p.yaxis.axis_label = "Clock (Hour)"

        p.title.text_font_size = '16pt'
        p.xaxis.axis_label_text_font_size = '14pt'
        p.yaxis.axis_label_text_font_size = '14pt'
        p.xaxis.major_label_text_font_size = "14pt"
        p.yaxis.major_label_text_font_size = "14pt"
        return p
