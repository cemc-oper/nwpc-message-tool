import pandas as pd
import numpy as np
from bokeh.io import output_file, output_notebook, show
from bokeh.plotting import figure
from bokeh.models import LinearColorMapper, HoverTool
import colorcet as cc

from .presenter import Presenter


class StepGridPlotPresenter(Presenter):
    def __init__(
            self,
            system="",
            output_type=("file",),
            output_path=None,
    ):
        super(StepGridPlotPresenter, self).__init__()
        self.system = system
        self.output_type = output_type
        self.output_path = output_path
        if "file" in output_type:
            output_file(self.output_path)
        elif "notebook" in output_type:
            output_notebook()

    def show(self, table_data: pd.DataFrame):
        table_data = self._append_column(table_data)

        forecast_hours = table_data.fh.unique().astype(int)
        forecast_hours.sort()
        forecast_hours = forecast_hours.astype(str)
        start_times = table_data.st.unique().astype(int)
        start_times.sort()
        start_times = start_times.astype(str)

        cmap = LinearColorMapper(
            palette=cc.CET_L18,
            low=min(table_data['clocks']),
            high=max(table_data['clocks']),
        )

        p = figure(
            title=f"Production time of each step for {self.system}",
            plot_width=30 * len(forecast_hours),
            plot_height=35 * len(start_times),
            x_range=forecast_hours,
            y_range=start_times,
        )

        r = p.rect(
            "fh", "st", 0.9, 0.9,
            source=table_data,
            fill_alpha=0.6,
            fill_color={
                "field": "clocks",
                "transform": cmap,
            },
        )

        p.add_tools(HoverTool(
            tooltips=[
                ("time", "@time_string")
            ],
        ))

        show(p)

    def _append_column(self, table_data: pd.DataFrame):
        table_data["fh"] = table_data.forecast_hour.astype(str)
        table_data["st"] = table_data.start_time.apply(lambda x: x.strftime("%Y%m%d%H"))
        table_data["clock"] = table_data.time - table_data.start_time
        table_data["clocks"] = table_data['clock'].astype(np.int64) / int(1e6)
        table_data["time_string"] = table_data["time"].apply(lambda x: x.strftime("%H:%M:%S"))
        return table_data
