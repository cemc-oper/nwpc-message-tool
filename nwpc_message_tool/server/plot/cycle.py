import datetime
import typing

import pandas as pd
from bokeh.models import ColumnDataSource, HoverTool, DatetimeTickFormatter
from bokeh.plotting import figure, Figure
from flask import current_app

from nwpc_message_tool.source.production import nwpc_message
from nwpc_message_tool.processor import TableProcessor
from nwpc_message_tool.storage import EsMessageStorage
from nwpc_message_tool._type import StartTimeType


def get_cycle_time_line(
        start_time: StartTimeType,
        system: str,
        production_stream: str="oper",
        production_type: str="grib2",
        production_name: str="orig",
) -> Figure:
    engine = nwpc_message

    hosts = current_app.config["SERVER_CONFIG"]["message_storage"]["hosts"]

    system = engine.fix_system_name(system)

    client = EsMessageStorage(
        hosts=hosts,
    )
    results = client.get_production_messages(
        system=system,
        production_stream=production_stream,
        production_type=production_type,
        production_name=production_name,
        start_time=start_time,
        engine=engine.production,
    )

    processor = TableProcessor()
    table = processor.process_messages(results)

    table["time_str"] = table["time"].map(lambda x: x.isoformat())

    standard_time_messages = list(client.get_production_standard_time_message(
        system=system,
        production_stream=production_stream,
        production_type=production_type,
        production_name=production_name,
        engine=nwpc_message.production_standard_time,
    ))

    standard_time_message = standard_time_messages[0]
    standard_time_df = pd.DataFrame([
        {**time, "start_hour": i["start_hour"]}
        for i in standard_time_message.start_hours
        for time in i["times"]
    ])

    current_standard = standard_time_df[standard_time_df["start_hour"] == f"{start_time.hour:02}"].copy()
    current_standard["upper_time"] = current_standard["upper_duration"].map(lambda x: pd.Timedelta(x)) + start_time
    current_standard["lower_time"] = current_standard["lower_duration"].map(lambda x: pd.Timedelta(x)) + start_time
    current_standard["upper_time_str"] = current_standard["upper_time"].map(lambda x: x.isoformat())
    current_standard["lower_time_str"] = current_standard["lower_time"].map(lambda x: x.isoformat())

    current_source = ColumnDataSource(table)
    standard_source = ColumnDataSource(current_standard)

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
        title=f"Production time for {system} ({start_time})",
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
        source=current_source,
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
