import pandas as pd
from bokeh.models import (
    ColumnDataSource,
    HoverTool,
    DatetimeTickFormatter,
    BoxAnnotation
)
from bokeh.plotting import figure
from flask import current_app

from nwpc_message_tool import nwpc_message
from nwpc_message_tool.processor import TableProcessor
from nwpc_message_tool.storage import EsMessageStorage


def get_forecast_time_line(
        start_time,
        start_hour: int,
        forecast_hour: int,
        system: str,
        production_stream="oper",
        production_type="grib2",
        production_name="orig",
):
    engine = nwpc_message
    hosts = current_app.config["SERVER_CONFIG"]["message_storage"]["hosts"]
    system = engine.fix_system_name(system)

    client = EsMessageStorage(
        hosts=hosts,
        engine=engine,
    )

    results = client.get_production_messages(
        system=system,
        production_stream=production_stream,
        production_type=production_type,
        production_name=production_name,
        forecast_time=f"{forecast_hour:03}h",
        start_time=start_time
    )

    processor = TableProcessor()
    table = processor.process_messages(results)

    table["time_str"] = table["time"].map(lambda x: x.isoformat())
    table["clock"] = (table["time"] - table["start_time"]) + pd.Timedelta(hours=start_hour)
    table["start_hour"] = table["start_time"].map(lambda x: x.hour)
    source_table = table[table["start_hour"] == start_hour]

    standard_time_messages = list(client.get_production_standard_time_message(
        system=system,
        production_stream="oper",
        production_type="grib2",
        production_name="orig",
    ))

    standard_time_message = standard_time_messages[0]
    standard_time_df = pd.DataFrame([
        {**time, "start_hour": i["start_hour"]}
        for i in standard_time_message.start_hours
        for time in i["times"]
    ])

    standard_forecast_time = pd.to_timedelta(
        standard_time_df[
            (standard_time_df["forecast_hour"] == forecast_hour) &
            (standard_time_df["start_hour"] == f"{start_hour:02}")
        ]["upper_duration"]
    ).item()

    current_source = ColumnDataSource(source_table)

    tools = "pan,wheel_zoom,box_zoom,reset"

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
        title=f"Production time for GRAPES GFS ({forecast_hour:03}h)",
        tools=tools,
    )

    upper_box = BoxAnnotation(
        top=standard_forecast_time + pd.Timedelta(hours=start_hour),
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
        hours=['%H:%M:%S']
    )

    p.line(
        y="clock",
        x="time",
        source=current_source,
        color="blue",
    )

    p.xaxis.axis_label = "Start Time"
    p.yaxis.axis_label = "Clock (Hour)"

    return p
