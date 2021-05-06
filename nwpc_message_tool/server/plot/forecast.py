import pandas as pd
from bokeh.plotting import Figure
from flask import current_app

from nwpc_message_tool.source.production import nwpc_message
from nwpc_message_tool.processor import TableProcessor
from nwpc_message_tool.presenter.plot import ForecastTimeLinePlotPresenter
from nwpc_message_tool.storage import EsMessageStorage
from nwpc_message_tool._type import StartTimeType


def get_forecast_time_line(
        start_time: StartTimeType,
        start_hour: int,
        forecast_hour: int,
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
        forecast_time=f"{forecast_hour:03}h",
        start_time=start_time,
        engine=engine.production,
    )
    processor = TableProcessor()
    table = processor.process_messages(results)

    standard_time_messages = list(client.get_production_standard_time_message(
        system=system,
        production_stream="oper",
        production_type="grib2",
        production_name="orig",
        engine=nwpc_message.production_standard_time,
    ))
    standard_time_message = standard_time_messages[0]
    standard_time_df = pd.DataFrame([
        {**time, "start_hour": i["start_hour"]}
        for i in standard_time_message.start_hours
        for time in i["times"]
    ])

    presenter = ForecastTimeLinePlotPresenter(
        system=system,
        start_hour=start_hour,
        forecast_hour=forecast_hour,
        output_type=None
    )
    p = presenter.generate_plot(table, standard_time_df)
    return p
