import pandas as pd
import numpy as np
from flask import Blueprint, request, current_app, jsonify

from nwpc_message_tool import nwpc_message
from nwpc_message_tool.storage import EsMessageStorage
from nwpc_message_tool.processor import TableProcessor
from nwpc_message_tool.server.systems_config import systems_config

import json

api_app = Blueprint('api_app', __name__, template_folder='template')


@api_app.route('/prod/grib2', methods=['GET'])
def get_prod_grib2():
    system_name = request.args.get("system", "")
    query_date = request.args.get("date", "")

    start_time = (
        pd.to_datetime(f"{query_date} 00:00:00"),
        pd.to_datetime(f"{query_date} 23:00:00"),
    )

    engine = nwpc_message
    system = engine.fix_system_name(system_name)

    hosts = current_app.config["SERVER_CONFIG"]["message_storage"]["hosts"]

    client = EsMessageStorage(
        hosts=hosts,
        engine=engine,
    )

    # get standard times
    standard_time_messages = list(client.get_production_standard_time_message(
        system=system,
        production_stream="oper",
        production_type="grib2",
        production_name="orig",
    ))

    if len(standard_time_messages) == 0:
        standard_time_df = None
    else:
        standard_time_message = standard_time_messages[0]
        standard_time_df = pd.DataFrame([
            {**time, "start_hour": i["start_hour"]}
            for i in standard_time_message.start_hours
            for time in i["times"]
        ])

    results = client.get_production_messages(
        system=system,
        production_stream="oper",
        production_type="grib2",
        production_name="orig",
        start_time=start_time
    )

    processor = TableProcessor()
    df = processor.process_messages(results)

    result = []
    system_config = systems_config[system]
    for item in system_config["start_hours"]:
        start_hour = item["start_hour"]
        df_start_hour = df[df["start_time"] == f"{query_date} {start_hour}:00:00"]
        df_selected = df_start_hour[["forecast_hour","time"]]

        df_full_hours = pd.DataFrame({
            "forecast_hour": item["forecast_hours"],
        })

        df_merged = pd.merge(df_full_hours, df_selected, how="left")

        if standard_time_df is not None:
            current_start_clock = pd.to_datetime(f"{query_date} {start_hour}:00 UTC")

            start_hour_df = standard_time_df[standard_time_df["start_hour"] == start_hour][["forecast_hour", "duration"]]
            start_hour_df_merged = pd.merge(df_merged, start_hour_df, how="left")
            start_hour_df_merged["duration"] = start_hour_df_merged["duration"].apply(lambda x: pd.to_timedelta(x))
            start_hour_df_merged["standard_time"] = start_hour_df_merged["duration"] + current_start_clock

            start_hour_df_merged["flag"] = np.where(
                start_hour_df_merged["time"] > start_hour_df_merged["standard_time"],
                "late",
                "normal",
            )

            df_merged = start_hour_df_merged[["forecast_hour", "time", "standard_time", "flag"]]

        start_hour_json = df_merged.to_json(
            orient="records",
            date_format="iso",
        )
        current_production_times = json.loads(start_hour_json)

        result.append({
            "start_hour": start_hour,
            "times": current_production_times,
        })

    response = jsonify(result)
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response
