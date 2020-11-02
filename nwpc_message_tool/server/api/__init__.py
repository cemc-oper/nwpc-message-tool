import pandas as pd
import numpy as np
from flask import Blueprint, request, current_app, jsonify
from loguru import logger

from nwpc_message_tool.source.production import nwpc_message
from nwpc_message_tool.storage import EsMessageStorage
from nwpc_message_tool.processor import TableProcessor
from nwpc_message_tool.server.systems_config import SystemsConfig

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

    system = nwpc_message.fix_system_name(system_name)

    hosts = current_app.config["SERVER_CONFIG"]["message_storage"]["hosts"]

    client = EsMessageStorage(
        hosts=hosts,
        show_progress=False,
    )

    # get standard times
    standard_time_messages = list(client.get_production_standard_time_message(
        system=system,
        production_stream="oper",
        production_type="grib2",
        production_name="orig",
        engine=nwpc_message.production_standard_time,
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
        start_time=start_time,
        size=40,
        engine=nwpc_message.production,
    )

    processor = TableProcessor(
        keep_duplicates=False,
    )
    df = processor.process_messages(results)
    logger.debug(f"[{system}] API table has {len(df)} records")

    result = []
    system_config = SystemsConfig[system]
    for item in system_config["start_hours"]:
        start_hour = item["start_hour"]
        df_start_hour = df[df["start_time"] == pd.to_datetime(f"{query_date} {start_hour}:00:00 UTC")]
        df_selected = df_start_hour[["forecast_hour","time"]]
        logger.debug(f"[{system}][{start_hour}] select {len(df_selected)} records")

        df_full_hours = pd.DataFrame({
            "forecast_hour": item["forecast_hours"],
        })

        df_merged = pd.merge(df_full_hours, df_selected, how="left")

        # # check na
        # df_merged_na = df_merged["forecast_hour"][df_merged["time"].isna()]
        # if len(df_merged_na) > 0:
        #     # print(f"{system}/{start_hour}:", df_merged_na)
        #     pass

        if standard_time_df is not None:
            current_start_clock = pd.to_datetime(f"{query_date} {start_hour}:00 UTC")

            start_hour_df = standard_time_df[standard_time_df["start_hour"] == start_hour][["forecast_hour", "upper_duration"]]
            start_hour_df_merged = pd.merge(df_merged, start_hour_df, how="left")
            start_hour_df_merged["upper_duration"] = start_hour_df_merged["upper_duration"].apply(lambda x: pd.to_timedelta(x))
            start_hour_df_merged["standard_time"] = start_hour_df_merged["upper_duration"] + current_start_clock

            # ceil to minute when compare time with standard time.
            start_hour_df_merged["flag"] = np.where(
                start_hour_df_merged["time"].apply(lambda x: x.ceil("min")) > start_hour_df_merged["standard_time"].apply(lambda x: x.ceil("min")),
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
