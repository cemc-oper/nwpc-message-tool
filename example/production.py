import datetime

import pandas as pd
import click
from loguru import logger

from nwpc_message_tool.storage import EsMessageStorage
from nwpc_message_tool.message import ProductionEventMessage
from nwpc_message_tool import nmc_monitor, nwpc_message


def get_hour(message: ProductionEventMessage) -> int:
    return int(message.forecast_time.seconds/3600) + message.forecast_time.days * 24


@click.command()
@click.option("--elastic-server", required=True, multiple=True, help="ElasticSearch servers")
@click.option("--system", required=True, help="system")
@click.option("--production-stream", default="oper", help="stream")
@click.option("--production-type", default="grib2", help="type")
@click.option("--production-name", default="orig", help="name")
@click.option("--start-time", required=True, help="name, YYYYMMDDHH[/YYYYMMDDHH]")
@click.option("--engine", default="nwpc_message", type=click.Choice(["nwpc_message", "nmc_monitor"]))
def cli(elastic_server, system, production_stream, production_type, production_name, start_time: str, engine):
    if "/" in start_time:
        token = start_time.split("/")
        start_time = tuple(datetime.datetime.strptime(t, "%Y%m%d%H") for t in token)
    else:
        start_time = datetime.datetime.strptime(start_time, "%Y%m%d%H")
    if engine == "nwpc_message":
        engine = nwpc_message
    elif engine == "nmc_monitor":
        engine = nmc_monitor
    else:
        raise NotImplemented(f"engine is not supported: {engine}")
    logger.info(f"using search engine: {engine.__name__}")

    system = engine.fix_system_name(system)
    logger.info(f"fix system name to: {system}")

    logger.info(f"searching...")
    client = EsMessageStorage(
        hosts=elastic_server,
        engine=engine,
    )
    results = client.get_production_messages(
        system=system,
        production_stream=production_stream,
        production_type=production_type,
        production_name=production_name,
        start_time=start_time
    )

    df = pd.DataFrame(columns=["forecast_hour", "time"])
    for result in results:
        hours = get_hour(result)
        message_time = result.time.ceil("S")
        current_df = pd.DataFrame(
            {
                "forecast_hour": [hours],
                "time": [message_time]
            },
            columns=["forecast_hour", "time"],
            index=[f"{result.start_time.strftime('%Y%m%d%H')}+{hours:03}"]
        )
        df = df.append(current_df)
    logger.info(f"searching...done")

    logger.info(f"get {len(df)} results")
    df = df.sort_index()
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(df)
    print(f"Latest time: {df.time.max()}")


if __name__ == "__main__":
    cli()
