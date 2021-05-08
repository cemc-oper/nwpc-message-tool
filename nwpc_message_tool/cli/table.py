import click
from loguru import logger

from nwpc_message_tool._util import get_engine
from nwpc_message_tool.cli._util import parse_start_time
from nwpc_message_tool.storage import EsMessageStorage, get_es_message_storage
from nwpc_message_tool.presenter import (
    PrintPresenter,
    TableStorePresenter,
)
from nwpc_message_tool.processor import TableProcessor


@click.command("table")
@click.option("--elastic-server", multiple=True, help="set ElasticSearch servers.")
@click.option("--storage-name", help="Use storage which is set in config file. Default use ``default_storage`` key.")
@click.option("--system", required=True, help="system, such as grapes_gfs_gmf, grapes_meso_3km and so on.")
@click.option("--production-stream", default="oper", help="production stream, such as oper.")
@click.option("--production-type", default="grib2", help="production type, such as grib.")
@click.option("--production-name", default="orig", help="production name, such as orig.")
@click.option(
    "--start-time",
    required=True,
    metavar="YYYYMMDDHH[/YYYYMMDDHH]",
    help="start time, one date or a data range.")
@click.option(
    "--engine",
    default="nwpc_message",
    type=click.Choice(["nwpc_message", "nmc_monitor"]),
    help="data source"
)
@click.option(
    "--output-type",
    default="print",
    type=click.Choice(["print", "json", "csv"]),
    help="output type"
)
@click.option(
    "--output-file",
    help="output file path",
)
@click.option("--config-file", default=None, help="config file path, default is ``${HOME}/.config/nwpc-oper/nwpc-message-tool.yaml``.")
def table_cli(
        elastic_server,
        storage_name,
        system,
        production_stream,
        production_type,
        production_name,
        start_time: str,
        engine,
        output_type,
        output_file,
        config_file,
):
    """
    Show production messages for GRAPES operation systems as a table.

    Support print into console or write into text files with --output-type and --output-file options.
    """
    start_time = parse_start_time(start_time)

    engine = get_engine(engine)
    logger.info(f"using search engine: {engine.__name__}")

    system = engine.fix_system_name(system)
    logger.info(f"fix system name to: {system}")

    logger.info(f"searching...")
    if len(elastic_server) > 0:
        client = EsMessageStorage(
            hosts=elastic_server,
            show_progress=True
        )
    else:
        client = get_es_message_storage(
            storage_name,
            show_progress=True,
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

    if output_type == "print":
        presenter = PrintPresenter()
        presenter.show(table)
    elif output_type in ["json", "csv"]:
        presenter = TableStorePresenter(output_type=output_type, output_file=output_file)
        presenter.show(table)
    else:
        raise ValueError(f"output type is not supported: {output_type}")


if __name__ == "__main__":
    table_cli()
