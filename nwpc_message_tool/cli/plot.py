import click
from loguru import logger

from nwpc_message_tool._util import get_engine
from nwpc_message_tool.cli._util import parse_start_time
from nwpc_message_tool.storage import EsMessageStorage
from nwpc_message_tool.presenter import (
    StepGridPlotPresenter,
    CyclePeriodPlotPresenter,
)
from nwpc_message_tool.processor import TableProcessor


@click.command("plot")
@click.option("--plot-type", default="step_grid", type=click.Choice(["step_grid", "cycle_period"]), help="type of plot")
@click.option("--elastic-server", required=True, multiple=True, help="ElasticSearch servers")
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
    default="file",
    type=click.Choice(["file"]),
    help="output type"
)
@click.option(
    "--output-file",
    help="output file path",
)
def plot_cli(
        plot_type,
        elastic_server,
        system,
        production_stream,
        production_type,
        production_name,
        start_time: str,
        engine,
        output_type,
        output_file,
):
    """
    Plot forecast time points.
    """
    start_time = parse_start_time(start_time)

    engine = get_engine(engine)
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

    processor = TableProcessor()
    table = processor.process_messages(results)

    print(table)

    if plot_type == "step_grid":
        presenter = StepGridPlotPresenter(
            system=system,
            output_type=("file",),
            output_path=output_file,
        )
        presenter.show(table)
    elif plot_type == "cycle_period":
        presenter = CyclePeriodPlotPresenter(
            system=system,
            output_type=("file",),
            output_path=output_file,
        )
        presenter.show(table)
    else:
        raise ValueError(f"plot type is not supported: {plot_type}")


if __name__ == "__main__":
    plot_cli()
