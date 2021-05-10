import click
from .plot import plot_cli
from .table import table_cli


@click.group()
def production():
    pass


production.add_command(plot_cli, name="plot")
production.add_command(table_cli, name="table")
