import click
from nwpc_message_tool.cli.table import table_cli
from nwpc_message_tool.cli.plot import plot_cli


@click.group()
def cli():
    pass


def main():
    cli.add_command(table_cli, name="table")
    cli.add_command(plot_cli, name="plot")
    cli()


if __name__ == "__main__":
    main()
