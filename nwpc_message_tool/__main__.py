import click
from nwpc_message_tool.cli.table import table_cli


@click.group()
def cli():
    pass


if __name__ == "__main__":
    cli.add_command(table_cli, name="table")
    cli()
