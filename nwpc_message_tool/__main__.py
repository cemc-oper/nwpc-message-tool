import click
from nwpc_message_tool.cli.production import production


@click.group()
def cli():
    pass


def main():
    cli.add_command(production, name="production")
    cli()


if __name__ == "__main__":
    main()
