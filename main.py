import click

from src.core.command.etl import etl_command


@click.command()
@click.option(
    "-exec",
    "--execute",
    default="ALL",
    type=click.Choice(["ALL", "ELIGIBILITY", "CLAIM_RPT", "PROVIDER_CLAIM"]),
    help="ETL execute command",
)
@click.option("-f", "--file", help="Create ETL script file")
def main(execute):
    """
    This is the main description of your script.
    It appears at the top of the help message.

    You can add multiple lines here to explain what your script does.
    """
    print(f"Processing execute: {execute}")
    etl_command.execute(execute)


if __name__ == "__main__":
    main()
