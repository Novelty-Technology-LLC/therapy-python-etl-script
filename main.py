import click

from src.core.command.etl import etl_command


@click.command()
@click.option(
    "-exec",
    "--execute",
    default="ALL",
    type=click.Choice(["ALL", "CLAIM_RPT", "PROVIDER_CLAIM", "ELIGIBILITY"]),
    help="ETL execute command",
)
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
