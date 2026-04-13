import click

from src.core.command.etl import etl_command


@click.command()
@click.option(
    "-exec",
    "--execute",
    default="INVOICE_BILLING_DETAIL_NOTE",
    type=click.Choice(
        [
            "ALL",
            "CLAIM_RPT",
            "PROVIDER_CLAIM",
            "ELIGIBILITY",
            "PATIENT_FIX_SUBSCRIBER_NAME",
            "ELIGIBILITY_FIX_PRODUCT_AND_PATIENT_DOB_PATCH",
            "RECEIPT_DETAIL_NOTE",
            "INVOICE_BILLING_NOTE",
            "INVOICE_BILLING_DETAIL_NOTE",
            "PROVIDER_CLAIM_RPT_CHANGE_TO_EXCEL",
            "INVOICE_BILLING_MAP_ENROLLEE_SUBSCRIBER_PATIENT",
            "ELIGIBILITY_COPY_DATA_TO_THERAPY_COLLECTION",
            "UPDATE_FORMATTED_DATES",
            "TEST",
        ]
    ),
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
