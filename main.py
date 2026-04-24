import click

from src.core.command.etl import etl_command


@click.command()
@click.option(
    "-exec",
    "--execute",
    default="ELIGIBILITY",
    type=click.Choice(
        [
            "ALL",  # ❌
            "CLAIM_RPT",  # ❌
            "PROVIDER_CLAIM",  # ❌
            "ELIGIBILITY",  # ✅ -> This is used for migrating eligibility, patient, subscriber, enrollee data to python test collection
            "PATIENT_FIX_SUBSCRIBER_NAME",  # ⚠️ -> This is used for fixing subscriber name in patient only -> collection PYTHON TEST
            "ELIGIBILITY_FIX_PRODUCT_AND_PATIENT_DOB_PATCH",  # ⚠️ -> This is used for fixing product name and patient formatted date on eligibility only -> collection PYTHON TEST
            "PROVIDER_CLAIM_RPT_CHANGE_TO_EXCEL",  # ❌
            "INVOICE_BILLING_MAP_ENROLLEE_SUBSCRIBER_PATIENT",  # ✅ -> This is used for mapping enrollee, subscriber, patient data to invoice billing, invoice billing detail and receipt detail
            "ELIGIBILITY_COPY_DATA_TO_THERAPY_COLLECTION",  # ✅ -> This is used for copying eligibility, patient, subscriber, enrollee data to therapy collection from PYTHON TEST
            "UPDATE_FORMATTED_DATES",  # ⚠️ -> This is used for updating formatted dates on eligibility, patient, subscriber, enrollee data -> collection PYTHON TEST,
            "ELIGIBILITY_MAP_MISSING_ENROLLEE_PATIENT_SUBSCRIBER_AND_ELIGIBILITY_DATA_PATCH",  # ⚠️ -> This is used to create missing enrollee, patient, subscriber and eligibility data PYTHON TEST, meaning from ELIGIBILITY sheet
            "DOCUMENT_MOVE_PYTHON_DOCUMENT_TO_THERAPY",  # ✅ -> This is used for moving python document to therapy collection
            "THERAPY_NOTE_MIGRATE",  # ✅ -> This is used for migrating therapy note data to therapy collection
            "TEST",  # ❌
            "MOVE_PYTHON_TEST_NOTE_TO_THERAPY_NOTE",  # ✅ -> This is used for moving python test note to therapy note collection
            "PRESERVE_ARDB_CREATED_AND_UPDATED_DATE",  # ✅ -> This is used for preserving ardb created and updated date on therapy note
            "MAP_INVOICE_BILLING_DETAIL_STATUS",  # ✅ -> This is used for mapping invoice billing detail status on therapy note (Not completed)
            "MAP_INVOICE_BILLING_STATUS",  # ✅ -> This is used for mapping invoice billing status on therapy note (Not Completed)
            "ARDB_DUMP_MIGRATE",  # ✅ -> This is used for migrating ardb dump data to therapy collection
            "MAP_ENROLLEE_PATIENT_SUBSCRIBER_FROM_FILE",  # ✅ -> This is used for mapping enrollee, patient, subscriber data from file to therapy collection
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
