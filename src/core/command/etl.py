from pathlib import Path

from src.core.migrate.claim_rpt.claim_excel import Claim_Excel_Etl
from src.core.migrate.claim_rpt.etl import Claim_Rpt_Etl
from src.core.migrate.excel.eligibility.eligibility import Eligibility_Etl_Migrate
from src.core.migrate.provider_claim import Provider_Claim_Etl
from src.core.migrate.script.ardb_dump_migrate import ArdbDumpMigrate
from src.core.migrate.script.document_move_python_document_to_therapy import (
    DocumentMovePythonDocumentToTherapy,
)
from src.core.migrate.script.eligibility_copy_data_to_therapy_collection import (
    EligibilityCopyDataToTherapyCollectionPatch,
)
from src.core.migrate.script.eligibility_fix_product_and_patient_dob_patch import (
    EligibilityFixProductAndPatientDobPatch,
)

from src.core.migrate.script.eligibility_map_missing_enrollee_patient_subscriber_and_eligibility_data_patch import (
    EligibilityMapMissingEnrolleePatientSubscriberAndEligibilityDataPatch,
)
from src.core.migrate.script.eligibility_update_formatted_dates_patch import (
    EligibilityUpdateFormattedDatesPatch,
)
from src.core.migrate.script.invoice_billing_map_enrollee_subscriber_patient import (
    InvoiceBillingMapEnrolleeSubscriberPatientPatch,
)
from src.core.migrate.script.patient_fix_subscriber_name import PatientFixSubscriberName
from src.core.migrate.script.preserve_ardb_created_updated_dates import (
    PreserveArdbCreatedUpdatedDates,
)
from src.core.migrate.script.provider_claim_rpt_change_to_excel import (
    ProviderClaimRptChangeToExcel,
)
from src.core.migrate.script.therapy_note_move_from_python_collection_patch import (
    TherapyNoteMoveFromPythonCollectionPatch,
)
from src.core.migrate.therapy_note.etl import TherapyNote_Etl


class ETLCommand:
    def execute(self, execute: str):
        match execute:
            case "ALL":
                with Eligibility_Etl_Migrate(Path("input-files/eligibility")) as etl:
                    etl.execute()
                with Claim_Rpt_Etl(Path("input-files/claim_rpt")) as etl:
                    etl.execute()
                with Provider_Claim_Etl(Path("input-files/provider_claims")) as etl:
                    etl.execute()

            case "CLAIM_RPT":
                with Claim_Rpt_Etl(Path("input-files/claim_rpt")) as etl:
                    etl.execute()
            case "PROVIDER_CLAIM":
                with Claim_Excel_Etl(Path("input-files/provider_claims")) as etl:
                    etl.execute()

            case "ELIGIBILITY":
                with Eligibility_Etl_Migrate(Path("input-files/eligibility")) as etl:
                    etl.execute()

            case "PATIENT_FIX_SUBSCRIBER_NAME":
                with PatientFixSubscriberName() as etl:
                    etl.execute()

            case "ELIGIBILITY_FIX_PRODUCT_AND_PATIENT_DOB_PATCH":
                with EligibilityFixProductAndPatientDobPatch() as etl:
                    etl.execute()

            case "PROVIDER_CLAIM_RPT_CHANGE_TO_EXCEL":
                with ProviderClaimRptChangeToExcel(
                    input_file_path=Path("input-files/claim_rpt/"),
                    output_file_path="/Users/rajan/Desktop/personal-practice/etl/therapy-python-etl/input-files/output/claims/",
                ) as etl:
                    etl.execute()

            case "INVOICE_BILLING_MAP_ENROLLEE_SUBSCRIBER_PATIENT":
                with InvoiceBillingMapEnrolleeSubscriberPatientPatch() as etl:
                    etl.execute()

            case "ELIGIBILITY_COPY_DATA_TO_THERAPY_COLLECTION":
                with EligibilityCopyDataToTherapyCollectionPatch() as etl:
                    etl.execute()

            case "THERAPY_NOTE_MIGRATE":
                with TherapyNote_Etl(Path("input-files/notes/")) as etl:
                    etl.execute()

            case "UPDATE_FORMATTED_DATES":
                with EligibilityUpdateFormattedDatesPatch() as etl:
                    etl.execute()

            case "ELIGIBILITY_MAP_MISSING_ENROLLEE_PATIENT_SUBSCRIBER_AND_ELIGIBILITY_DATA_PATCH":
                with EligibilityMapMissingEnrolleePatientSubscriberAndEligibilityDataPatch(
                    Path("input-files/eligibility")
                ) as etl:
                    etl.execute()

            case "DOCUMENT_MOVE_PYTHON_DOCUMENT_TO_THERAPY":
                with DocumentMovePythonDocumentToTherapy() as etl:
                    etl.execute()

            case "PRESERVE_ARDB_CREATED_AND_UPDATED_DATE":
                with PreserveArdbCreatedUpdatedDates(Path("input-files/notes/")) as etl:
                    etl.execute()

            case "MOVE_PYTHON_TEST_NOTE_TO_THERAPY_NOTE":
                with TherapyNoteMoveFromPythonCollectionPatch() as etl:
                    etl.execute()

            case "ARDB_DUMP_MIGRATE":
                with ArdbDumpMigrate(Path("input-files/ardb/")) as etl:
                    etl.execute()

            case _:
                raise ValueError(f"Invalid execute command: {execute}")


etl_command = ETLCommand()
