from pathlib import Path

from src.core.migrate.claim_rpt.claim_excel import Claim_Excel_Etl
from src.core.migrate.claim_rpt.etl import Claim_Rpt_Etl
from src.core.migrate.excel.eligibility.eligibility import Eligibility_Etl_Migrate
from src.core.migrate.invoice_billing_note.invoice_billing_detail_note import (
    InvoiceBillingDetailNote_Etl,
)
from src.core.migrate.invoice_billing_note.invoice_billing_note import (
    InvoiceBillingNote_Etl,
)
from src.core.migrate.provider_claim import Provider_Claim_Etl
from src.core.migrate.receipt_detail_note.etl import ReceiptDetailNote_Etl
from src.core.migrate.script.eligibility_copy_data_to_therapy_collection import (
    EligibilityCopyDataToTherapyCollectionPatch,
)
from src.core.migrate.script.eligibility_fix_product_and_patient_dob_patch import (
    EligibilityFixProductAndPatientDobPatch,
)

from src.core.migrate.script.eligibility_map_missing_enrollee_patient_subscriber_and_eligibility_data_patch import (
    EligibilityMapMissingEnrolleePatientSubscriberAndEligibilityDataPatch,
)
from src.core.migrate.script.invoice_billing_map_enrollee_subscriber_patient import (
    InvoiceBillingMapEnrolleeSubscriberPatientPatch,
)
from src.core.migrate.script.patient_fix_subscriber_name import PatientFixSubscriberName
from src.core.migrate.script.provider_claim_rpt_change_to_excel import (
    ProviderClaimRptChangeToExcel,
)
from src.core.migrate.script.update_formatted_dates_patch import (
    UpdateFormattedDatesPatch,
)


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

            case "RECEIPT_DETAIL_NOTE":
                with ReceiptDetailNote_Etl(Path("input-files/receipt_detail")) as etl:
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

            case "INVOICE_BILLING_NOTE":
                with InvoiceBillingNote_Etl(Path("input-files/notes/")) as etl:
                    etl.execute()

            case "INVOICE_BILLING_DETAIL_NOTE":
                with InvoiceBillingDetailNote_Etl(Path("input-files/notes/")) as etl:
                    etl.execute()

            case "UPDATE_FORMATTED_DATES":
                with UpdateFormattedDatesPatch() as etl:
                    etl.execute()

            case "ELIGIBILITY_MAP_MISSING_ENROLLEE_PATIENT_SUBSCRIBER_AND_ELIGIBILITY_DATA_PATCH":
                with EligibilityMapMissingEnrolleePatientSubscriberAndEligibilityDataPatch(
                    Path("input-files/eligibility")
                ) as etl:
                    etl.execute()

            case _:
                raise ValueError(f"Invalid execute command: {execute}")


etl_command = ETLCommand()
