from pathlib import Path

from src.core.migrate.claim_rpt.claim_excel import Claim_Excel_Etl
from src.core.migrate.claim_rpt.etl import Claim_Rpt_Etl
from src.core.migrate.excel.eligibility.eligibility import Eligibility_Etl_Migrate
from src.core.migrate.provider_claim import Provider_Claim_Etl
from src.core.migrate.script.eligibility_fix_product_and_patient_dob_patch import (
    EligibilityFixProductAndPatientDobPatch,
)
from src.core.migrate.script.patient_fix_subscriber_name import PatientFixSubscriberName


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

            case _:
                raise ValueError(f"Invalid execute command: {execute}")


etl_command = ETLCommand()
