from src.core.migrate.eligibility import Eligibility_Etl
from src.core.migrate.claim_rpt import Claim_Rpt_Etl
from src.core.migrate.provider_claim import Provider_Claim_Etl


class ETLCommand:
    def execute(self, execute: str):
        match execute:
            case "ALL":
                with Eligibility_Etl() as etl:
                    etl.execute()
                with Claim_Rpt_Etl() as etl:
                    etl.execute()
                with Provider_Claim_Etl() as etl:
                    etl.execute()
            case "ELIGIBILITY":
                with Eligibility_Etl() as etl:
                    etl.execute()
            case "CLAIM_RPT":
                with Claim_Rpt_Etl() as etl:
                    etl.execute()
            case "PROVIDER_CLAIM":
                with Provider_Claim_Etl() as etl:
                    etl.execute()
            case _:
                raise ValueError(f"Invalid execute command: {execute}")


etl_command = ETLCommand()
