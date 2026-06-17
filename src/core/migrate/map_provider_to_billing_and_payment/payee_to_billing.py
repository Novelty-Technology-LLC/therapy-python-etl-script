import time
from typing import Dict, Set
import numpy as np
import pandas as pd
from pymongo.operations import UpdateMany, UpdateOne
from src.core.service.invoice_billings.model import invoiceBillingsModel
from src.core.service.invoice_payment_receipts.model import invoicePaymentReceiptsModel
from src.core.service.providers.model import therapyProvidersModel
from src.core.service.receipt_details.model import receiptDetailsModel
from src.shared.interface.etl.migration import FileMetadata
from src.shared.utils.date import format_duration
from src.shared.utils.obj import get_obj_value


class PayeeToBilling:
    def execute(self, chunk: pd.DataFrame, file_metadata: FileMetadata):
        print("===========📊 [START] Load data ===========")
        data_load_time = time.perf_counter()

        print(f"📦📦📦 Total rows: {len(chunk)}")
        if len(chunk) == 0:
            print(f"===========📊 [END] No rows to process ===========")
            return

        chunk.replace({np.nan: None}, inplace=True)

        collect_payee_ids: Set[str] = set()
        collect_claim_ids: Set[str] = set()

        for row in chunk.to_dict(orient="records"):
            payee_id = row.get("PAYEE_ID")
            claim_id = row.get("CLAIM_ID")

            if payee_id:
                collect_payee_ids.add(payee_id)

            if claim_id:
                collect_claim_ids.add(claim_id)

        # --- Batch DB fetches ---
        providers_from_db = (
            list(
                therapyProvidersModel.get_model().find(
                    filter={"referenceId": {"$in": list(collect_payee_ids)}}
                )
            )
            if collect_payee_ids
            else []
        )

        invoice_billings_from_db = (
            list(
                invoiceBillingsModel.get_model().find(
                    filter={"claim.referenceId": {"$in": list(collect_claim_ids)}}
                )
            )
            if collect_claim_ids
            else []
        )

        # --- Build lookup dicts for O(1) access ---
        providers_by_reference_id: Dict[str, dict] = {
            p["referenceId"]: p for p in providers_from_db if p.get("referenceId")
        }
        invoice_billings_by_claim_reference_id: Dict[str, dict] = {
            ib["claim"]["referenceId"]: ib
            for ib in invoice_billings_from_db
            if ib.get("claim") and ib.get("claim").get("referenceId")
        }

        update_invoice_billing_by_invoice_billing_number: Dict[str, dict] = {}

        for row in chunk.to_dict(orient="records"):
            payee_id = row.get("PAYEE_ID")
            claim_id = row.get("CLAIM_ID")

            if not payee_id or not claim_id:
                continue

            provider = providers_by_reference_id.get(payee_id)
            if not provider:
                continue

            invoice_billing = invoice_billings_by_claim_reference_id.get(claim_id)
            if not invoice_billing:
                continue

            update_invoice_billing_by_invoice_billing_number[
                invoice_billing["invoiceBillingNumber"]
            ] = {
                "billingProvider": {
                    "refId": get_obj_value(provider, "_id"),
                    "referenceId": get_obj_value(provider, "referenceId"),
                    "identificationCode": get_obj_value(provider, "npi"),
                    "phone": (
                        get_obj_value(provider, "contact", "cellPhone")
                        or get_obj_value(provider, "contact", "textPhone")
                        or get_obj_value(provider, "contact", "phone")
                        or ""
                    ),
                    "fax": get_obj_value(provider, "contact", "fax"),
                    "email": get_obj_value(provider, "contact", "email"),
                    "name": get_obj_value(provider, "demographic", "name"),
                    "address": get_obj_value(provider, "demographic", "address"),
                    **(
                        {
                            "taxId": get_obj_value(
                                provider, "metadata", "payeeDetail", "taxId"
                            )
                        }
                        if get_obj_value(provider, "metadata", "payeeDetail", "taxId")
                        else {}
                    ),
                    **(
                        {
                            "primaryTaxonomy": {
                                "code": get_obj_value(
                                    provider, "metadata", "taxonomyCode"
                                ),
                                "description": "",
                            }
                        }
                        if get_obj_value(provider, "metadata", "taxonomyCode")
                        else {}
                    ),
                },
                "invoiceBillingNumber": invoice_billing["invoiceBillingNumber"],
            }

        # --- DB writes ---
        update_invoice_billings = list(
            update_invoice_billing_by_invoice_billing_number.values()
        )
        print(f"📦📦📦 Update invoice billings: {len(update_invoice_billings)}")
        if update_invoice_billings:
            invoiceBillingsModel.get_model().bulk_write(
                [
                    UpdateOne(
                        {"invoiceBillingNumber": ib["invoiceBillingNumber"]},
                        {
                            "$set": {
                                k: v
                                for k, v in ib.items()
                                if k != "invoiceBillingNumber"
                            }
                        },
                    )
                    for ib in update_invoice_billings
                ]
            )

            invoicePaymentReceiptsModel.get_model().bulk_write(
                [
                    UpdateMany(
                        {"invoiceNumber": ib["invoiceBillingNumber"]},
                        {
                            "$set": {
                                k: v
                                for k, v in ib.items()
                                if k != "invoiceBillingNumber"
                            }
                        },
                    )
                    for ib in update_invoice_billings
                ]
            )

        print(
            f"===========📊 [END] Load data in {format_duration(time.perf_counter() - data_load_time)} ==========="
        )


payee_to_billing = PayeeToBilling()
