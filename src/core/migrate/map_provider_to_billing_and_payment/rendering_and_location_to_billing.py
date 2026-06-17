import time
from typing import Dict, Set
import numpy as np
import pandas as pd
from pymongo.operations import UpdateMany, UpdateOne
from src.core.service.invoice_billing_details.model import invoiceBillingDetailsModel
from src.core.service.invoice_billings.model import invoiceBillingsModel
from src.core.service.invoice_payment_receipts.model import invoicePaymentReceiptsModel
from src.core.service.providers.model import therapyProvidersModel
from src.core.service.receipt_details.model import receiptDetailsModel
from src.shared.interface.etl.migration import FileMetadata
from src.shared.utils.date import format_duration
from src.shared.utils.name import get_name
from src.shared.utils.obj import get_obj_value


class RenderingAndLocationToBilling:
    def execute(self, chunk: pd.DataFrame, file_metadata: FileMetadata):
        print("===========📊 [START] Load data ===========")
        data_load_time = time.perf_counter()

        print(f"📦📦📦 Total rows: {len(chunk)}")
        if len(chunk) == 0:
            print(f"===========📊 [END] No rows to process ===========")
            return

        chunk.replace({np.nan: None}, inplace=True)

        collect_rendering_and_location_ids: Set[str] = set()

        for row in chunk.to_dict(orient="records"):
            rendering_id = row.get("PROVIDER_ID")
            location_id = row.get("LOCATION_ID")

            if rendering_id:
                collect_rendering_and_location_ids.add(rendering_id)

            if location_id:
                collect_rendering_and_location_ids.add(location_id)

        # --- Batch DB fetches ---
        providers_from_db = (
            list(
                therapyProvidersModel.get_model().find(
                    filter={
                        "referenceId": {"$in": list(collect_rendering_and_location_ids)}
                    },
                )
            )
            if collect_rendering_and_location_ids
            else []
        )

        # --- Build lookup dicts for O(1) access ---
        providers_by_reference_id: Dict[str, dict] = {
            p["referenceId"]: p for p in providers_from_db if p.get("referenceId")
        }

        update_invoice_billing_by_invoice_billing_number: Dict[str, dict] = {}

        # --- Process rows ---
        for row in chunk.to_dict(orient="records"):
            rendering_id = row.get("PROVIDER_ID")
            location_id = row.get("LOCATION_ID")

            invoice_billing_number = row.get("INVOICE_BILLING_ID")

            if not rendering_id or not location_id or not invoice_billing_number:
                continue

            rendering_provider = providers_by_reference_id.get(rendering_id)
            location_provider = providers_by_reference_id.get(location_id)

            if not rendering_provider or not location_provider:
                continue

            update_invoice_billing_by_invoice_billing_number[invoice_billing_number] = {
                "renderingProvider": {
                    "refId": get_obj_value(rendering_provider, "_id"),
                    "referenceId": get_obj_value(rendering_provider, "referenceId"),
                    "identificationCode": get_obj_value(rendering_provider, "npi"),
                    "phone": (
                        get_obj_value(rendering_provider, "contact", "cellPhone")
                        or get_obj_value(rendering_provider, "contact", "textPhone")
                        or get_obj_value(rendering_provider, "contact", "phone")
                        or ""
                    ),
                    "fax": get_obj_value(rendering_provider, "contact", "fax"),
                    "email": get_obj_value(rendering_provider, "contact", "email"),
                    "name": get_name(
                        {
                            "firstName": get_obj_value(
                                rendering_provider, "demographic", "firstName"
                            ),
                            "middleName": get_obj_value(
                                rendering_provider, "demographic", "middleName"
                            ),
                            "lastName": get_obj_value(
                                rendering_provider, "demographic", "lastName"
                            ),
                        }
                    ),
                    "firstName": get_obj_value(
                        rendering_provider, "demographic", "firstName"
                    ),
                    **(
                        {
                            "middleName": get_obj_value(
                                rendering_provider, "demographic", "middleName"
                            ),
                        }
                    ),
                    "lastName": get_obj_value(
                        rendering_provider, "demographic", "lastName"
                    ),
                },
                "serviceFacility": {
                    "refId": get_obj_value(location_provider, "_id"),
                    "referenceId": get_obj_value(location_provider, "referenceId"),
                    "identificationCode": get_obj_value(location_provider, "npi"),
                    "phone": (
                        get_obj_value(location_provider, "contact", "cellPhone")
                        or get_obj_value(location_provider, "contact", "textPhone")
                        or get_obj_value(location_provider, "contact", "phone")
                        or ""
                    ),
                    "fax": get_obj_value(location_provider, "contact", "fax"),
                    "email": get_obj_value(location_provider, "contact", "email"),
                    "name": get_obj_value(location_provider, "demographic", "name"),
                    "address": get_obj_value(
                        location_provider, "demographic", "address"
                    ),
                },
                "invoiceBillingNumber": invoice_billing_number,
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

            update_rendering_providers_in_invoice_billing_details = []
            update_rendering_providers_in_invoice_payment_receipts = []

            for ib in update_invoice_billings:
                if not ib.get("renderingProvider"):
                    continue

                update_rendering_providers_in_invoice_billing_details.append(
                    UpdateMany(
                        {"invoiceBillingNumber": ib["invoiceBillingNumber"]},
                        {"$set": {"renderingProvider": ib["renderingProvider"]}},
                    )
                )

                update_rendering_providers_in_invoice_payment_receipts.append(
                    UpdateMany(
                        {"invoiceNumber": ib["invoiceBillingNumber"]},
                        {"$set": {"renderingProvider": ib["renderingProvider"]}},
                    )
                )

            if update_rendering_providers_in_invoice_billing_details:
                invoiceBillingDetailsModel.get_model().bulk_write(
                    update_rendering_providers_in_invoice_billing_details
                )

                receiptDetailsModel.get_model().bulk_write(
                    update_rendering_providers_in_invoice_billing_details
                )

            if update_rendering_providers_in_invoice_payment_receipts:
                invoicePaymentReceiptsModel.get_model().bulk_write(
                    update_rendering_providers_in_invoice_payment_receipts
                )

        print(
            f"===========📊 [END] Load data in {format_duration(time.perf_counter() - data_load_time)} ==========="
        )


rendering_and_location_to_billing = RenderingAndLocationToBilling()
