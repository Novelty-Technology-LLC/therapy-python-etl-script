from pathlib import Path
from typing import List
from src.config.config import Config
from src.core.migrate.base_etl import BaseEtl
from src.core.service.invoice_billing_details.model import invoiceBillingDetailsModel
from src.core.service.invoice_billings.model import invoiceBillingsModel
from src.core.service.receipt_details.model import receiptDetailsModel
from src.core.service.therapy_notes.entity import TherapyNoteProjectModule
from src.core.service.therapy_notes.model import therapy_notes_model
from src.shared.constant.constant import BATCH_SIZE
from src.shared.interface.project_module import ProjectModule
from src.shared.utils.obj import get_obj_value


class RemoveMismatchAssignedNumberInvoiceBilling(BaseEtl):
    def __init__(self):
        super().__init__()
        self.batch_size = BATCH_SIZE
        self.enable_backup = False
        self.support_duplicate_documents = Config.get_documents().get(
            "support_duplicate_documents"
        )
        self.mismatched_invoice_billing_ids: List[str] = []
        self.modules: List[ProjectModule] = [
            ProjectModule.INVOICE_BILLING,
            ProjectModule.INVOICE_BILLING_DETAIL,
            ProjectModule.RECEIPT_DETAIL,
            ProjectModule.THERAPY_NOTE,
        ]

    def find_mismatched_invoice_billing_ids(self):
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "ibdID": "$invoiceBillingNumber",
                        "procedureCode": "$procedureCode",
                        "DOS": "$serviceDate.formattedStartDate",
                        "assignedNumber": "$assignedNumber",
                    },
                    "fieldN": {"$push": "$$ROOT"},
                }
            },
            {"$match": {"$expr": {"$gt": [{"$size": "$fieldN"}, 1]}}},
            {"$unwind": "$fieldN"},
            {"$unwind": "$fieldN"},
            {"$replaceRoot": {"newRoot": "$fieldN"}},
            {
                "$project": {
                    "assignedNumber": 1,
                    "procedureCode": 1,
                    "invoiceBillingNumber": 1,
                    "formattedStartDate": "$serviceDate.formattedStartDate",
                }
            },
            {
                "$group": {
                    "_id": "$invoiceBillingNumber",
                    "serviceLines": {"$push": "$$ROOT"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "invoiceBillingNumber": "$_id",
                }
            },
        ]

        invoice_billing_numbers_from_db = list(
            invoiceBillingDetailsModel.get_model().aggregate(pipeline)
        )

        for invoice_billing_number in invoice_billing_numbers_from_db:

            invoice_billing_number = get_obj_value(
                invoice_billing_number, "invoiceBillingNumber"
            )

            if invoice_billing_number:
                self.mismatched_invoice_billing_ids.append(invoice_billing_number)

    def execute(self):
        print(f"🔄 [START] Remove Mismatch Assigned Number Invoice Billing")

        # self.find_mismatched_invoice_billing_ids()

        if len(self.mismatched_invoice_billing_ids):
            print(
                f"📦📦📦 Total mismatched invoice billing ids: {len(self.mismatched_invoice_billing_ids)}"
            )
        else:
            print(f"📦📦📦 No mismatched invoice billing ids found")
            return

        for module in self.modules:
            print(f"🔄 [START] Processing module: {module}")

            match module:
                case ProjectModule.INVOICE_BILLING:
                    invoiceBillingsModel.get_model().delete_many(
                        {
                            "invoiceBillingNumber": {
                                "$in": self.mismatched_invoice_billing_ids
                            }
                        }
                    )
                case ProjectModule.INVOICE_BILLING_DETAIL:
                    invoiceBillingDetailsModel.get_model().delete_many(
                        {
                            "invoiceBillingNumber": {
                                "$in": self.mismatched_invoice_billing_ids
                            }
                        }
                    )
                case ProjectModule.RECEIPT_DETAIL:
                    receiptDetailsModel.get_model().delete_many(
                        {
                            "invoiceBillingNumber": {
                                "$in": self.mismatched_invoice_billing_ids
                            }
                        }
                    )

                case ProjectModule.THERAPY_NOTE:
                    therapy_notes_model.get_model().delete_many(
                        {
                            "references.invoiceBillingRef.identificationCode": {
                                "$in": self.mismatched_invoice_billing_ids
                            },
                            "references.module": {
                                "$in": [
                                    ProjectModule.INVOICE_BILLING,
                                    ProjectModule.INVOICE_BILLING_DETAIL,
                                    ProjectModule.RECEIPT_DETAIL,
                                ]
                            },
                        }
                    )

                case _:
                    print(f"Invalid module: {module}")
