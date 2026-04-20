from datetime import datetime
import time
from typing import Dict, Set
import numpy as np
import pandas as pd
from pymongo.operations import UpdateOne
from src.core.service.invoice_billings.model import invoiceBillingsModel
from src.core.service.receipt_details.model import receiptDetailsModel
from src.core.service.therapy_notes.entity import (
    ITherapyNote,
    TherapyNoteProjectModule,
)
from src.core.service.therapy_notes.model import therapy_notes_model
from src.shared.constant.constant import SYSTEM_USER
from src.shared.interface.etl.migration import FileMetadata
from src.shared.utils.date import format_duration, to_datetime, to_utc_datetime
from src.shared.utils.migration import generate_uuid
from src.shared.utils.obj import get_obj_value


class ReceiptDetailNote_Etl:
    def execute(self, chunk: pd.DataFrame, file_metadata: FileMetadata):
        print("===========📊 [START] Load data ===========")
        data_load_time = time.perf_counter()

        chunk = chunk[chunk["PAYMENT_NOTES"].notna()].reset_index(drop=True)
        chunk.replace({np.nan: None}, inplace=True)

        # --- Collect IDs for batch DB queries ---
        collect_invoice_billing_numbers: Set[str] = set()
        collect_receipt_detail_reference_ids: Set[str] = set()

        for row in chunk.to_dict(orient="records"):
            invoice_billing_number = row.get("INVOICE_BILLING_ID")
            if invoice_billing_number:
                collect_invoice_billing_numbers.add(invoice_billing_number)

            receipt_detail_id = row.get("RECEIPT_DETAIL_ID")
            if receipt_detail_id:
                collect_receipt_detail_reference_ids.add(receipt_detail_id)

        # --- Batch DB fetches ---
        invoice_billings_from_db = (
            list(
                invoiceBillingsModel.get_model().find(
                    filter={
                        "invoiceBillingNumber": {
                            "$in": list(collect_invoice_billing_numbers)
                        }
                    },
                    projection={
                        "_id": 1,
                        "invoiceBillingNumber": 1,
                        "enrollee": 1,
                        "patient": 1,
                    },
                )
            )
            if collect_invoice_billing_numbers
            else []
        )

        receipt_details_from_db = (
            list(
                receiptDetailsModel.get_model().find(
                    filter={
                        "referenceId": {
                            "$in": list(collect_receipt_detail_reference_ids)
                        }
                    },
                    projection={
                        "_id": 1,
                        "referenceId": 1,
                        "receipt": 1,
                        "receiptId": 1,
                        "invoiceBillingNumber": 1,
                        "assignedNumber": 1,
                        "serviceDate": 1,
                        "procedureCode": 1,
                        "checkNumber": 1,
                        "invoiceBillingDetailId": 1,
                        "invoicePaymentReceiptId": 1,
                        "created": 1,
                        "updated": 1,
                    },
                )
            )
            if collect_receipt_detail_reference_ids
            else []
        )

        collect_therapy_note_ids: Set[str] = set()
        for receipt_detail in receipt_details_from_db:
            _id = receipt_detail.get("_id")
            if _id:
                collect_therapy_note_ids.add(_id)

        therapy_notes_from_db = (
            list(
                therapy_notes_model.get_model().find(
                    filter={
                        "references.receiptDetailRef.refId": {
                            "$in": list(collect_therapy_note_ids)
                        }
                    },
                )
            )
            if collect_therapy_note_ids
            else []
        )

        # --- Build lookup dicts for O(1) access ---
        receipt_detail_by_ref: Dict[str, dict] = {
            rd["referenceId"]: rd
            for rd in receipt_details_from_db
            if rd.get("referenceId")
        }

        invoice_billing_by_num: Dict[str, dict] = {
            ib["invoiceBillingNumber"]: ib
            for ib in invoice_billings_from_db
            if ib.get("invoiceBillingNumber")
        }

        therapy_note_by_rd_id: Dict[str, dict] = {}
        for tn in therapy_notes_from_db:
            ref_id = get_obj_value(tn, "references", "receiptDetailRef", "refId")
            if ref_id:
                therapy_note_by_rd_id[ref_id] = tn

        inserted_notes_by_rd_id: Dict[str, ITherapyNote] = {}
        updated_notes_by_rd_id: Dict[str, ITherapyNote] = {}

        # --- Process rows ---
        for row in chunk.to_dict(orient="records"):
            receipt_detail_id = row.get("RECEIPT_DETAIL_ID")
            payment_note = row.get("PAYMENT_NOTES")
            date_entered = row.get("DATE_ENTERED")
            invoice_billing_number = row.get("INVOICE_BILLING_ID")
            to_date_entered = to_datetime(date_entered)

            if not payment_note or not receipt_detail_id:
                continue

            receipt_detail = receipt_detail_by_ref.get(receipt_detail_id)
            if not receipt_detail:
                continue

            invoice_billing = invoice_billing_by_num.get(
                receipt_detail.get("invoiceBillingNumber")
            )
            # if not invoice_billing:
            #     continue

            rd_id = receipt_detail.get("_id")
            therapy_note = therapy_note_by_rd_id.get(rd_id)

            if not therapy_note:
                existing = inserted_notes_by_rd_id.get(rd_id)
                if existing:
                    self._apply_note_to_therapy(
                        existing, payment_note, to_date_entered, file_metadata
                    )
                else:
                    new_note = self._build_new_therapy_note(
                        receipt_detail,
                        invoice_billing,
                        payment_note,
                        to_date_entered,
                        invoice_billing_number,
                        file_metadata,
                    )
                    inserted_notes_by_rd_id[rd_id] = new_note
            else:
                existing = updated_notes_by_rd_id.get(rd_id)
                if existing:
                    self._apply_note_to_therapy(
                        existing, payment_note, to_date_entered, file_metadata
                    )
                else:
                    updated_note = self._build_updated_therapy_note(
                        therapy_note, payment_note, to_date_entered, file_metadata
                    )
                    updated_notes_by_rd_id[rd_id] = updated_note

        # --- DB writes ---
        inserted_therapy_notes = list(inserted_notes_by_rd_id.values())
        updated_therapy_notes = list(updated_notes_by_rd_id.values())

        print(f"📦📦📦 Inserted therapy notes: {len(inserted_therapy_notes)}")
        print(f"📦📦📦 Updated therapy notes: {len(updated_therapy_notes)}")

        if inserted_therapy_notes:
            therapy_notes_model.insert_many(inserted_therapy_notes)

        if updated_therapy_notes:
            therapy_notes_model.get_model().bulk_write(
                [
                    UpdateOne(
                        {"_id": tn["_id"]},
                        {"$set": {k: v for k, v in tn.items() if k != "_id"}},
                    )
                    for tn in updated_therapy_notes
                ]
            )

        print(
            f"===========📊 [END] Load data in {format_duration(time.perf_counter() - data_load_time)} ==========="
        )

    def _apply_note_to_therapy(
        self,
        therapy_note: ITherapyNote,
        payment_note: str,
        to_date_entered: datetime,
        file_metadata: FileMetadata,
    ) -> None:
        """Mutates therapy_note in place: promotes or appends based on date comparison."""
        existing_date = to_utc_datetime(get_obj_value(therapy_note, "created", "at"))

        if to_date_entered >= existing_date:
            therapy_note["histories"].append(
                {
                    "note": therapy_note.get("note"),
                    "locked": therapy_note.get("locked"),
                    "isEdited": therapy_note.get("isEdited"),
                    "tags": therapy_note.get("tags"),
                    "updated": therapy_note.get("updated"),
                    "ardbCreated": therapy_note.get("ardbCreated"),
                    "ardbUpdated": therapy_note.get("ardbUpdated"),
                    "ardbSourceDocument": get_obj_value(
                        therapy_note, "references", "ardbSourceDocument"
                    ),
                }
            )
            therapy_note["note"] = payment_note
            therapy_note["created"] = {"by": SYSTEM_USER, "at": to_date_entered}
            therapy_note["updated"] = {"by": SYSTEM_USER, "at": to_date_entered}
            therapy_note["locked"] = {"by": SYSTEM_USER, "at": to_date_entered}
            therapy_note["ardbCreated"] = {"by": SYSTEM_USER, "at": to_date_entered}
            therapy_note["ardbUpdated"] = {"by": SYSTEM_USER, "at": to_date_entered}
        else:
            therapy_note["histories"].append(
                {
                    "note": payment_note,
                    "isEdited": False,
                    "locked": {"by": SYSTEM_USER, "at": to_date_entered},
                    "tags": [],
                    "updated": {"by": SYSTEM_USER, "at": to_date_entered},
                    "ardbCreated": {"by": SYSTEM_USER, "at": to_date_entered},
                    "ardbUpdated": {"by": SYSTEM_USER, "at": to_date_entered},
                    "ardbSourceDocument": get_obj_value(
                        file_metadata, "original_file_name"
                    ),
                }
            )

    def _build_new_therapy_note(
        self,
        receipt_detail: dict,
        invoice_billing: dict,
        payment_note: str,
        to_date_entered: datetime,
        invoice_billing_number: str,
        file_metadata: FileMetadata,
    ) -> ITherapyNote:
        return {
            "_id": generate_uuid(),
            "histories": [],
            "isEdited": False,
            "locked": {"by": SYSTEM_USER, "at": to_date_entered},
            "note": payment_note,
            "tags": [],
            "linkedDocuments": [],
            "references": {
                "module": TherapyNoteProjectModule.RECEIPT_DETAIL,
                "enrolleeRef": {
                    "refId": get_obj_value(invoice_billing, "enrollee", "refId"),
                    "identificationCode": get_obj_value(
                        invoice_billing, "enrollee", "referenceId"
                    ),
                },
                "invoiceBillingRef": {
                    "refId": get_obj_value(invoice_billing, "_id"),
                    "identificationCode": get_obj_value(
                        invoice_billing, "invoiceBillingNumber"
                    )
                    or invoice_billing_number,
                },
                "patientRef": {
                    "refId": get_obj_value(invoice_billing, "patient", "refId"),
                    "identificationCode": get_obj_value(
                        invoice_billing, "patient", "referenceId"
                    ),
                    "name": get_obj_value(invoice_billing, "patient", "name"),
                },
                "procedureCode": get_obj_value(receipt_detail, "procedureCode"),
                "serviceDate": get_obj_value(receipt_detail, "serviceDate"),
                "receiptDetailRef": {
                    "refId": get_obj_value(receipt_detail, "_id"),
                    "identificationCode": get_obj_value(
                        receipt_detail, "procedureCode"
                    ),
                    "referenceId": get_obj_value(receipt_detail, "referenceId"),
                },
                "invoiceBillingDetailRef": {
                    "refId": get_obj_value(receipt_detail, "invoiceBillingDetailId"),
                    "identificationCode": get_obj_value(
                        receipt_detail, "procedureCode"
                    ),
                },
                "receiptRef": {
                    "refId": get_obj_value(receipt_detail, "receiptId"),
                    "identificationCode": get_obj_value(
                        receipt_detail, "receipt", "referenceId"
                    ),
                    "name": get_obj_value(receipt_detail, "checkNumber"),
                },
                "invoicePaymentReceiptRef": {
                    "refId": get_obj_value(receipt_detail, "invoicePaymentReceiptId"),
                    "identificationCode": get_obj_value(
                        receipt_detail, "invoiceBillingNumber"
                    ),
                    "name": get_obj_value(receipt_detail, "invoiceBillingNumber"),
                },
                "ardbSourceDocument": get_obj_value(
                    file_metadata, "original_file_name"
                ),
            },
            "created": {"by": SYSTEM_USER, "at": to_date_entered},
            "updated": {"by": SYSTEM_USER, "at": to_date_entered},
            "ardbCreated": {"by": SYSTEM_USER, "at": to_date_entered},
            "ardbUpdated": {"by": SYSTEM_USER, "at": to_date_entered},
        }

    def _build_updated_therapy_note(
        self,
        therapy_note: ITherapyNote,
        payment_note: str,
        to_date_entered: datetime,
        file_metadata: FileMetadata,
    ) -> ITherapyNote:
        date_entered_from_db = to_utc_datetime(
            get_obj_value(therapy_note, "created", "at")
        )

        if to_date_entered >= date_entered_from_db:
            return {
                "_id": therapy_note.get("_id"),
                "isEdited": False,
                "created": {"by": SYSTEM_USER, "at": to_date_entered},
                "updated": {"by": SYSTEM_USER, "at": to_date_entered},
                "ardbCreated": {"by": SYSTEM_USER, "at": to_date_entered},
                "ardbUpdated": {"by": SYSTEM_USER, "at": to_date_entered},
                "locked": {"by": SYSTEM_USER, "at": to_date_entered},
                "linkedDocuments": [],
                "references": {
                    **(therapy_note.get("references") or {}),
                    "ardbSourceDocument": get_obj_value(
                        file_metadata, "original_file_name"
                    ),
                },
                "tags": therapy_note.get("tags") or [],
                "note": payment_note,
                "histories": [
                    *(therapy_note.get("histories") or []),
                    {
                        "note": therapy_note.get("note"),
                        "isEdited": therapy_note.get("isEdited"),
                        "locked": therapy_note.get("locked"),
                        "tags": therapy_note.get("tags"),
                        "updated": therapy_note.get("updated"),
                        "ardbCreated": therapy_note.get("ardbCreated"),
                        "ardbUpdated": therapy_note.get("ardbUpdated"),
                        "ardbSourceDocument": get_obj_value(
                            therapy_note, "references", "ardbSourceDocument"
                        ),
                    },
                ],
            }
        else:
            return {
                **(therapy_note or {}),
                "histories": [
                    *(therapy_note.get("histories") or []),
                    {
                        "note": payment_note,
                        "isEdited": False,
                        "locked": {"by": SYSTEM_USER, "at": to_date_entered},
                        "tags": [],
                        "updated": {"by": SYSTEM_USER, "at": to_date_entered},
                        "ardbCreated": {"by": SYSTEM_USER, "at": to_date_entered},
                        "ardbUpdated": {"by": SYSTEM_USER, "at": to_date_entered},
                        "ardbSourceDocument": get_obj_value(
                            file_metadata, "original_file_name"
                        ),
                    },
                ],
            }


receipt_detail_note_etl = ReceiptDetailNote_Etl()
