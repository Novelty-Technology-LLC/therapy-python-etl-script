from datetime import datetime
import time
from typing import Dict, Set
import numpy as np
import pandas as pd
from pymongo.operations import UpdateOne
from src.core.service.invoice_billings.model import invoiceBillingsModel
from src.core.service.therapy_notes.entity import ITherapyNote, TherapyNoteProjectModule
from src.core.service.therapy_notes.model import therapy_notes_model
from src.shared.constant.constant import SYSTEM_USER
from src.shared.interface.etl.migration import FileMetadata
from src.shared.utils.date import format_duration, to_datetime, to_utc_datetime
from src.shared.utils.migration import generate_uuid
from src.shared.utils.obj import get_obj_value


class InvoiceBillingNote_Etl:

    def execute(self, chunk: pd.DataFrame, file_metadata: FileMetadata):
        print("===========📊 [START] Load data ===========")
        data_load_time = time.perf_counter()

        chunk = chunk[chunk["NOTES"].notna()].reset_index(drop=True)
        print(f"📦📦📦 Total rows: {len(chunk)}")

        if len(chunk) == 0:
            print(f"===========📊 [END] No rows to process ===========")
            return

        chunk.replace({np.nan: None}, inplace=True)

        # --- Collect IDs for batch DB queries ---
        collect_invoice_billing_numbers: Set[str] = set()

        for row in chunk.to_dict(orient="records"):
            invoice_billing_number = row.get("INVOICE_BILLING_ID")
            if invoice_billing_number:
                collect_invoice_billing_numbers.add(invoice_billing_number)

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

        collect_invoice_billing_reference_ids: Set[str] = set()
        for invoice_billing in invoice_billings_from_db:
            _id = invoice_billing.get("_id")
            if _id:
                collect_invoice_billing_reference_ids.add(_id)

        therapy_notes_from_db = (
            list(
                therapy_notes_model.get_model().find(
                    filter={
                        "references.invoiceBillingRef.refId": {
                            "$in": list(collect_invoice_billing_reference_ids)
                        }
                    },
                )
            )
            if collect_invoice_billing_reference_ids
            else []
        )

        # --- Build lookup dicts for O(1) access ---
        invoice_billings_by_id: Dict[str, dict] = {
            ib["invoiceBillingNumber"]: ib
            for ib in invoice_billings_from_db
            if ib.get("invoiceBillingNumber")
        }

        therapy_note_by_ib_id: Dict[str, dict] = {}
        for tn in therapy_notes_from_db:
            ref_id = get_obj_value(tn, "references", "invoiceBillingRef", "refId")
            if ref_id:
                therapy_note_by_ib_id[ref_id] = tn

        inserted_notes_by_ib_id: Dict[str, ITherapyNote] = {}
        updated_notes_by_ib_id: Dict[str, ITherapyNote] = {}

        # --- Process rows ---
        for row in chunk.to_dict(orient="records"):
            invoice_billing_id = row.get("INVOICE_BILLING_ID")
            note = row.get("NOTES")
            creation_date = row.get("CREATION_DATE")
            last_modified_date = row.get("LAST_MODIFIED_DATE")

            to_creation_date = to_datetime(creation_date)
            to_last_modified_date = to_datetime(last_modified_date)

            if not note or not invoice_billing_id:
                continue

            invoice_billing = invoice_billings_by_id.get(invoice_billing_id)
            if not invoice_billing:
                continue

            ib_id = invoice_billing.get("_id")
            therapy_note = therapy_note_by_ib_id.get(ib_id)

            if not therapy_note:
                existing = inserted_notes_by_ib_id.get(ib_id)
                if existing:
                    self._apply_note_to_therapy(
                        existing,
                        note,
                        to_creation_date,
                        to_last_modified_date,
                        file_metadata,
                    )
                else:
                    new_note = self._build_new_therapy_note(
                        invoice_billing,
                        note,
                        to_creation_date,
                        to_last_modified_date,
                        file_metadata,
                    )
                    inserted_notes_by_ib_id[ib_id] = new_note
            else:
                existing = updated_notes_by_ib_id.get(ib_id)
                if existing:
                    self._apply_note_to_therapy(
                        existing,
                        note,
                        to_creation_date,
                        to_last_modified_date,
                        file_metadata,
                    )
                else:
                    updated_note = self._build_updated_therapy_note(
                        therapy_note,
                        note,
                        to_creation_date,
                        to_last_modified_date,
                        file_metadata,
                    )
                    updated_notes_by_ib_id[ib_id] = updated_note

        # --- DB writes ---
        inserted_therapy_notes = list(inserted_notes_by_ib_id.values())
        updated_therapy_notes = list(updated_notes_by_ib_id.values())

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

    def _build_new_therapy_note(
        self,
        invoice_billing: dict,
        payment_note: str,
        to_creation_date: datetime,
        to_last_modified_date: datetime,
        file_metadata: FileMetadata,
    ) -> ITherapyNote:
        return {
            "_id": generate_uuid(),
            "histories": [],
            "isEdited": False,
            "locked": {"by": SYSTEM_USER, "at": to_last_modified_date},
            "note": payment_note,
            "tags": [],
            "linkedDocuments": [],
            "references": {
                "module": TherapyNoteProjectModule.INVOICE_BILLING,
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
                    ),
                },
                "patientRef": {
                    "refId": get_obj_value(invoice_billing, "patient", "refId"),
                    "identificationCode": get_obj_value(
                        invoice_billing, "patient", "referenceId"
                    ),
                    "name": get_obj_value(invoice_billing, "patient", "name"),
                },
                "ardbSourceDocument": get_obj_value(
                    file_metadata, "original_file_name"
                ),
            },
            "created": {"by": SYSTEM_USER, "at": to_creation_date},
            "updated": {"by": SYSTEM_USER, "at": to_last_modified_date},
            "ardbCreated": {"by": SYSTEM_USER, "at": to_creation_date},
            "ardbUpdated": {"by": SYSTEM_USER, "at": to_last_modified_date},
        }

    def _build_updated_therapy_note(
        self,
        therapy_note: ITherapyNote,
        note: str,
        to_creation_date: datetime,
        to_last_modified_date: datetime,
        file_metadata: FileMetadata,
    ) -> ITherapyNote:
        date_entered_from_db = to_utc_datetime(
            get_obj_value(therapy_note, "created", "at")
        )

        if to_last_modified_date >= date_entered_from_db:
            return {
                "_id": therapy_note.get("_id"),
                "isEdited": False,
                "created": {"by": SYSTEM_USER, "at": to_creation_date},
                "updated": {"by": SYSTEM_USER, "at": to_last_modified_date},
                "locked": {"by": SYSTEM_USER, "at": to_last_modified_date},
                "ardbCreated": {"by": SYSTEM_USER, "at": to_creation_date},
                "ardbUpdated": {"by": SYSTEM_USER, "at": to_last_modified_date},
                "linkedDocuments": [],
                "references": {
                    **(therapy_note.get("references") or {}),
                    "ardbSourceDocument": get_obj_value(
                        file_metadata, "original_file_name"
                    ),
                },
                "tags": therapy_note.get("tags") or [],
                "note": note,
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
                        "note": note,
                        "isEdited": False,
                        "locked": {"by": SYSTEM_USER, "at": to_last_modified_date},
                        "tags": [],
                        "updated": {"by": SYSTEM_USER, "at": to_last_modified_date},
                        "ardbCreated": {"by": SYSTEM_USER, "at": to_creation_date},
                        "ardbUpdated": {"by": SYSTEM_USER, "at": to_last_modified_date},
                        "ardbSourceDocument": get_obj_value(
                            file_metadata, "original_file_name"
                        ),
                    },
                ],
            }

    def _apply_note_to_therapy(
        self,
        therapy_note: ITherapyNote,
        note: str,
        to_creation_date: datetime,
        to_last_modified_date: datetime,
        file_metadata: FileMetadata,
    ) -> None:
        """Mutates therapy_note in place: promotes or appends based on date comparison."""
        existing_date = to_utc_datetime(get_obj_value(therapy_note, "updated", "at"))

        if to_last_modified_date >= existing_date:
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
            therapy_note["note"] = note
            therapy_note["created"] = {"by": SYSTEM_USER, "at": to_creation_date}
            therapy_note["updated"] = {"by": SYSTEM_USER, "at": to_last_modified_date}
            therapy_note["locked"] = {"by": SYSTEM_USER, "at": to_last_modified_date}
            therapy_note["ardbCreated"] = {"by": SYSTEM_USER, "at": to_creation_date}
            therapy_note["ardbUpdated"] = {
                "by": SYSTEM_USER,
                "at": to_last_modified_date,
            }
        else:
            therapy_note["histories"].append(
                {
                    "note": note,
                    "isEdited": False,
                    "locked": {"by": SYSTEM_USER, "at": to_last_modified_date},
                    "tags": [],
                    "updated": {"by": SYSTEM_USER, "at": to_last_modified_date},
                    "ardbCreated": {"by": SYSTEM_USER, "at": to_creation_date},
                    "ardbUpdated": {"by": SYSTEM_USER, "at": to_last_modified_date},
                    "ardbSourceDocument": get_obj_value(
                        file_metadata, "original_file_name"
                    ),
                }
            )


invoice_billing_note_etl = InvoiceBillingNote_Etl()
