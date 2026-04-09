from datetime import datetime
from pathlib import Path
import time
from typing import Any, Dict, List, Optional, Set
import numpy as np
import pandas as pd
from pymongo.operations import UpdateOne

from src.config.config import Config
from src.core.migrate.base_etl import BaseEtl
from src.core.service.documents.model import documentsModel
from src.core.service.invoice_billings.model import invoiceBillingsModel
from src.core.service.therapy_notes.entity import ITherapyNote, TherapyNoteProjectModule
from src.core.service.therapy_notes.model import therapy_notes_model
from src.shared.constant.constant import BATCH_SIZE, SYSTEM_USER
from src.shared.interface.document import DocumentStatusEnum
from src.shared.interface.etl.migration import FileMetadata
from src.shared.interface.migration import InputFileType
from src.shared.utils.batch import get_total_batch
from src.shared.utils.dataframe import batch_iterator
from src.shared.utils.date import format_duration, to_datetime
from src.shared.utils.migration import generate_uuid, verify_and_generate_document
from src.shared.utils.obj import get_obj_value
from src.shared.utils.path import get_input_files_path


class InvoiceBillingNote_Etl(BaseEtl):
    def __init__(self, input_file_path: Path):
        super().__init__()
        self.batch_size = BATCH_SIZE
        self.input_file_path = input_file_path
        self.support_duplicate_documents = Config.get_documents().get(
            "support_duplicate_documents"
        )
        self.enable_backup = False
        self.file_type = InputFileType.EXCEL
        self.sheet_name = "BILLING"
        self.etl_type = "INVOICE_BILLING_NOTE"

    def execute(self):
        all_files = get_input_files_path(
            input_file_path=self.input_file_path, file_type=self.file_type
        )
        print(f"📁 Total files: {len(all_files)}")

        for file in all_files:
            documentId: Optional[str] = None
            start = time.perf_counter()

            try:
                print(f"===========📁 [START] Processing file: {file.name} ===========")

                document_response = verify_and_generate_document(
                    file,
                    self.support_duplicate_documents,
                    "ardb-backup/invoice_billing",
                    self.file_type,
                    self.enable_backup,
                    self.etl_type,
                )
                if document_response is None:
                    continue

                documentId = document_response.get("documentId")
                file_metadata = document_response.get("file_metadata")

                if documentId:
                    documentsModel.get_model().update_one(
                        {"_id": documentId},
                        {"$set": {"status": DocumentStatusEnum.PROCESSING}},
                    )

                print("===========📊 [START] Load on data frame ===========")
                data_frame_load_time = time.perf_counter()
                df = pd.read_excel(
                    file,
                    sheet_name=self.sheet_name,
                    dtype={
                        "INVOICE_BILLING_ID": str,
                        "NOTES": str,
                        "CREATION_DATE": str,
                        "LAST_MODIFIED_DATE": str,
                    },
                )
                print(
                    f"===========📊 [END] Load on data frame in {format_duration(time.perf_counter() - data_frame_load_time)} ==========="
                )

                total_batches = get_total_batch(df)
                print(f"📦📦📦 Total batches: {total_batches}")

                for batch_num, chunk in enumerate(batch_iterator(df)):
                    print(f"Processing batch {batch_num + 1} of {total_batches}")
                    self._load_data(chunk, file_metadata)

                elapsed = time.perf_counter() - start

                if documentId:
                    documentsModel.get_model().update_one(
                        {"_id": documentId},
                        {"$set": {"status": DocumentStatusEnum.COMPLETED}},
                    )

                print(
                    f"==========📁 [END] Processing file: {file.name} in {format_duration(elapsed)} =========="
                )
            except Exception as e:
                print(f"Error processing file: {file.name} - {e}")
                if documentId:
                    documentsModel.get_model().update_one(
                        {"_id": documentId},
                        {
                            "$set": {
                                "status": DocumentStatusEnum.FAILED,
                                "reason": str(e),
                            }
                        },
                    )

                print(
                    f"===========📊 [END] Failed to process file: {file.name} in {format_duration(time.perf_counter() - start)} ==========="
                )

    def _load_data(self, chunk: pd.DataFrame, file_metadata: FileMetadata):
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
        }

    def _build_updated_therapy_note(
        self,
        therapy_note: ITherapyNote,
        note: str,
        to_creation_date: datetime,
        to_last_modified_date: datetime,
        file_metadata: FileMetadata,
    ) -> ITherapyNote:
        date_entered_from_db = get_obj_value(therapy_note, "created", "at")

        if to_last_modified_date > date_entered_from_db:
            return {
                "_id": therapy_note.get("_id"),
                "isEdited": False,
                "created": {"by": SYSTEM_USER, "at": to_creation_date},
                "updated": {"by": SYSTEM_USER, "at": to_last_modified_date},
                "locked": {"by": SYSTEM_USER, "at": to_last_modified_date},
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
                        "updated": therapy_note.get("created"),
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
        existing_date = get_obj_value(therapy_note, "updated", "at")

        if to_last_modified_date > existing_date:
            therapy_note["histories"].append(
                {
                    "note": therapy_note.get("note"),
                    "locked": therapy_note.get("locked"),
                    "isEdited": therapy_note.get("isEdited"),
                    "tags": therapy_note.get("tags"),
                    "updated": therapy_note.get("updated"),
                    "ardbSourceDocument": get_obj_value(
                        therapy_note, "references", "ardbSourceDocument"
                    ),
                }
            )
            therapy_note["note"] = note
            therapy_note["created"] = {"by": SYSTEM_USER, "at": to_creation_date}
            therapy_note["updated"] = {"by": SYSTEM_USER, "at": to_last_modified_date}
            therapy_note["locked"] = {"by": SYSTEM_USER, "at": to_last_modified_date}
        else:
            therapy_note["histories"].append(
                {
                    "note": note,
                    "isEdited": False,
                    "locked": {"by": SYSTEM_USER, "at": to_last_modified_date},
                    "tags": [],
                    "updated": {"by": SYSTEM_USER, "at": to_last_modified_date},
                    "ardbSourceDocument": get_obj_value(
                        file_metadata, "original_file_name"
                    ),
                }
            )
