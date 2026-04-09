from datetime import datetime
from pathlib import Path
import time
from typing import Any, Dict, List, Optional, Set
import numpy as np
import pandas as pd
from pymongo.operations import UpdateOne

from src.config.config import Config
from src.core.migrate.base_etl import BaseEtl
from src.core.migrate.invoice_billing_note.interface import IInvoiceBillingDetailQuery
from src.core.service.documents.model import documentsModel
from src.core.service.invoice_billing_details.model import invoiceBillingDetailsModel
from src.core.service.invoice_billings.model import invoiceBillingsModel
from src.core.service.therapy_notes.entity import ITherapyNote, TherapyNoteProjectModule
from src.core.service.therapy_notes.model import therapy_notes_model
from src.shared.constant.constant import BATCH_SIZE, SYSTEM_USER
from src.shared.interface.document import DocumentStatusEnum
from src.shared.interface.etl.migration import FileMetadata
from src.shared.interface.migration import InputFileType
from src.shared.utils.batch import get_total_batch
from src.shared.utils.dataframe import batch_iterator
from src.shared.utils.date import (
    format_duration,
    from_string_to_formatted_date,
    to_datetime,
)
from src.shared.utils.migration import generate_uuid, verify_and_generate_document
from src.shared.utils.obj import get_obj_value
from src.shared.utils.path import get_input_files_path


class InvoiceBillingDetailNote_Etl(BaseEtl):
    def __init__(self, input_file_path: Path):
        super().__init__()
        self.batch_size = BATCH_SIZE
        self.input_file_path = input_file_path
        self.support_duplicate_documents = Config.get_documents().get(
            "support_duplicate_documents"
        )
        self.enable_backup = False
        self.file_type = InputFileType.EXCEL
        self.sheet_name = "BILLING DETAIL"
        self.etl_type = "INVOICE_BILLING_DETAIL_NOTE"

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
                    "ardb-backup/invoice_billing_detail",
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
                        "INVOICE_ITEM_NUMBER": str,
                        "CODE": str,
                        "DATE_OF_SERVICE": str,
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
        collect_invoice_billing_detail_query: List[IInvoiceBillingDetailQuery] = []

        for row in chunk.to_dict(orient="records"):
            invoice_billing_number = row.get("INVOICE_BILLING_ID")

            invoice_item_number = row.get("INVOICE_ITEM_NUMBER")
            invoice_item_number = (
                int(invoice_item_number) if invoice_item_number else None
            )

            code = row.get("CODE")

            date_of_service = row.get("DATE_OF_SERVICE")
            formatted_date_of_service = (
                from_string_to_formatted_date(date_of_service)
                if date_of_service
                else None
            )

            if (
                invoice_billing_number
                and invoice_item_number
                and code
                and formatted_date_of_service
            ):
                collect_invoice_billing_detail_query.append(
                    {
                        "invoiceBillingNumber": invoice_billing_number,
                        "assignedNumber": invoice_item_number,
                        "procedureCode": code,
                        "serviceDate.formattedStartDate": formatted_date_of_service,
                    }
                )

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

        invoice_billing_details_from_db = (
            list(
                invoiceBillingDetailsModel.get_model().find(
                    filter={"$or": collect_invoice_billing_detail_query},
                    projection={
                        "_id": 1,
                        "invoiceBillingNumber": 1,
                        "assignedNumber": 1,
                        "procedureCode": 1,
                        "serviceDate": 1,
                        "invoiceBillingId": 1,
                    },
                )
            )
            if collect_invoice_billing_detail_query
            else []
        )

        collect_invoice_billing_detail_reference_ids: Set[str] = set()
        for invoice_billing in invoice_billing_details_from_db:
            _id = invoice_billing.get("_id")
            if _id:
                collect_invoice_billing_detail_reference_ids.add(_id)

        therapy_notes_from_db = (
            list(
                therapy_notes_model.get_model().find(
                    filter={
                        "references.invoiceBillingDetailRef.refId": {
                            "$in": list(collect_invoice_billing_detail_reference_ids)
                        }
                    },
                )
            )
            if collect_invoice_billing_detail_reference_ids
            else []
        )

        # --- Build lookup dicts for O(1) access ---
        invoice_billings_by_id: Dict[str, dict] = {
            ib["invoiceBillingNumber"]: ib
            for ib in invoice_billings_from_db
            if ib.get("invoiceBillingNumber")
        }

        invoice_billing_details_by_id: Dict[str, dict] = {
            f"{ibd['invoiceBillingNumber']}-{ibd['procedureCode']}-{get_obj_value(ibd, 'serviceDate', 'formattedStartDate')}-{ibd['assignedNumber']}": ibd
            for ibd in invoice_billing_details_from_db
            if ibd.get("invoiceBillingNumber")
            and ibd.get("procedureCode")
            and get_obj_value(ibd, "serviceDate", "formattedStartDate")
            and ibd.get("assignedNumber")
        }

        therapy_note_by_ibd_id: Dict[str, dict] = {}
        for tn in therapy_notes_from_db:
            ref_id = get_obj_value(tn, "references", "invoiceBillingDetailRef", "refId")
            if ref_id:
                therapy_note_by_ibd_id[ref_id] = tn

        inserted_notes_by_ibd_id: Dict[str, ITherapyNote] = {}
        updated_notes_by_ibd_id: Dict[str, ITherapyNote] = {}

        # --- Process rows ---
        for row in chunk.to_dict(orient="records"):
            invoice_billing_number = row.get("INVOICE_BILLING_ID")

            invoice_item_number = row.get("INVOICE_ITEM_NUMBER")
            invoice_item_number = (
                int(invoice_item_number) if invoice_item_number else None
            )

            code = row.get("CODE")

            date_of_service = row.get("DATE_OF_SERVICE")
            formatted_date_of_service = (
                from_string_to_formatted_date(date_of_service)
                if date_of_service
                else None
            )

            note = row.get("NOTES")
            creation_date = row.get("CREATION_DATE")
            last_modified_date = row.get("LAST_MODIFIED_DATE")

            to_creation_date = to_datetime(creation_date)
            to_last_modified_date = to_datetime(last_modified_date)

            if not note or not invoice_billing_number:
                continue

            if not formatted_date_of_service and not invoice_item_number and not code:
                continue

            invoice_billing = invoice_billings_by_id.get(invoice_billing_number)
            if not invoice_billing:
                continue

            invoice_billing_detail = invoice_billing_details_by_id.get(
                f"{invoice_billing_number}-{code}-{formatted_date_of_service}-{invoice_item_number}"
            )
            if not invoice_billing_detail:
                continue

            ibd_id = invoice_billing_detail.get("_id")
            therapy_note = therapy_note_by_ibd_id.get(ibd_id)

            if not therapy_note:
                existing = inserted_notes_by_ibd_id.get(ibd_id)
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
                        invoice_billing_detail,
                        invoice_billing,
                        note,
                        to_creation_date,
                        to_last_modified_date,
                        file_metadata,
                    )
                    inserted_notes_by_ibd_id[ibd_id] = new_note
            else:
                existing = updated_notes_by_ibd_id.get(ibd_id)
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
                    updated_notes_by_ibd_id[ibd_id] = updated_note

        # --- DB writes ---
        inserted_therapy_notes = list(inserted_notes_by_ibd_id.values())
        updated_therapy_notes = list(updated_notes_by_ibd_id.values())

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
        invoice_billing_detail: dict,
        invoice_billing: Optional[dict],
        note: str,
        to_creation_date: datetime,
        to_last_modified_date: datetime,
        file_metadata: FileMetadata,
    ) -> ITherapyNote:
        return {
            "_id": generate_uuid(),
            "histories": [],
            "isEdited": False,
            "locked": {"by": SYSTEM_USER, "at": to_last_modified_date},
            "note": note,
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
                "invoiceBillingDetailRef": {
                    "refId": get_obj_value(invoice_billing_detail, "_id"),
                    "identificationCode": get_obj_value(
                        invoice_billing_detail, "procedureCode"
                    ),
                },
                "procedureCode": get_obj_value(invoice_billing_detail, "procedureCode"),
                "serviceDate": get_obj_value(invoice_billing_detail, "serviceDate"),
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
