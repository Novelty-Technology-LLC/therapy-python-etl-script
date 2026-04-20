from datetime import datetime
from pathlib import Path
import time
from typing import List, Optional
from src.config.config import Config
from src.core.migrate.base_etl import BaseEtl
from src.core.migrate.therapy_note.invoice_billing_detail_note import (
    invoice_billing_detail_note_etl,
)
from src.core.migrate.therapy_note.invoice_billing_note import invoice_billing_note_etl
from src.core.migrate.therapy_note.receipt_detail_note_etl import (
    receipt_detail_note_etl,
)
from src.core.service.documents.model import documentsModel
from src.core.service.therapy_notes.entity import TherapyNoteProjectModule
from src.shared.constant.constant import BATCH_SIZE
from src.shared.interface.document import DocumentStatusEnum
from src.shared.interface.etl.migration import FileMetadata
from src.shared.interface.migration import InputFileType
from src.shared.utils.batch import get_total_batch
from src.shared.utils.dataframe import batch_iterator
from src.shared.utils.date import format_duration
from src.shared.utils.migration import verify_and_generate_document
from src.shared.utils.path import get_input_files_path
import pandas as pd


class TherapyNote_Etl(BaseEtl):
    def __init__(self, input_file_path: Path):
        super().__init__()
        self.batch_size = BATCH_SIZE
        self.input_file_path = input_file_path
        self.support_duplicate_documents = Config.get_documents().get(
            "support_duplicate_documents"
        )
        self.enable_backup = False
        self.file_type = InputFileType.EXCEL
        self.sheet_names = [
            {
                "sheet_name": "RECEIPTS DETAIL",
                "etl_type": "RECEIPT_DETAIL_NOTE",
                "module": TherapyNoteProjectModule.RECEIPT_DETAIL,
                "dtype": {
                    "RECEIPT_DETAIL_ID": str,
                    "RECEIPT_ID": str,
                    "INVOICE_BILLING_ID": str,
                    "INVOICE_ITEM_NUMBER": str,
                    "PAYMENT_NOTES": str,
                    "DATE_ENTERED": str,
                },
            },
            {
                "sheet_name": "BILLING",
                "etl_type": "INVOICE_BILLING_NOTE",
                "module": TherapyNoteProjectModule.INVOICE_BILLING,
                "dtype": {
                    "INVOICE_BILLING_ID": str,
                    "NOTES": str,
                    "CREATION_DATE": str,
                    "LAST_MODIFIED_DATE": str,
                },
            },
            {
                "sheet_name": "BILLING DETAIL",
                "etl_type": "INVOICE_BILLING_DETAIL_NOTE",
                "module": TherapyNoteProjectModule.INVOICE_BILLING_DETAIL,
                "dtype": {
                    "INVOICE_BILLING_ID": str,
                    "INVOICE_ITEM_NUMBER": str,
                    "CODE": str,
                    "DATE_OF_SERVICE": str,
                    "NOTES": str,
                    "CREATION_DATE": str,
                    "LAST_MODIFIED_DATE": str,
                },
            },
        ]
        self.etl_type = "THERAPY_NOTE"

    def execute(self):
        print(f"🔄 [START] Therapy Note ETL")
        print(f"🔧 [START] Processing therapy note")
        start_time = time.perf_counter()
        all_files = get_input_files_path(
            input_file_path=self.input_file_path, file_type=self.file_type
        )
        print(f"📁 Total files: {len(all_files)}")

        for file in all_files:
            print(f"📁 [START] Processing file: {file.name}")
            documentId: Optional[str] = None
            file_start_time = time.perf_counter()

            try:
                document_response = verify_and_generate_document(
                    file,
                    self.support_duplicate_documents,
                    "ardb-backup/therapy_note",
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

                print(f"📊 [START] Processing sheets")
                sheet_name_from_file: List[str] = pd.ExcelFile(file).sheet_names
                print(f"(📝 Sheet names from file: {", ".join(sheet_name_from_file)})")

                for sheet_name in sheet_name_from_file:
                    print(f"📊 [START] Processing sheet: {sheet_name}")
                    sheet_process_time = time.perf_counter()
                    sheet_name_match = next(
                        (
                            sheet
                            for sheet in self.sheet_names
                            if sheet["sheet_name"] == sheet_name
                        ),
                        None,
                    )
                    etl_type = sheet_name_match["etl_type"]
                    module = sheet_name_match["module"]
                    dtype = sheet_name_match["dtype"]

                    if sheet_name_match is None:
                        print(f"❌ Sheet does not match: {sheet_name}")
                        continue

                    print("📊 [START] Loading on data frame")
                    df = pd.read_excel(file, sheet_name=sheet_name, dtype=dtype)

                    total_batches = get_total_batch(df)
                    print(f"📦📦📦 Total batches: {total_batches}")

                    for batch_num, chunk in enumerate(batch_iterator(df)):
                        print(f"Processing batch {batch_num + 1} of {total_batches}")
                        self._load_data(
                            chunk, etl_type, module, documentId, file_metadata
                        )

                    print(
                        f"📊 [END] Processing sheet: {sheet_name} in {format_duration(time.perf_counter() - sheet_process_time)}"
                    )

                if documentId:
                    documentsModel.get_model().update_one(
                        {"_id": documentId},
                        {"$set": {"status": DocumentStatusEnum.COMPLETED}},
                    )

                print(
                    f"📊 [END] Successfully processed file: {file.name} in {format_duration(time.perf_counter() - file_start_time)}"
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
                    f"📊 [END] Failed to process file: {file.name} in {format_duration(time.perf_counter() - file_start_time)}"
                )

        print(
            f"✅[END] Processed therapy note in {format_duration(time.perf_counter() - start_time)}"
        )

    def _load_data(
        self,
        chunk: pd.DataFrame,
        etl_type: str,
        module: TherapyNoteProjectModule,
        documentId: str,
        file_metadata: FileMetadata,
    ):
        match module:
            case TherapyNoteProjectModule.RECEIPT_DETAIL:
                self._load_receipt_detail_data(
                    chunk, etl_type, module, documentId, file_metadata
                )
            case TherapyNoteProjectModule.INVOICE_BILLING:
                self._load_invoice_billing_data(
                    chunk, etl_type, module, documentId, file_metadata
                )
            case TherapyNoteProjectModule.INVOICE_BILLING_DETAIL:
                self._load_invoice_billing_detail_data(
                    chunk, etl_type, module, documentId, file_metadata
                )
            case _:
                print(f"Invalid module: {module}")

    def _load_receipt_detail_data(
        self,
        chunk: pd.DataFrame,
        etl_type: str,
        module: TherapyNoteProjectModule,
        documentId: str,
        file_metadata: FileMetadata,
    ):
        start_time = time.perf_counter()
        documentsModel.get_model().update_one(
            filter={"_id": documentId},
            update={
                "$set": {
                    "metadata.etl.receipt_detail.status": DocumentStatusEnum.PROCESSING,
                    "metadata.etl.receipt_detail.processed_at": datetime.now(),
                    "metadata.etl.receipt_detail.etl_type": etl_type,
                }
            },
        )
        receipt_detail_note_etl.execute(chunk, file_metadata)

        documentsModel.get_model().update_one(
            filter={"_id": documentId},
            update={
                "$set": {
                    "metadata.etl.receipt_detail.status": DocumentStatusEnum.COMPLETED,
                    "metadata.etl.receipt_detail.completed_at": datetime.now(),
                    "metadata.etl.receipt_detail.time_taken": format_duration(
                        time.perf_counter() - start_time
                    ),
                }
            },
        )

        print(
            f"📊 [END] Successfully processed {module.value} Note in {format_duration(time.perf_counter() - start_time)}"
        )

    def _load_invoice_billing_data(
        self,
        chunk: pd.DataFrame,
        etl_type: str,
        module: TherapyNoteProjectModule,
        documentId: str,
        file_metadata: FileMetadata,
    ):
        start_time = time.perf_counter()
        documentsModel.get_model().update_one(
            filter={"_id": documentId},
            update={
                "$set": {
                    "metadata.etl.invoice_billing.status": DocumentStatusEnum.PROCESSING,
                    "metadata.etl.invoice_billing.processed_at": datetime.now(),
                    "metadata.etl.invoice_billing.etl_type": etl_type,
                }
            },
        )

        invoice_billing_note_etl.execute(chunk, file_metadata)

        documentsModel.get_model().update_one(
            filter={"_id": documentId},
            update={
                "$set": {
                    "metadata.etl.invoice_billing.status": DocumentStatusEnum.COMPLETED,
                    "metadata.etl.invoice_billing.completed_at": datetime.now(),
                    "metadata.etl.invoice_billing.time_taken": format_duration(
                        time.perf_counter() - start_time
                    ),
                }
            },
        )

        print(
            f"📊 [END] Successfully processed {module.value} Note in {format_duration(time.perf_counter() - start_time)}"
        )

    def _load_invoice_billing_detail_data(
        self,
        chunk: pd.DataFrame,
        etl_type: str,
        module: TherapyNoteProjectModule,
        documentId: str,
        file_metadata: FileMetadata,
    ):
        start_time = time.perf_counter()
        documentsModel.get_model().update_one(
            filter={"_id": documentId},
            update={
                "$set": {
                    "metadata.etl.invoice_billing_detail.status": DocumentStatusEnum.PROCESSING,
                    "metadata.etl.invoice_billing_detail.processed_at": datetime.now(),
                    "metadata.etl.invoice_billing_detail.etl_type": etl_type,
                }
            },
        )

        invoice_billing_detail_note_etl.execute(chunk, file_metadata)

        documentsModel.get_model().update_one(
            filter={"_id": documentId},
            update={
                "$set": {
                    "metadata.etl.invoice_billing_detail.status": DocumentStatusEnum.COMPLETED,
                    "metadata.etl.invoice_billing_detail.completed_at": datetime.now(),
                    "metadata.etl.invoice_billing_detail.time_taken": format_duration(
                        time.perf_counter() - start_time
                    ),
                }
            },
        )

        print(
            f"📊 [END] Successfully processed {module.value} Note in {format_duration(time.perf_counter() - start_time)}"
        )
