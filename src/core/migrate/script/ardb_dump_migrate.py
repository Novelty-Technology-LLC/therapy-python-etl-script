from datetime import datetime
from pathlib import Path
import time
from typing import List, Optional
from src.core.migrate.base_etl import BaseEtl
from src.core.service.documents.model import documentsModel
from src.shared.base.base_model import BaseModel
from src.shared.constant.collection_name import CollectionName
from src.shared.interface.document import DocumentStatusEnum
from src.shared.interface.etl.migration import FileMetadata
from src.shared.interface.migration import InputFileType
from src.shared.utils.batch import get_total_batch
from src.shared.utils.dataframe import batch_iterator
from src.shared.utils.date import format_duration
from src.shared.utils.migration import generate_uuid, verify_and_generate_document
from src.shared.utils.path import get_input_files_path
import pandas as pd
import numpy as np


class ArdbDumpMigrate(BaseEtl):
    def __init__(self, input_file_path: Path):
        super().__init__()
        self.input_file_path = input_file_path
        self.support_duplicate_documents = False
        self.enable_backup = False
        self.file_type = InputFileType.EXCEL
        self.sheet_names = [
            {
                "sheet_name": "BILLING",
                "model": BaseModel(CollectionName.ARDB_DUMP_INVOICE_BILLINGS),
                "etl_type": CollectionName.ARDB_DUMP_INVOICE_BILLINGS,
            },
            {
                "sheet_name": "BILLING DETAIL",
                "model": BaseModel(CollectionName.ARDB_DUMP_INVOICE_BILLING_DETAILS),
                "etl_type": CollectionName.ARDB_DUMP_INVOICE_BILLING_DETAILS,
            },
            {
                "sheet_name": "RECEIPTS",
                "model": BaseModel(CollectionName.ARDB_DUMP_RECEIPTS),
                "etl_type": CollectionName.ARDB_DUMP_RECEIPTS,
            },
            {
                "sheet_name": "RECEIPTS DETAIL",
                "model": BaseModel(CollectionName.ARDB_DUMP_RECEIPT_DETAILS),
                "etl_type": CollectionName.ARDB_DUMP_RECEIPT_DETAILS,
            },
            {
                "sheet_name": "AUTH",
                "model": BaseModel(CollectionName.ARDB_DUMP_AUTHORIZATIONS),
                "etl_type": CollectionName.ARDB_DUMP_AUTHORIZATIONS,
            },
        ]
        self.etl_type = "ARDB_DUMP_MIGRATE"

    def execute(self):
        print(f"🔄 [START] Ardb Dump Migrate")
        print(f"🔧 [START] Processing ardb dump")
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
                    if sheet_name_match is None:
                        print(f"❌ Sheet does not match: {sheet_name}")
                        continue

                    print("📊 [START] Loading on data frame")
                    df = pd.read_excel(file, sheet_name=sheet_name, dtype=str)

                    total_batches = get_total_batch(df)
                    print(f"📦📦📦 Total batches: {total_batches}")

                    etl_type = sheet_name_match["etl_type"]

                    documentsModel.get_model().update_one(
                        filter={"_id": documentId},
                        update={
                            "$set": {
                                f"metadata.etl.{etl_type}.status": DocumentStatusEnum.PROCESSING,
                                f"metadata.etl.{etl_type}.processed_at": datetime.now(),
                                f"metadata.etl.{etl_type}.etl_type": self.etl_type,
                                f"metadata.etl.{etl_type}.total_batches": total_batches,
                                f"metadata.etl.{etl_type}.processed_batches": 0,
                            }
                        },
                    )

                    for batch_num, chunk in enumerate(batch_iterator(df)):
                        print(f"Processing batch {batch_num + 1} of {total_batches}")
                        batch_start_time = time.perf_counter()

                        chunk.replace(
                            {np.nan: None, r"^\s*$": None}, regex=True, inplace=True
                        )

                        chunk["ardbSourceDocument"] = file_metadata.get(
                            "original_file_name"
                        )
                        chunk["ardbLastModifiedDate"] = file_metadata.get(
                            "ardb_file_processed_at"
                        )

                        records = []
                        for record in chunk.to_dict("records"):
                            record["_id"] = generate_uuid()

                            records.append(record)

                        sheet_name_match["model"].get_model().insert_many(records)

                        print(
                            f"Processing batch {batch_num + 1} of {total_batches} in {format_duration(time.perf_counter() - batch_start_time)}"
                        )

                        documentsModel.get_model().update_one(
                            filter={"_id": documentId},
                            update={
                                "$set": {
                                    f"metadata.etl.{etl_type}.processed_batches": batch_num
                                    + 1,
                                }
                            },
                        )

                    documentsModel.get_model().update_one(
                        filter={"_id": documentId},
                        update={
                            "$set": {
                                f"metadata.etl.{etl_type}.status": DocumentStatusEnum.COMPLETED,
                                f"metadata.etl.{etl_type}.completed_at": datetime.now(),
                                f"metadata.etl.{etl_type}.time_taken": format_duration(
                                    time.perf_counter() - sheet_process_time
                                ),
                            }
                        },
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
                    f"✅[END] Ardb Dump Migrate in {format_duration(time.perf_counter() - start_time)}"
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
            f"✅[END] Ardb Dump Migrate in {format_duration(time.perf_counter() - start_time)}"
        )
