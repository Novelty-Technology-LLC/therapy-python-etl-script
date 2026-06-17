from pathlib import Path
import time
from typing import List, Optional
from src.config.config import Config
from src.core.migrate.base_etl import BaseEtl
from src.core.migrate.map_provider_to_billing_and_payment.payee_to_billing import (
    payee_to_billing,
)
from src.core.migrate.map_provider_to_billing_and_payment.rendering_and_location_to_billing import (
    rendering_and_location_to_billing,
)
from src.core.service.documents.model import documentsModel
from src.shared.interface.document import DocumentStatusEnum
from src.shared.interface.etl.migration import FileMetadata
from src.shared.interface.migration import InputFileType
from src.shared.utils.batch import get_total_batch
from src.shared.utils.dataframe import batch_iterator
from src.shared.utils.date import format_duration
from src.shared.utils.migration import verify_and_generate_document
from src.shared.utils.path import get_input_files_path
import pandas as pd
from datetime import datetime


class ProviderMapToBillingAndPayment(BaseEtl):
    def __init__(self, input_file_path: Path):
        super().__init__()
        self.input_file_path = input_file_path
        self.support_duplicate_documents = Config.get_documents().get(
            "support_duplicate_documents"
        )
        self.enable_backup = False
        self.file_type = InputFileType.EXCEL
        self.sheet_names = [
            {
                "sheet_name": "BILLING",
                "etl_type": "MAP_RENDERING_AND_LOCATION_TO_BILLING_AND_PAYMENT",
                "dtype": {
                    "INVOICE_BILLING_ID": str,
                    "PROVIDER_ID": str,
                    "LOCATION_ID": str,
                },
            },
            {
                "sheet_name": "CLAIMS",
                "etl_type": "MAP_PAYEE_TO_BILLING_AND_PAYMENT",
                "dtype": {
                    "CLAIM_ID": str,
                    "PAYEE_ID": str,
                },
            },
        ]
        self.etl_type = "PROVIDER_MAP_TO_BILLING_AND_PAYMENT"

    def execute(self):
        print(f"🔄 [START] Provider Map to Billing and Payment ETL")
        print(f"🔧 [START] Processing provider map to billing and payment")

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
                    "ardb-backup/provider_map_to_billing_and_payment",
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

                    etl_type = sheet_name_match["etl_type"]
                    sheet_name = sheet_name_match["sheet_name"]
                    dtype = sheet_name_match["dtype"]

                    print("📊 [START] Loading on data frame")
                    df = pd.read_excel(file, sheet_name=sheet_name, dtype=dtype)

                    total_batches = get_total_batch(df)
                    print(f"📦📦📦 Total batches: {total_batches}")

                    for batch_num, chunk in enumerate(batch_iterator(df)):
                        print(f"Processing batch {batch_num + 1} of {total_batches}")
                        self._load_data(
                            chunk, etl_type, sheet_name, documentId, file_metadata
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
            f"✅[END] Processed provider map to billing and payment in {format_duration(time.perf_counter() - start_time)}"
        )

    def _load_data(
        self,
        chunk: pd.DataFrame,
        etl_type: str,
        sheet_name: str,
        documentId: str,
        file_metadata: FileMetadata,
    ):
        match sheet_name:
            case "BILLING":
                self._load_rendering_and_location_to_billing_and_payment(
                    chunk, etl_type, sheet_name, documentId, file_metadata
                )
            case "CLAIMS":
                self._load_payee_to_billing_and_payment(
                    chunk, etl_type, sheet_name, documentId, file_metadata
                )
            case _:
                print(f"Invalid sheet name: {sheet_name}")

    def _load_rendering_and_location_to_billing_and_payment(
        self,
        chunk: pd.DataFrame,
        etl_type: str,
        sheet_name: str,
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

        ## load function here
        rendering_and_location_to_billing.execute(chunk, file_metadata)

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
            f"📊 [END] Successfully processed {sheet_name} in {format_duration(time.perf_counter() - start_time)}"
        )

    def _load_payee_to_billing_and_payment(
        self,
        chunk: pd.DataFrame,
        etl_type: str,
        sheet_name: str,
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

        ## load function here
        payee_to_billing.execute(chunk, file_metadata)

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
            f"📊 [END] Successfully processed {sheet_name} in {format_duration(time.perf_counter() - start_time)}"
        )
