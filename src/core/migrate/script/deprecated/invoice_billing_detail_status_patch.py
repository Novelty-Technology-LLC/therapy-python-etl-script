from datetime import datetime
from pathlib import Path
import time
from typing import Any, Dict, List, Optional, Set, TypedDict

from pymongo import UpdateOne
from src.config import config
from src.core.migrate.base_etl import BaseEtl
from src.core.service.documents.model import documentsModel
from src.core.service.invoice_billing_details.model import invoiceBillingDetailsModel
from src.core.service.invoice_billings.model import invoiceBillingsModel
from src.shared.constant.constant import BATCH_SIZE
from src.shared.interface.document import DocumentStatusEnum
from src.shared.interface.etl.migration import FileMetadata
from src.shared.interface.migration import InputFileType
from src.shared.utils.batch import get_total_batch
from src.shared.utils.dataframe import batch_iterator
from src.shared.utils.date import (
    format_duration,
    from_string_to_formatted_date,
    to_datetime,
    to_utc_datetime,
)
from src.shared.utils.migration import verify_and_generate_document
from src.shared.utils.obj import get_obj_value
from src.shared.utils.path import get_input_files_path
import pandas as pd
import numpy as np


IInvoiceBillingDetailQuery = TypedDict(
    "IInvoiceBillingDetailQuery",
    {
        "invoiceBillingNumber": str,
        "assignedNumber": int,
        "procedureCode": str,
        "serviceDate.formattedStartDate": str,
    },
)


class InvoiceBillingDetailStatusPatch(BaseEtl):
    def __init__(self, input_file_path: Path):
        super().__init__()
        self.batch_size = BATCH_SIZE
        self.input_file_path = input_file_path
        self.support_duplicate_documents = config.get_documents().get(
            "support_duplicate_documents"
        )
        self.enable_backup = False
        self.file_type = InputFileType.EXCEL
        self.sheet_name = "BILLING DETAIL"
        self.etl_type = "INVOICE_BILLING_DETAIL_STATUS_PATCH"

    def execute(self):
        print(f"🔄 [PATCH] Invoice Billing Detail Status Patch")
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

                df = pd.read_excel(
                    file,
                    sheet_name=self.sheet_name,
                    dtype={
                        "INVOICE_BILLING_ID": str,
                        "CODE": str,
                        "DATE_OF_SERVICE": str,
                        "INVOICE_ITEM_NUMBER": int,
                        "COMPLETE_FLAG": int,
                        "LAST_MODIFIED_DATE": str,
                    },
                )
                df = df[
                    [
                        "INVOICE_BILLING_ID",
                        "CODE",
                        "DATE_OF_SERVICE",
                        "INVOICE_ITEM_NUMBER",
                        "COMPLETE_FLAG",
                        "LAST_MODIFIED_DATE",
                    ]
                ]

                total_batches = get_total_batch(df)
                print(f"📦📦📦 Total batches: {total_batches}")

                for batch_num, chunk in enumerate(batch_iterator(df)):
                    self._load_data(
                        chunk, batch_num, total_batches, documentId, file_metadata
                    )

                print(
                    f"📊 [END] Processed successfully in {format_duration(time.perf_counter() - file_start_time)}"
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
            f"✅[END] Invoice Billing Detail Status Patch in {format_duration(time.perf_counter() - start_time)}"
        )

    def _load_data(
        self,
        chunk: pd.DataFrame,
        batch_num: int,
        total_batches: int,
        documentId: str,
        file_metadata: FileMetadata,
    ):
        print(f"Processing batch {batch_num + 1} of {total_batches}")
        start_time = time.perf_counter()

        documentsModel.get_model().update_one(
            filter={"_id": documentId},
            update={
                "$set": {
                    "metadata.etl.status": DocumentStatusEnum.PROCESSING,
                    "metadata.etl.processed_at": datetime.now(),
                    "metadata.etl.etl_type": self.etl_type,
                }
            },
        )

        # code
        self._load_invoice_billing_detail_data(chunk, documentId, file_metadata)

        documentsModel.get_model().update_one(
            filter={"_id": documentId},
            update={
                "$set": {
                    "metadata.etl.status": DocumentStatusEnum.COMPLETED,
                    "metadata.etl.completed_at": datetime.now(),
                    "metadata.etl.time_taken": format_duration(
                        time.perf_counter() - start_time
                    ),
                }
            },
        )

        print(
            f"📊 [END] Successfully processed batch {batch_num + 1} of {total_batches} in {format_duration(time.perf_counter() - start_time)}"
        )

    def _load_invoice_billing_detail_data(
        self, chunk: pd.DataFrame, documentId: str, file_metadata: FileMetadata
    ):
        chunk.replace({np.nan: None}, inplace=True)

        collect_invoice_billing_numbers: Set[str] = set()
        collect_invoice_billing_detail_query: List[IInvoiceBillingDetailQuery] = []

        for row in chunk.to_dict(orient="records"):
            invoice_billing_number = row.get("INVOICE_BILLING_ID")
            invoice_item_number = row.get("INVOICE_ITEM_NUMBER")

            code = row.get("CODE")

            date_of_service = row.get("DATE_OF_SERVICE")

            formatted_date_of_service = (
                from_string_to_formatted_date(date_of_service)
                if date_of_service
                else None
            )

            if (
                not invoice_billing_number
                or invoice_item_number is not None
                or not code
                or not formatted_date_of_service
            ):
                continue

            collect_invoice_billing_numbers.add(invoice_billing_number)
            collect_invoice_billing_detail_query.append(
                {
                    "invoiceBillingNumber": invoice_billing_number,
                    "assignedNumber": invoice_item_number,
                    "procedureCode": code,
                    "serviceDate.formattedStartDate": formatted_date_of_service,
                }
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
                        "status": 1,
                        "ardbUpdated": 1,
                        "migrationHistories": 1,
                    },
                )
            )
            if collect_invoice_billing_detail_query
            else []
        )

        invoice_billing_details_by_id: Dict[str, dict] = {
            f"{ibd['invoiceBillingNumber']}-{ibd['procedureCode']}-{get_obj_value(ibd, 'serviceDate', 'formattedStartDate')}-{ibd['assignedNumber']}": ibd
            for ibd in invoice_billing_details_from_db
            if ibd.get("invoiceBillingNumber")
            and ibd.get("procedureCode")
            and get_obj_value(ibd, "serviceDate", "formattedStartDate")
            and ibd.get("assignedNumber")
        }

        collect_invoice_billing_detail_db_ops: Dict[str, Any] = {}

        # --- Process rows ---
        for row in chunk.to_dict(orient="records"):
            invoice_billing_number = row.get("INVOICE_BILLING_ID")

            invoice_item_number = row.get("INVOICE_ITEM_NUMBER")
            invoice_item_number = (
                int(invoice_item_number) if invoice_item_number else None
            )

            code = row.get("CODE")
            creation_date = row.get("CREATION_DATE")
            last_modified_date = row.get("LAST_MODIFIED_DATE")

            to_creation_date = to_datetime(creation_date)
            to_last_modified_date = to_datetime(last_modified_date)

            date_of_service = row.get("DATE_OF_SERVICE")
            formatted_date_of_service = (
                from_string_to_formatted_date(date_of_service)
                if date_of_service
                else None
            )

            status = self._format_status(row.get("COMPLETE_FLAG"))

            if (
                not invoice_billing_number
                or not code
                or not formatted_date_of_service
                or invoice_item_number is not None
            ):
                continue

            invoice_billing_detail = invoice_billing_details_by_id.get(
                f"{invoice_billing_number}-{code}-{formatted_date_of_service}-{invoice_item_number}"
            )

            if not invoice_billing_detail:
                continue

            ibd_id = invoice_billing_detail.get("_id")

            migration_histories = (
                get_obj_value(invoice_billing_detail, "migrationHistories") or []
            )

            existing = collect_invoice_billing_detail_db_ops.get(ibd_id)

            # compare last modified date with ardb updated date
            if not existing:
                to_last_modified_date_from_db = to_utc_datetime(
                    get_obj_value(invoice_billing_detail, "ardbUpdated", "at")
                )

                is_migration_updated_date_exist = (
                    True if to_last_modified_date_from_db is not None else False
                )

                if not is_migration_updated_date_exist:
                    pass

                if is_migration_updated_date_exist:
                    if to_last_modified_date >= to_last_modified_date_from_db:
                        pass
                    else:
                        pass

            else:
                to_last_modified_date_from_db = to_utc_datetime(
                    get_obj_value(existing, "ardbUpdated", "at")
                )

                if to_last_modified_date >= to_last_modified_date_from_db:
                    pass
                else:
                    pass

    def _format_status(self, status: int) -> str:
        return "COMPLETE" if status == 1 else "INCOMPLETE"
