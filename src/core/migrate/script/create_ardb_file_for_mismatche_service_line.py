from pathlib import Path
import time
from typing import List, Optional, Set
from src.config.config import Config
from src.core.migrate.base_etl import BaseEtl
from src.core.service.documents.model import documentsModel
from src.core.service.invoice_billing_details.model import invoiceBillingDetailsModel
from src.core.service.therapy_notes.entity import TherapyNoteProjectModule
from src.shared.constant.constant import BATCH_SIZE
from src.shared.interface.document import DocumentStatusEnum
from src.shared.interface.migration import InputFileType
from src.shared.utils.date import format_duration
from src.shared.utils.migration import verify_and_generate_document
from src.shared.utils.obj import get_obj_value
from src.shared.utils.path import get_input_files_path
import pandas as pd
from datetime import datetime
import os
import numpy as np


class CreateArdbFileForMismatchedServiceLine(BaseEtl):
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
                "sheet_name": "BILLING",
                "etl_type": "INVOICE_BILLING_MISMATCHED_SERVICE_LINE",
                "module": TherapyNoteProjectModule.INVOICE_BILLING,
                "dtype": {
                    "INVOICE_BILLING_ID": str,
                },
            },
            {
                "sheet_name": "BILLING DETAIL",
                "etl_type": "INVOICE_BILLING_DETAIL_MISMATCHED_SERVICE_LINE",
                "module": TherapyNoteProjectModule.INVOICE_BILLING_DETAIL,
                "dtype": {
                    "INVOICE_BILLING_ID": str,
                    # "INVOICE_ITEM_NUMBER": int,
                    # "CODE": str,
                    # "DATE_OF_SERVICE": str,
                },
            },
            {
                "sheet_name": "RECEIPTS DETAIL",
                "etl_type": "RECEIPT_DETAIL_MISMATCHED_SERVICE_LINE",
                "module": TherapyNoteProjectModule.RECEIPT_DETAIL,
                "dtype": {
                    "RECEIPT_ID": str,
                    "INVOICE_BILLING_ID": str,
                    # "INVOICE_ITEM_NUMBER": int,
                },
            },
            {
                "sheet_name": "RECEIPTS",
                "etl_type": "RECEIPT_MISMATCHED_SERVICE_LINE",
                "module": TherapyNoteProjectModule.RECEIPT,
                "dtype": {
                    "RECEIPT_ID": str,
                },
            },
        ]

        self.mismatched_invoice_billing_ids: List[str] = []

        self.etl_type = "MISMATCHED_SERVICE_LINE"
        self.output_file_path = Path("input-files/output/")
        self.receipt_ids: Set[str] = set()

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
        print(f"🔄 [START] Create Ardb File for Mismatched Service Line ETL")
        print(f"🔧 [START] Processing ardb files")
        start_time = time.perf_counter()
        all_files = get_input_files_path(
            input_file_path=self.input_file_path, file_type=self.file_type
        )
        print(f"📁 Total files: {len(all_files)}")

        # if len(all_files):
        #     self.find_mismatched_invoice_billing_ids()

        if len(self.mismatched_invoice_billing_ids):
            print(
                f"📦📦📦 Total mismatched invoice billing ids: {len(self.mismatched_invoice_billing_ids)}"
            )
        else:
            print(f"📦📦📦 No mismatched invoice billing ids found")
            return

        for file in all_files:
            print(f"📁 [START] Processing file: {file.name}")
            documentId: Optional[str] = None
            file_start_time = time.perf_counter()

            # clear receipt ids set
            self.receipt_ids.clear()

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
                sheet_names_from_file: List[str] = pd.ExcelFile(file).sheet_names
                print(f"(📝 Sheet names from file: {", ".join(sheet_names_from_file)})")

                for sheet in self.sheet_names:
                    print(f"📊 [START] Processing sheet: {sheet['sheet_name']}")
                    sheet_process_time = time.perf_counter()

                    sheet_name_match = next(
                        (
                            sheet
                            for sheet_name_from_file in sheet_names_from_file
                            if sheet_name_from_file == sheet["sheet_name"]
                        ),
                        None,
                    )
                    if sheet_name_match is None:
                        print(f"❌ Sheet does not match: {sheet['sheet_name']}")
                        continue

                    etl_type = sheet_name_match["etl_type"]
                    module = sheet_name_match["module"]
                    dtype = sheet_name_match["dtype"]

                    print("📊 [START] Loading on data frame")
                    df = pd.read_excel(file, sheet_name=sheet["sheet_name"], dtype=str)

                    self._load_data(
                        df, etl_type, module, documentId, file.name, sheet["sheet_name"]
                    )

                    print(
                        f"📊 [END] Processing sheet: {sheet['sheet_name']} in {format_duration(time.perf_counter() - sheet_process_time)}"
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
            f"✅[END] Created ardb file for mismatched service line in {format_duration(time.perf_counter() - start_time)}"
        )

    def write_sheet(self, data: pd.DataFrame, file_name: str, sheet_name: str):

        data = data.where(data.notna(), "NULL")

        file_path = Path(self.output_file_path) / file_name
        if os.path.exists(file_path):
            # file exists -> open in append mode and add the sheet
            with pd.ExcelWriter(
                file_path, engine="openpyxl", mode="a", if_sheet_exists="replace"
            ) as writer:
                data.to_excel(writer, sheet_name=sheet_name, index=False)
        else:
            # file doesn't exist -> create it with this sheet
            with pd.ExcelWriter(file_path, engine="openpyxl", mode="w") as writer:
                data.to_excel(writer, sheet_name=sheet_name, index=False)

    def _load_data(
        self,
        df: pd.DataFrame,
        etl_type: str,
        module: TherapyNoteProjectModule,
        documentId: str,
        file_name: str,
        sheet_name: str,
    ):
        match module:
            case TherapyNoteProjectModule.RECEIPT_DETAIL:
                self._load_receipt_detail_data(
                    df,
                    etl_type,
                    module,
                    documentId,
                    sheet_name,
                    file_name,
                )
            case TherapyNoteProjectModule.INVOICE_BILLING:
                self._load_invoice_billing_data(
                    df,
                    etl_type,
                    module,
                    documentId,
                    sheet_name,
                    file_name,
                )
            case TherapyNoteProjectModule.INVOICE_BILLING_DETAIL:
                self._load_invoice_billing_detail_data(
                    df,
                    etl_type,
                    module,
                    documentId,
                    sheet_name,
                    file_name,
                )

            case TherapyNoteProjectModule.RECEIPT:
                self._load_receipt_data(
                    df,
                    etl_type,
                    module,
                    documentId,
                    sheet_name,
                    file_name,
                )

            case _:
                print(f"Invalid module: {module}")

    def _load_invoice_billing_data(
        self,
        df: pd.DataFrame,
        etl_type: str,
        module: TherapyNoteProjectModule,
        documentId: str,
        sheet_name: str,
        file_name: str,
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

        self._execute_invoice_billing_data(df, sheet_name, file_name)

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
            f"📊 [END] Successfully processed {module.value} in {format_duration(time.perf_counter() - start_time)}"
        )

    def _load_invoice_billing_detail_data(
        self,
        df: pd.DataFrame,
        etl_type: str,
        module: TherapyNoteProjectModule,
        documentId: str,
        sheet_name: str,
        file_name: str,
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

        self._execute_invoice_billing_detail_data(df, sheet_name, file_name)

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
            f"📊 [END] Successfully processed {module.value} in {format_duration(time.perf_counter() - start_time)}"
        )

    def _load_receipt_detail_data(
        self,
        df: pd.DataFrame,
        etl_type: str,
        module: TherapyNoteProjectModule,
        documentId: str,
        sheet_name: str,
        file_name: str,
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

        self._execute_receipt_detail_data(df, sheet_name, file_name)

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
            f"📊 [END] Successfully processed {module.value} in {format_duration(time.perf_counter() - start_time)}"
        )

    def _load_receipt_data(
        self,
        df: pd.DataFrame,
        etl_type: str,
        module: TherapyNoteProjectModule,
        documentId: str,
        sheet_name: str,
        file_name: str,
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

        self._execute_receipt_data(df, sheet_name, file_name)

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
            f"📊 [END] Successfully processed {module.value} in {format_duration(time.perf_counter() - start_time)}"
        )

    def _execute_invoice_billing_data(
        self,
        df: pd.DataFrame,
        sheet_name: str,
        file_name: str,
    ):
        print("===========📊 [START] Load data ===========")

        data_load_time = time.perf_counter()

        filtered_chunk = df[
            df["INVOICE_BILLING_ID"].isin(self.mismatched_invoice_billing_ids)
        ]

        print(f"📦📦📦 Total rows: {len(filtered_chunk)}")

        if len(filtered_chunk) == 0:
            print(f"===========📊 [END] No rows to process ===========")
            return

        self.write_sheet(filtered_chunk, file_name, sheet_name)

        print(
            f"===========📊 [END] Load data in {format_duration(time.perf_counter() - data_load_time)} ==========="
        )

    def _execute_invoice_billing_detail_data(
        self,
        df: pd.DataFrame,
        sheet_name: str,
        file_name: str,
    ):
        print("===========📊 [START] Load data ===========")

        data_load_time = time.perf_counter()

        filtered_chunk = df[
            df["INVOICE_BILLING_ID"].isin(self.mismatched_invoice_billing_ids)
        ]

        print(f"📦📦📦 Total rows: {len(filtered_chunk)}")

        if len(filtered_chunk) == 0:
            print(f"===========📊 [END] No rows to process ===========")
            return

        self.write_sheet(filtered_chunk, file_name, sheet_name)

        print(
            f"===========📊 [END] Load data in {format_duration(time.perf_counter() - data_load_time)} ==========="
        )

    def _execute_receipt_detail_data(
        self,
        df: pd.DataFrame,
        sheet_name: str,
        file_name: str,
    ):
        print("===========📊 [START] Load data ===========")

        data_load_time = time.perf_counter()

        filtered_chunk = df[
            df["INVOICE_BILLING_ID"].isin(self.mismatched_invoice_billing_ids)
        ]

        print(f"📦📦📦 Total rows: {len(filtered_chunk)}")

        if len(filtered_chunk) == 0:
            print(f"===========📊 [END] No rows to process ===========")
            return

        # store receipt ids in a set
        receipt_ids = set(filtered_chunk["RECEIPT_ID"].tolist())
        if len(receipt_ids) > 0:
            self.receipt_ids.update(receipt_ids)

        self.write_sheet(filtered_chunk, file_name, sheet_name)

        print(
            f"===========📊 [END] Load data in {format_duration(time.perf_counter() - data_load_time)} ==========="
        )

    def _execute_receipt_data(
        self,
        df: pd.DataFrame,
        sheet_name: str,
        file_name: str,
    ):
        print("===========📊 [START] Load data ===========")

        data_load_time = time.perf_counter()

        if len(self.receipt_ids) == 0:
            print(f"===========📊 [END] No rows to process ===========")
            return

        filtered_chunk = df[df["RECEIPT_ID"].isin(self.receipt_ids)]

        print(f"📦📦📦 Total rows: {len(filtered_chunk)}")

        if len(filtered_chunk) == 0:
            print(f"===========📊 [END] No rows to process ===========")
            return

        self.write_sheet(filtered_chunk, file_name, sheet_name)

        print(
            f"===========📊 [END] Load data in {format_duration(time.perf_counter() - data_load_time)} ==========="
        )
