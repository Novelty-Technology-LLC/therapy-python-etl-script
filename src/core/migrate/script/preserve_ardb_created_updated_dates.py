from datetime import datetime
from pathlib import Path
import time
from typing import Dict, List, Optional, Set

from pymongo import UpdateOne
from src.config.config import Config
from src.core.migrate.base_etl import BaseEtl
from src.core.migrate.therapy_note.interface import IInvoiceBillingDetailQuery
from src.core.service.documents.model import documentsModel
from src.core.service.invoice_billing_details.model import invoiceBillingDetailsModel
from src.core.service.invoice_billings.model import invoiceBillingsModel
from src.core.service.receipt_details.model import receiptDetailsModel
from src.core.service.receipts.model import receiptsModel
from src.shared.constant.constant import BATCH_SIZE, SYSTEM_USER
from src.shared.interface.document import DocumentStatusEnum
from src.shared.interface.etl.migration import FileMetadata
from src.shared.interface.migration import InputFileType
from src.shared.interface.project_module import ProjectModule
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


class PreserveArdbCreatedUpdatedDates(BaseEtl):
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
                "etl_type": "PRESERVE_ARDB_CREATED_AND_UPDATED_DATE_RECEIPT_DETAIL",
                "module": ProjectModule.RECEIPT_DETAIL,
                "dtype": {
                    "RECEIPT_DETAIL_ID": str,
                    "DATE_ENTERED": str,
                },
                "selected_columns": [
                    "RECEIPT_DETAIL_ID",
                    "DATE_ENTERED",
                ],
                "document_metadata_key": "receipt_detail",
            },
            {
                "sheet_name": "RECEIPTS",
                "etl_type": "PRESERVE_ARDB_CREATED_AND_UPDATED_DATE_RECEIPT",
                "module": ProjectModule.RECEIPT,
                "dtype": {
                    "RECEIPT_ID": str,
                    "CREATION_DATE": str,
                    "LAST_MODIFIED_DATE": str,
                },
                "selected_columns": [
                    "RECEIPT_ID",
                    "CREATION_DATE",
                    "LAST_MODIFIED_DATE",
                ],
                "document_metadata_key": "receipt",
            },
            {
                "sheet_name": "BILLING",
                "etl_type": "PRESERVE_ARDB_CREATED_AND_UPDATED_DATE_INVOICE_BILLING",
                "module": ProjectModule.INVOICE_BILLING,
                "dtype": {
                    "INVOICE_BILLING_ID": str,
                    "CREATION_DATE": str,
                    "LAST_MODIFIED_DATE": str,
                },
                "selected_columns": [
                    "INVOICE_BILLING_ID",
                    "CREATION_DATE",
                    "LAST_MODIFIED_DATE",
                ],
                "document_metadata_key": "invoice_billing",
            },
            {
                "sheet_name": "BILLING DETAIL",
                "etl_type": "PRESERVE_ARDB_CREATED_AND_UPDATED_DATE_INVOICE_BILLING_DETAIL",
                "module": ProjectModule.INVOICE_BILLING_DETAIL,
                "dtype": {
                    "INVOICE_BILLING_ID": str,
                    "INVOICE_ITEM_NUMBER": int,
                    "CODE": str,
                    "DATE_OF_SERVICE": str,
                    "CREATION_DATE": str,
                    "LAST_MODIFIED_DATE": str,
                },
                "selected_columns": [
                    "INVOICE_BILLING_ID",
                    "INVOICE_ITEM_NUMBER",
                    "CODE",
                    "DATE_OF_SERVICE",
                    "CREATION_DATE",
                    "LAST_MODIFIED_DATE",
                ],
                "document_metadata_key": "invoice_billing_detail",
            },
        ]
        self.etl_type = "PRESERVE_ARDB_CREATED_AND_UPDATED_DATE"

    def execute(self):
        print(f"🔄 [START] Preserve Ardb Created and Updated Dates ETL")

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
                    "ardb-backup/preserve_ardb_created_updated_dates",
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
                    module = sheet_name_match["module"]
                    dtype = sheet_name_match["dtype"]
                    selected_columns = sheet_name_match["selected_columns"]
                    document_metadata_key = sheet_name_match["document_metadata_key"]

                    print("📊 [START] Loading on data frame")
                    df = pd.read_excel(file, sheet_name=sheet_name, dtype=dtype)

                    total_batches = get_total_batch(df)
                    print(f"📦📦📦 Total batches: {total_batches}")

                    # update document metadata for tracking the process
                    documentsModel.get_model().update_one(
                        filter={"_id": documentId},
                        update={
                            "$set": {
                                f"metadata.etl.{document_metadata_key}.status": DocumentStatusEnum.PROCESSING,
                                f"metadata.etl.{document_metadata_key}.processed_at": datetime.now(),
                                f"metadata.etl.{document_metadata_key}.etl_type": etl_type,
                                f"metadata.etl.{document_metadata_key}.total_batches": total_batches,
                                f"metadata.etl.{document_metadata_key}.processed_batches": 0,
                            }
                        },
                    )

                    for batch_num, chunk in enumerate(batch_iterator(df)):
                        batch_process_time = time.perf_counter()
                        print(f"⏳ Processing batch {batch_num + 1} of {total_batches}")

                        chunk = chunk[selected_columns].replace({np.nan: None})

                        self._load_data(chunk, module, file_metadata)

                        documentsModel.get_model().update_one(
                            filter={"_id": documentId},
                            update={
                                "$set": {
                                    f"metadata.etl.{document_metadata_key}.processed_batches": batch_num
                                    + 1,
                                }
                            },
                        )

                        print(
                            f"⏳ Processed batch {batch_num + 1} of {total_batches} in {format_duration(time.perf_counter() - batch_process_time)}"
                        )

                    documentsModel.get_model().update_one(
                        filter={"_id": documentId},
                        update={
                            "$set": {
                                f"metadata.etl.{document_metadata_key}.status": DocumentStatusEnum.COMPLETED,
                                f"metadata.etl.{document_metadata_key}.processed_at": datetime.now(),
                                f"metadata.etl.{document_metadata_key}.etl_type": etl_type,
                                f"metadata.etl.{document_metadata_key}.time_taken": format_duration(
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
                    f"✅[END] Processed preserve ardb created and updated dates for file: {file.name} in {format_duration(time.perf_counter() - file_start_time)}"
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
            f"✅[END] Preserve Ardb Created and Updated Dates ETL in {format_duration(time.perf_counter() - start_time)}"
        )

    def _load_data(
        self,
        chunk: pd.DataFrame,
        module: ProjectModule,
        file_metadata: FileMetadata,
    ):
        match module:
            case ProjectModule.RECEIPT_DETAIL:
                self._load_receipt_detail_data(chunk, file_metadata)
            case ProjectModule.INVOICE_BILLING:
                self._load_invoice_billing_data(chunk, file_metadata)
            case ProjectModule.INVOICE_BILLING_DETAIL:
                self._load_invoice_billing_detail_data(chunk, file_metadata)

            case ProjectModule.RECEIPT:
                self._load_receipt_data(chunk, file_metadata)
            case _:
                print(f"Invalid module: {module}")

    def _load_receipt_detail_data(
        self,
        chunk: pd.DataFrame,
        file_metadata: FileMetadata,
    ):
        # code
        collect_receipt_detail_reference_ids: Set[str] = set()

        for row in chunk.to_dict(orient="records"):
            receipt_detail_reference_id = row.get("RECEIPT_DETAIL_ID")
            if receipt_detail_reference_id:
                collect_receipt_detail_reference_ids.add(receipt_detail_reference_id)

        # --- Batch DB fetches ---
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
                        "ardbUpdated": 1,
                        "migrationHistories": 1,
                        "ardbAssignerSourceDocument": 1,
                    },
                )
            )
            if collect_receipt_detail_reference_ids
            else []
        )

        # --- Build lookup dicts for O(1) access ---
        receipt_details_by_reference_id: Dict[str, dict] = {
            rd["referenceId"]: rd
            for rd in receipt_details_from_db
            if rd.get("referenceId")
        }

        updated_payload: Dict[str, dict] = {}

        # --- Process rows ---
        for row in chunk.to_dict(orient="records"):
            receipt_detail_reference_id = row.get("RECEIPT_DETAIL_ID")
            last_modified_date = row.get("DATE_ENTERED")

            to_last_modified_date = to_datetime(last_modified_date)

            if not receipt_detail_reference_id:
                continue

            receipt_detail_from_db = receipt_details_by_reference_id.get(
                receipt_detail_reference_id
            )
            if not receipt_detail_from_db:
                continue

            rd_id = receipt_detail_from_db.get("_id")  # _id from receipt detail from db

            if not rd_id:
                continue

            existing = updated_payload.get(rd_id)

            if not existing:
                # to_creation_date_from_db = to_utc_datetime(
                #     get_obj_value(invoice_billing_from_db, "ardbCreated", "at")
                # )
                to_last_modified_date_from_db = to_utc_datetime(
                    get_obj_value(receipt_detail_from_db, "ardbUpdated", "at")
                )

                is_migration_updated_date_exist = (
                    True if to_last_modified_date_from_db is not None else False
                )

                if not is_migration_updated_date_exist:
                    # migration date is not exist, so we directly update the date without history
                    updated_payload[rd_id] = {
                        "_id": rd_id,
                        "ardbUpdated": {"by": SYSTEM_USER, "at": to_last_modified_date},
                        "ardbAssignerSourceDocument": get_obj_value(
                            file_metadata, "original_file_name"
                        ),
                        "migrationHistories": [],
                    }

                if is_migration_updated_date_exist:
                    # comparison between ardb date and db migration date
                    if to_last_modified_date >= to_last_modified_date_from_db:
                        updated_payload[rd_id] = {
                            "_id": rd_id,
                            "ardbUpdated": {
                                "by": SYSTEM_USER,
                                "at": to_last_modified_date,
                            },
                            "ardbAssignerSourceDocument": get_obj_value(
                                file_metadata, "original_file_name"
                            ),
                            "migrationHistories": [
                                *(
                                    get_obj_value(
                                        receipt_detail_from_db, "migrationHistories"
                                    )
                                    or []
                                ),
                                {
                                    "ardbUpdated": get_obj_value(
                                        receipt_detail_from_db, "ardbUpdated"
                                    ),
                                    "ardbAssignerSourceDocument": get_obj_value(
                                        receipt_detail_from_db,
                                        "ardbAssignerSourceDocument",
                                    ),
                                },
                            ],
                        }
                    else:
                        updated_payload[rd_id] = {
                            "_id": rd_id,
                            "ardbUpdated": get_obj_value(
                                receipt_detail_from_db, "ardbUpdated"
                            ),
                            "ardbAssignerSourceDocument": get_obj_value(
                                receipt_detail_from_db, "ardbAssignerSourceDocument"
                            ),
                            "migrationHistories": [
                                *(
                                    get_obj_value(
                                        receipt_detail_from_db, "migrationHistories"
                                    )
                                    or []
                                ),
                                {
                                    "ardbUpdated": {
                                        "by": SYSTEM_USER,
                                        "at": to_last_modified_date,
                                    },
                                    "ardbAssignerSourceDocument": get_obj_value(
                                        file_metadata, "original_file_name"
                                    ),
                                },
                            ],
                        }

            if existing:
                # to_creation_date_from_db = to_utc_datetime(
                #     get_obj_value(existing, "ardbCreated", "at")
                # )
                to_last_modified_date_from_db = to_utc_datetime(
                    get_obj_value(existing, "ardbUpdated", "at")
                )

                if to_last_modified_date >= to_last_modified_date_from_db:
                    existing["migrationHistories"].append(
                        {
                            "ardbUpdated": get_obj_value(existing, "ardbUpdated"),
                            "ardbAssignerSourceDocument": get_obj_value(
                                existing, "ardbAssignerSourceDocument"
                            ),
                        }
                    )
                    existing["ardbUpdated"] = {
                        "by": SYSTEM_USER,
                        "at": to_last_modified_date,
                    }
                    existing["ardbAssignerSourceDocument"] = get_obj_value(
                        file_metadata, "original_file_name"
                    )

                else:
                    existing["ardbCreated"] = get_obj_value(existing, "ardbCreated")
                    existing["ardbUpdated"] = get_obj_value(existing, "ardbUpdated")
                    existing["ardbAssignerSourceDocument"] = get_obj_value(
                        existing, "ardbAssignerSourceDocument"
                    )
                    existing["migrationHistories"] = [
                        *(get_obj_value(existing, "migrationHistories") or []),
                        {
                            "ardbUpdated": {
                                "by": SYSTEM_USER,
                                "at": to_last_modified_date,
                            },
                            "ardbAssignerSourceDocument": get_obj_value(
                                file_metadata, "original_file_name"
                            ),
                        },
                    ]

        # --- DB writes ---
        updated_payload_list = list(updated_payload.values())
        print(f"📦📦📦 Updated payload list: {len(updated_payload_list)}")

        if updated_payload_list:
            receiptDetailsModel.get_model().bulk_write(
                [
                    UpdateOne(
                        {"_id": rd["_id"]},
                        {"$set": {k: v for k, v in rd.items() if k != "_id"}},
                    )
                    for rd in updated_payload_list
                ]
            )

    def _load_invoice_billing_data(
        self,
        chunk: pd.DataFrame,
        file_metadata: FileMetadata,
    ):
        # code
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
                        "ardbCreated": 1,
                        "ardbUpdated": 1,
                        "migrationHistories": 1,
                        "ardbAssignerSourceDocument": 1,
                        "invoiceBillingNumber": 1,
                    },
                )
            )
            if collect_invoice_billing_numbers
            else []
        )

        # --- Build lookup dicts for O(1) access ---
        invoice_billings_by_id: Dict[str, dict] = {
            ib["invoiceBillingNumber"]: ib
            for ib in invoice_billings_from_db
            if ib.get("invoiceBillingNumber")
        }

        updated_payload: Dict[str, dict] = {}

        # --- Process rows ---
        for row in chunk.to_dict(orient="records"):
            invoice_billing_number = row.get("INVOICE_BILLING_ID")
            creation_date = row.get("CREATION_DATE")
            last_modified_date = row.get("LAST_MODIFIED_DATE")

            to_creation_date = to_datetime(creation_date)
            to_last_modified_date = to_datetime(last_modified_date)

            if not invoice_billing_number:
                continue

            invoice_billing_from_db = invoice_billings_by_id.get(invoice_billing_number)
            if not invoice_billing_from_db:
                continue

            ib_id = invoice_billing_from_db.get(
                "_id"
            )  # _id from invoice billing from db

            if not ib_id:
                continue

            existing = updated_payload.get(ib_id)

            if not existing:
                # to_creation_date_from_db = to_utc_datetime(
                #     get_obj_value(invoice_billing_from_db, "ardbCreated", "at")
                # )
                to_last_modified_date_from_db = to_utc_datetime(
                    get_obj_value(invoice_billing_from_db, "ardbUpdated", "at")
                )

                is_migration_updated_date_exist = (
                    True if to_last_modified_date_from_db is not None else False
                )

                if not is_migration_updated_date_exist:
                    # migration date is not exist, so we directly update the date without history
                    updated_payload[ib_id] = {
                        "_id": ib_id,
                        "ardbCreated": {"by": SYSTEM_USER, "at": to_creation_date},
                        "ardbUpdated": {"by": SYSTEM_USER, "at": to_last_modified_date},
                        "ardbAssignerSourceDocument": get_obj_value(
                            file_metadata, "original_file_name"
                        ),
                        "migrationHistories": [],
                    }

                if is_migration_updated_date_exist:
                    # comparison between ardb date and db migration date
                    if to_last_modified_date >= to_last_modified_date_from_db:
                        updated_payload[ib_id] = {
                            "_id": ib_id,
                            "ardbCreated": {"by": SYSTEM_USER, "at": to_creation_date},
                            "ardbUpdated": {
                                "by": SYSTEM_USER,
                                "at": to_last_modified_date,
                            },
                            "ardbAssignerSourceDocument": get_obj_value(
                                file_metadata, "original_file_name"
                            ),
                            "migrationHistories": [
                                *(
                                    get_obj_value(
                                        invoice_billing_from_db, "migrationHistories"
                                    )
                                    or []
                                ),
                                {
                                    "ardbCreated": get_obj_value(
                                        invoice_billing_from_db, "ardbCreated"
                                    ),
                                    "ardbUpdated": get_obj_value(
                                        invoice_billing_from_db, "ardbUpdated"
                                    ),
                                    "ardbAssignerSourceDocument": get_obj_value(
                                        invoice_billing_from_db,
                                        "ardbAssignerSourceDocument",
                                    ),
                                },
                            ],
                        }
                    else:
                        updated_payload[ib_id] = {
                            "_id": ib_id,
                            "ardbCreated": get_obj_value(
                                invoice_billing_from_db, "ardbCreated"
                            ),
                            "ardbUpdated": get_obj_value(
                                invoice_billing_from_db, "ardbUpdated"
                            ),
                            "ardbAssignerSourceDocument": get_obj_value(
                                invoice_billing_from_db, "ardbAssignerSourceDocument"
                            ),
                            "migrationHistories": [
                                *(
                                    get_obj_value(
                                        invoice_billing_from_db, "migrationHistories"
                                    )
                                    or []
                                ),
                                {
                                    "ardbCreated": {
                                        "by": SYSTEM_USER,
                                        "at": to_creation_date,
                                    },
                                    "ardbUpdated": {
                                        "by": SYSTEM_USER,
                                        "at": to_last_modified_date,
                                    },
                                    "ardbAssignerSourceDocument": get_obj_value(
                                        file_metadata, "original_file_name"
                                    ),
                                },
                            ],
                        }

            if existing:
                # to_creation_date_from_db = to_utc_datetime(
                #     get_obj_value(existing, "ardbCreated", "at")
                # )
                to_last_modified_date_from_db = to_utc_datetime(
                    get_obj_value(existing, "ardbUpdated", "at")
                )

                if to_last_modified_date >= to_last_modified_date_from_db:
                    existing["migrationHistories"].append(
                        {
                            "ardbCreated": get_obj_value(existing, "ardbCreated"),
                            "ardbUpdated": get_obj_value(existing, "ardbUpdated"),
                            "ardbAssignerSourceDocument": get_obj_value(
                                existing, "ardbAssignerSourceDocument"
                            ),
                        }
                    )
                    existing["ardbCreated"] = {
                        "by": SYSTEM_USER,
                        "at": to_creation_date,
                    }
                    existing["ardbUpdated"] = {
                        "by": SYSTEM_USER,
                        "at": to_last_modified_date,
                    }
                    existing["ardbAssignerSourceDocument"] = get_obj_value(
                        file_metadata, "original_file_name"
                    )

                else:
                    existing["ardbCreated"] = get_obj_value(existing, "ardbCreated")
                    existing["ardbUpdated"] = get_obj_value(existing, "ardbUpdated")
                    existing["ardbAssignerSourceDocument"] = get_obj_value(
                        existing, "ardbAssignerSourceDocument"
                    )
                    existing["migrationHistories"] = [
                        *(get_obj_value(existing, "migrationHistories") or []),
                        {
                            "ardbCreated": {"by": SYSTEM_USER, "at": to_creation_date},
                            "ardbUpdated": {
                                "by": SYSTEM_USER,
                                "at": to_last_modified_date,
                            },
                            "ardbAssignerSourceDocument": get_obj_value(
                                file_metadata, "original_file_name"
                            ),
                        },
                    ]

        # --- DB writes ---
        updated_payload_list = list(updated_payload.values())
        print(f"📦📦📦 Updated payload list: {len(updated_payload_list)}")

        if updated_payload_list:
            invoiceBillingsModel.get_model().bulk_write(
                [
                    UpdateOne(
                        {"_id": ib["_id"]},
                        {"$set": {k: v for k, v in ib.items() if k != "_id"}},
                    )
                    for ib in updated_payload_list
                ]
            )

    def _load_invoice_billing_detail_data(
        self,
        chunk: pd.DataFrame,
        file_metadata: FileMetadata,
    ):
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
                or not code
                or not formatted_date_of_service
                or invoice_item_number is not None
            ):
                continue

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
                        "ardbUpdated": 1,
                        "ardbCreated": 1,
                        "migrationHistories": 1,
                        "ardbAssignerSourceDocument": 1,
                    },
                )
            )
            if collect_invoice_billing_detail_query
            else []
        )

        # --- Build lookup dicts for O(1) access ---
        invoice_billing_details_by_id: Dict[str, dict] = {
            f"{ibd['invoiceBillingNumber']}-{ibd['procedureCode']}-{get_obj_value(ibd, 'serviceDate', 'formattedStartDate')}-{ibd['assignedNumber']}": ibd
            for ibd in invoice_billing_details_from_db
            if ibd.get("invoiceBillingNumber")
            and ibd.get("procedureCode")
            and get_obj_value(ibd, "serviceDate", "formattedStartDate")
            and ibd.get("assignedNumber")
        }

        updated_payload: Dict[str, dict] = {}

        # --- Process rows ---
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

            creation_date = row.get("CREATION_DATE")
            last_modified_date = row.get("LAST_MODIFIED_DATE")

            to_creation_date = to_datetime(creation_date)
            to_last_modified_date = to_datetime(last_modified_date)

            if (
                not invoice_billing_number
                or not code
                or not formatted_date_of_service
                or invoice_item_number is not None
            ):
                continue

            invoice_billing_detail_from_db = invoice_billing_details_by_id.get(
                f"{invoice_billing_number}-{code}-{formatted_date_of_service}-{invoice_item_number}"
            )
            if not invoice_billing_detail_from_db:
                continue

            ibd_id = invoice_billing_detail_from_db.get("_id")
            if not ibd_id:
                continue

            existing = updated_payload.get(ibd_id)

            if not existing:
                to_last_modified_date_from_db = to_utc_datetime(
                    get_obj_value(invoice_billing_detail_from_db, "ardbUpdated", "at")
                )

                is_migration_updated_date_exist = (
                    True if to_last_modified_date_from_db is not None else False
                )

                if not is_migration_updated_date_exist:
                    # migration date is not exist, so we directly update the date without history
                    updated_payload[ibd_id] = {
                        "_id": ibd_id,
                        "ardbCreated": {"by": SYSTEM_USER, "at": to_creation_date},
                        "ardbUpdated": {"by": SYSTEM_USER, "at": to_last_modified_date},
                        "ardbAssignerSourceDocument": get_obj_value(
                            file_metadata, "original_file_name"
                        ),
                        "migrationHistories": [],
                    }

                if is_migration_updated_date_exist:
                    # comparison between ardb date and db migration date
                    if to_last_modified_date >= to_last_modified_date_from_db:
                        updated_payload[ibd_id] = {
                            "_id": ibd_id,
                            "ardbCreated": {"by": SYSTEM_USER, "at": to_creation_date},
                            "ardbUpdated": {
                                "by": SYSTEM_USER,
                                "at": to_last_modified_date,
                            },
                            "ardbAssignerSourceDocument": get_obj_value(
                                file_metadata, "original_file_name"
                            ),
                            "migrationHistories": [
                                *(
                                    get_obj_value(
                                        invoice_billing_detail_from_db,
                                        "migrationHistories",
                                    )
                                    or []
                                ),
                                {
                                    "ardbCreated": get_obj_value(
                                        invoice_billing_detail_from_db, "ardbCreated"
                                    ),
                                    "ardbUpdated": get_obj_value(
                                        invoice_billing_detail_from_db, "ardbUpdated"
                                    ),
                                    "ardbAssignerSourceDocument": get_obj_value(
                                        invoice_billing_detail_from_db,
                                        "ardbAssignerSourceDocument",
                                    ),
                                },
                            ],
                        }
                    else:
                        updated_payload[ib_id] = {
                            "_id": ib_id,
                            "ardbCreated": get_obj_value(
                                invoice_billing_from_db, "ardbCreated"
                            ),
                            "ardbUpdated": get_obj_value(
                                invoice_billing_from_db, "ardbUpdated"
                            ),
                            "ardbAssignerSourceDocument": get_obj_value(
                                invoice_billing_from_db, "ardbAssignerSourceDocument"
                            ),
                            "migrationHistories": [
                                *(
                                    get_obj_value(
                                        invoice_billing_from_db, "migrationHistories"
                                    )
                                    or []
                                ),
                                {
                                    "ardbCreated": {
                                        "by": SYSTEM_USER,
                                        "at": to_creation_date,
                                    },
                                    "ardbUpdated": {
                                        "by": SYSTEM_USER,
                                        "at": to_last_modified_date,
                                    },
                                    "ardbAssignerSourceDocument": get_obj_value(
                                        file_metadata, "original_file_name"
                                    ),
                                },
                            ],
                        }

            if existing:
                # to_creation_date_from_db = to_utc_datetime(
                #     get_obj_value(existing, "ardbCreated", "at")
                # )
                to_last_modified_date_from_db = to_utc_datetime(
                    get_obj_value(existing, "ardbUpdated", "at")
                )

                if to_last_modified_date >= to_last_modified_date_from_db:
                    existing["migrationHistories"].append(
                        {
                            "ardbCreated": get_obj_value(existing, "ardbCreated"),
                            "ardbUpdated": get_obj_value(existing, "ardbUpdated"),
                            "ardbAssignerSourceDocument": get_obj_value(
                                existing, "ardbAssignerSourceDocument"
                            ),
                        }
                    )
                    existing["ardbCreated"] = {
                        "by": SYSTEM_USER,
                        "at": to_creation_date,
                    }
                    existing["ardbUpdated"] = {
                        "by": SYSTEM_USER,
                        "at": to_last_modified_date,
                    }
                    existing["ardbAssignerSourceDocument"] = get_obj_value(
                        file_metadata, "original_file_name"
                    )

                else:
                    existing["ardbCreated"] = get_obj_value(existing, "ardbCreated")
                    existing["ardbUpdated"] = get_obj_value(existing, "ardbUpdated")
                    existing["ardbAssignerSourceDocument"] = get_obj_value(
                        existing, "ardbAssignerSourceDocument"
                    )
                    existing["migrationHistories"] = [
                        *(get_obj_value(existing, "migrationHistories") or []),
                        {
                            "ardbCreated": {"by": SYSTEM_USER, "at": to_creation_date},
                            "ardbUpdated": {
                                "by": SYSTEM_USER,
                                "at": to_last_modified_date,
                            },
                            "ardbAssignerSourceDocument": get_obj_value(
                                file_metadata, "original_file_name"
                            ),
                        },
                    ]

        # --- DB writes ---
        updated_payload_list = list(updated_payload.values())
        print(f"📦📦📦 Updated payload list: {len(updated_payload_list)}")

        if updated_payload_list:
            invoiceBillingDetailsModel.get_model().bulk_write(
                [
                    UpdateOne(
                        {"_id": ibd["_id"]},
                        {"$set": {k: v for k, v in ibd.items() if k != "_id"}},
                    )
                    for ibd in updated_payload_list
                ]
            )

    def _load_receipt_data(
        self,
        chunk: pd.DataFrame,
        file_metadata: FileMetadata,
    ):
        # code
        collect_receipt_reference_ids: Set[str] = set()

        for row in chunk.to_dict(orient="records"):
            receipt_reference_id = row.get("RECEIPT_ID")
            if receipt_reference_id:
                collect_receipt_reference_ids.add(receipt_reference_id)

        # --- Batch DB fetches ---
        receipts_from_db = (
            list(
                receiptsModel.get_model().find(
                    filter={
                        "referenceId": {"$in": list(collect_receipt_reference_ids)}
                    },
                    projection={
                        "_id": 1,
                        "referenceId": 1,
                        "ardbCreated": 1,
                        "ardbUpdated": 1,
                        "migrationHistories": 1,
                        "ardbAssignerSourceDocument": 1,
                    },
                )
            )
            if collect_receipt_reference_ids
            else []
        )

        # --- Build lookup dicts for O(1) access ---
        receipts_by_reference_id: Dict[str, dict] = {
            r["referenceId"]: r for r in receipts_from_db if r.get("referenceId")
        }

        updated_payload: Dict[str, dict] = {}

        # --- Process rows ---
        for row in chunk.to_dict(orient="records"):
            receipt_reference_id = row.get("RECEIPT_ID")
            creation_date = row.get("CREATION_DATE")
            last_modified_date = row.get("LAST_MODIFIED_DATE")

            to_creation_date = to_datetime(creation_date)
            to_last_modified_date = to_datetime(last_modified_date)

            if not receipt_reference_id:
                continue

            receipt_from_db = receipts_by_reference_id.get(receipt_reference_id)
            if not receipt_from_db:
                continue

            r_id = receipt_from_db.get("_id")  # _id from receipt from db

            if not r_id:
                continue

            existing = updated_payload.get(r_id)

            if not existing:
                # to_creation_date_from_db = to_utc_datetime(
                #     get_obj_value(invoice_billing_from_db, "ardbCreated", "at")
                # )
                to_last_modified_date_from_db = to_utc_datetime(
                    get_obj_value(receipt_from_db, "ardbUpdated", "at")
                )

                is_migration_updated_date_exist = (
                    True if to_last_modified_date_from_db is not None else False
                )

                if not is_migration_updated_date_exist:
                    # migration date is not exist, so we directly update the date without history
                    updated_payload[r_id] = {
                        "_id": r_id,
                        "ardbCreated": {"by": SYSTEM_USER, "at": to_creation_date},
                        "ardbUpdated": {"by": SYSTEM_USER, "at": to_last_modified_date},
                        "ardbAssignerSourceDocument": get_obj_value(
                            file_metadata, "original_file_name"
                        ),
                        "migrationHistories": [],
                    }

                if is_migration_updated_date_exist:
                    # comparison between ardb date and db migration date
                    if to_last_modified_date >= to_last_modified_date_from_db:
                        updated_payload[r_id] = {
                            "_id": r_id,
                            "ardbCreated": {"by": SYSTEM_USER, "at": to_creation_date},
                            "ardbUpdated": {
                                "by": SYSTEM_USER,
                                "at": to_last_modified_date,
                            },
                            "ardbAssignerSourceDocument": get_obj_value(
                                file_metadata, "original_file_name"
                            ),
                            "migrationHistories": [
                                *(
                                    get_obj_value(receipt_from_db, "migrationHistories")
                                    or []
                                ),
                                {
                                    "ardbCreated": get_obj_value(
                                        receipt_from_db, "ardbCreated"
                                    ),
                                    "ardbUpdated": get_obj_value(
                                        receipt_from_db, "ardbUpdated"
                                    ),
                                    "ardbAssignerSourceDocument": get_obj_value(
                                        receipt_from_db,
                                        "ardbAssignerSourceDocument",
                                    ),
                                },
                            ],
                        }
                    else:
                        updated_payload[r_id] = {
                            "_id": r_id,
                            "ardbCreated": get_obj_value(
                                receipt_from_db, "ardbCreated"
                            ),
                            "ardbUpdated": get_obj_value(
                                receipt_from_db, "ardbUpdated"
                            ),
                            "ardbAssignerSourceDocument": get_obj_value(
                                receipt_from_db, "ardbAssignerSourceDocument"
                            ),
                            "migrationHistories": [
                                *(
                                    get_obj_value(receipt_from_db, "migrationHistories")
                                    or []
                                ),
                                {
                                    "ardbCreated": {
                                        "by": SYSTEM_USER,
                                        "at": to_creation_date,
                                    },
                                    "ardbUpdated": {
                                        "by": SYSTEM_USER,
                                        "at": to_last_modified_date,
                                    },
                                    "ardbAssignerSourceDocument": get_obj_value(
                                        file_metadata, "original_file_name"
                                    ),
                                },
                            ],
                        }

            if existing:
                # to_creation_date_from_db = to_utc_datetime(
                #     get_obj_value(existing, "ardbCreated", "at")
                # )
                to_last_modified_date_from_db = to_utc_datetime(
                    get_obj_value(existing, "ardbUpdated", "at")
                )

                if to_last_modified_date >= to_last_modified_date_from_db:
                    existing["migrationHistories"].append(
                        {
                            "ardbCreated": get_obj_value(existing, "ardbCreated"),
                            "ardbUpdated": get_obj_value(existing, "ardbUpdated"),
                            "ardbAssignerSourceDocument": get_obj_value(
                                existing, "ardbAssignerSourceDocument"
                            ),
                        }
                    )
                    existing["ardbCreated"] = {
                        "by": SYSTEM_USER,
                        "at": to_creation_date,
                    }
                    existing["ardbUpdated"] = {
                        "by": SYSTEM_USER,
                        "at": to_last_modified_date,
                    }
                    existing["ardbAssignerSourceDocument"] = get_obj_value(
                        file_metadata, "original_file_name"
                    )

                else:
                    existing["ardbCreated"] = get_obj_value(existing, "ardbCreated")
                    existing["ardbUpdated"] = get_obj_value(existing, "ardbUpdated")
                    existing["ardbAssignerSourceDocument"] = get_obj_value(
                        existing, "ardbAssignerSourceDocument"
                    )
                    existing["migrationHistories"] = [
                        *(get_obj_value(existing, "migrationHistories") or []),
                        {
                            "ardbCreated": {"by": SYSTEM_USER, "at": to_creation_date},
                            "ardbUpdated": {
                                "by": SYSTEM_USER,
                                "at": to_last_modified_date,
                            },
                            "ardbAssignerSourceDocument": get_obj_value(
                                file_metadata, "original_file_name"
                            ),
                        },
                    ]

        # --- DB writes ---
        updated_payload_list = list(updated_payload.values())
        print(f"📦📦📦 Updated payload list: {len(updated_payload_list)}")

        if updated_payload_list:
            receiptsModel.get_model().bulk_write(
                [
                    UpdateOne(
                        {"_id": r["_id"]},
                        {"$set": {k: v for k, v in r.items() if k != "_id"}},
                    )
                    for r in updated_payload_list
                ]
            )
