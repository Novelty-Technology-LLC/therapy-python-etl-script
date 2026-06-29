from pathlib import Path
import time
from typing import List, Optional
from src.config.config import Config
from src.core.migrate.base_etl import BaseEtl
from src.core.service.authorization.model import authorizationsModel
from src.core.service.documents.model import documentsModel
from src.core.service.therapy_notes.entity import TherapyNoteProjectModule
from src.shared.constant.constant import BATCH_SIZE
from src.shared.interface.document import DocumentStatusEnum
from src.shared.interface.etl.sheet_name import SheetName
from src.shared.interface.migration import InputFileType
from src.shared.interface.project_module import ProjectModule
from src.shared.utils.date import format_duration
from src.shared.utils.migration import verify_and_generate_document
from src.shared.utils.path import get_input_files_path
import pandas as pd
from datetime import datetime
import os
import numpy as np


class CreateArdbFileForMissingAuthorization(BaseEtl):
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
                "sheet_name": "AUTH",
                "etl_type": "MISSING_AUTHORIZATION",
                "module": ProjectModule.AUTHORIZATION,
                "dtype": {
                    "AUTHORIZATION_NUMBER": str,
                },
            },
        ]

        self.etl_type = "MISSING_AUTHORIZATION"
        self.output_file_path = Path("input-files/output/missing-authorization/")

    def execute(self):
        print(f"🔄 [START] Create Ardb File for Missing Authorization ETL")
        print(f"🔧 [START] Processing ardb files")
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
                    "ardb-backup/auth",
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

                    self._load_authorization_data(
                        df, etl_type, module, documentId, sheet["sheet_name"], file.name
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
            f"✅[END] Created ardb file for missing authorization in {format_duration(time.perf_counter() - start_time)}"
        )

    def _load_authorization_data(
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
                    "metadata.etl.authorization.status": DocumentStatusEnum.PROCESSING,
                    "metadata.etl.authorization.processed_at": datetime.now(),
                    "metadata.etl.authorization.etl_type": etl_type,
                }
            },
        )

        self._execute_authorization_data(df, sheet_name, file_name)

        documentsModel.get_model().update_one(
            filter={"_id": documentId},
            update={
                "$set": {
                    "metadata.etl.authorization.status": DocumentStatusEnum.COMPLETED,
                    "metadata.etl.authorization.completed_at": datetime.now(),
                    "metadata.etl.authorization.time_taken": format_duration(
                        time.perf_counter() - start_time
                    ),
                }
            },
        )

        print(
            f"📊 [END] Successfully processed {module.value} in {format_duration(time.perf_counter() - start_time)}"
        )

    def _execute_authorization_data(
        self,
        df: pd.DataFrame,
        sheet_name: str,
        file_name: str,
    ):
        print("===========📊 [START] Load data ===========")

        data_load_time = time.perf_counter()

        authorization_numbers = df["AUTHORIZATION_NUMBER"].unique().tolist()

        print(f"📦📦📦 Total authorization numbers: {len(authorization_numbers)}")

        if len(authorization_numbers) == 0:
            print(f"===========📊 [END] No rows to process ===========")
            return

        authorizationsFromDb = list(
            authorizationsModel.get_model().find(
                filter={"authNumber": {"$in": authorization_numbers}},
                projection={"_id": 1, "authNumber": 1},
            )
        )

        authorizationsFromDbSet = set[str](
            authorization["authNumber"] for authorization in authorizationsFromDb
        )

        filtered_chunk = df[
            ~df["AUTHORIZATION_NUMBER"].isin(list(authorizationsFromDbSet))
        ]

        print(f"📦📦📦 Total authorization numbers to process: {len(filtered_chunk)}")

        if len(filtered_chunk) == 0:
            print(f"===========📊 [END] No rows to process ===========")
            return

        self.write_sheet(filtered_chunk, file_name, sheet_name)

        print(
            f"===========📊 [END] Load data in {format_duration(time.perf_counter() - data_load_time)} ==========="
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
