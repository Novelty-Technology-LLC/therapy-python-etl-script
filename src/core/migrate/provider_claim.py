from typing import Optional
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import time
from pymongo.operations import UpdateMany, UpdateOne

from src.core.migrate.base_etl import BaseEtl
from src.core.service.documents.model import documentsModel
from src.core.service.provider_claims.entity import ITherapyProviderClaim
from src.core.service.provider_claims.model import provider_claims_model
from src.core.data_frame_type.provider_claim import (
    PROVIDER_CLAIM_DATA_FRAME_TYPE,
    SELECTED_PROVIDER_CLAIM_COLS,
)
from src.shared.interface.document import DocumentStatusEnum
from src.shared.interface.etl.migration import FileMetadata
from src.shared.interface.migration import InputFileType
from src.shared.utils.batch import get_total_batch
from src.shared.utils.dataframe import batch_iterator
from src.shared.utils.date import format_duration
from src.shared.utils.migration import (
    generate_file_metadata,
    verify_and_generate_document,
)
from src.shared.utils.path import get_input_files_path


class Provider_Claim_Etl(BaseEtl):

    def __init__(self, input_file_path: Path):
        super().__init__()
        self.input_file_path = input_file_path

    def load_provider_claim(self, df: pd.DataFrame, file_metadata: FileMetadata):
        print("=========== [START] Loading provider claim ===========")

        provider_claim_df = df[SELECTED_PROVIDER_CLAIM_COLS].copy()

        # fetch provider claim from db
        query = {
            "CLAIM_ID": {"$in": provider_claim_df["CLAIM_ID"].tolist()},
            # "ardbSourceDocument": { "$exists": True }
        }

        # fetch data
        provider_claims_from_db = list[ITherapyProviderClaim](
            provider_claims_model.get_model().find(
                query,
                projection={"CLAIM_ID": 1, "SUBSCRIBER_INFO.INSURED_ENROLLEE_ID": 1},
            )
        )

        updated_provider_claims = []

        for provider_claim_from_db in provider_claims_from_db:
            provider_claim_from_ardb_df = provider_claim_df[
                provider_claim_df["CLAIM_ID"] == provider_claim_from_db["CLAIM_ID"]
            ]

            if provider_claim_from_ardb_df.empty:
                continue

            if (
                provider_claim_from_db["SUBSCRIBER_INFO"]["INSURED_ENROLLEE_ID"]
                != provider_claim_from_ardb_df["INSURED_ENROLLEE_ID"].values[0]
            ):
                updated_provider_claims.append(
                    UpdateMany(
                        {"CLAIM_ID": provider_claim_from_db["CLAIM_ID"]},
                        {
                            "$set": {
                                "SUBSCRIBER_INFO.INSURED_ENROLLEE_ID": provider_claim_from_ardb_df[
                                    "INSURED_ENROLLEE_ID"
                                ].values[
                                    0
                                ],
                                "ardbSourceDocument": file_metadata["ardb_file_name"],
                                "ardbLastModifiedDate": file_metadata[
                                    "ardb_file_processed_at"
                                ],
                            },
                            "$push": {
                                "ardbDocuments": generate_file_metadata(file_metadata)
                            },
                        },
                    )
                )

        # 4. DB operations
        if len(updated_provider_claims) > 0:
            provider_claims_model.get_model().bulk_write(updated_provider_claims)

        print("=========== [END] Loading provider claim ===========")

    def execute(self):

        documentId: Optional[str] = None
        all_files = get_input_files_path(
            input_file_path=self.input_file_path,
            file_type=InputFileType.RPT,
        )

        for file in all_files:
            try:
                start = time.perf_counter()

                document_response = verify_and_generate_document(
                    file,
                    self.support_duplicate_documents,
                    "provider_claim",
                    InputFileType.RPT,
                    False,
                )

                if document_response is None:
                    continue

                documentId = document_response.get("documentId")
                file_metadata = document_response.get("file_metadata")

                print(f"========== [START] Processing file: {file.name} ==========")
                if documentId:
                    documentsModel.get_model().update_one(
                        {"_id": documentId},
                        {"$set": {"status": DocumentStatusEnum.PROCESSING}},
                    )

                df = pd.read_excel(
                    file, sheet_name="CLAIMS", dtype=PROVIDER_CLAIM_DATA_FRAME_TYPE
                )

                df = df.drop_duplicates(subset=["CLAIM_ID"], keep="first").reset_index(
                    drop=True
                )
                df.replace({np.nan: None}, inplace=True)

                total_batches = get_total_batch(df)
                print(f"Total batches: {total_batches}")

                for batch_num, chunk in enumerate(batch_iterator(df)):
                    print(f"Processing batch {batch_num + 1} of {total_batches}")

                    self.load_provider_claim(chunk, file_metadata)

                elapsed = time.perf_counter() - start
                if documentId:
                    documentsModel.get_model().update_one(
                        {"_id": documentId},
                        {"$set": {"status": DocumentStatusEnum.COMPLETED}},
                    )
                print(
                    f"========== [END] Processing file: {file.name} in {format_duration(elapsed)} =========="
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
