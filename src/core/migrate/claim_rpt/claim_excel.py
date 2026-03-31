import numpy as np
import pandas as pd


from pathlib import Path
import time
from typing import Optional

from pymongo.operations import UpdateOne
from src.config.config import Config
from src.core.migrate.base_etl import BaseEtl
from src.core.migrate.claim_rpt.data_type.claim_data_type import CLAIM_MIGRATE_COLS
from src.core.migrate.claim_rpt.data_type.provider_claim_data_type import (
    PROVIDER_CLAIM_DATA_FRAME_TYPE,
)
from src.core.migrate.claim_rpt.data_type.service_line_data_type import (
    SERVICE_LINE_MIGRATE_COLS_EXCEL,
)
from src.core.service.documents.model import documentsModel
from src.core.service.dump_records.model import DumpRecordsModel
from src.core.service.provider_claims.entity import (
    IProviderClaimServiceLine,
    ITherapyProviderClaim,
)
from src.core.service.provider_claims.mapper import provider_claim_mapper
from src.core.service.provider_claims.model import provider_claims_model
from src.shared.constant.collection_name import CollectionName
from src.shared.interface.document import DocumentStatusEnum
from src.shared.interface.etl.migration import FileMetadata
from src.shared.interface.migration import InputFileType
from src.shared.utils.batch import get_total_batch
from src.shared.utils.dataframe import batch_iterator
from src.shared.utils.date import format_duration
from src.shared.utils.migration import generate_uuid, verify_and_generate_document
from src.shared.utils.path import get_input_files_path


class Claim_Excel_Etl(BaseEtl):
    def __init__(self, input_file_path: Path):
        super().__init__()
        self.input_file_path = input_file_path
        self.dump_records_model = DumpRecordsModel(
            collection_name=CollectionName.DUMP_PROVIDER_CLAIM
        )
        self.support_duplicate_documents = Config.get_documents().get(
            "support_duplicate_documents"
        )
        self.enable_backup = True

    def execute(self):
        all_files = get_input_files_path(
            input_file_path=self.input_file_path, file_type=InputFileType.EXCEL
        )
        print(f"📁 Total files: {len(all_files)}")

        for file in all_files:
            documentId: Optional[str] = None
            try:
                print(f"===========📁 [START] Processing file: {file.name} ===========")

                start = time.perf_counter()
                document_response = verify_and_generate_document(
                    file,
                    self.support_duplicate_documents,
                    "ardb-backup/provider_claims",
                    InputFileType.EXCEL,
                    self.enable_backup,
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
                df = pd.read_excel(
                    file,
                    sheet_name="CLAIMS",
                    dtype=PROVIDER_CLAIM_DATA_FRAME_TYPE,
                )
                print("===========📊 [END] Load on data frame ===========")

                # Batch processing
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
                print(f"Error processing file: {file.name} - {e}")

    def load_provider_claim(self, chunk: pd.DataFrame, file_metadata: FileMetadata):

        print("=========== [START] Loading provider claim ===========")

        inserted_provider_claims: list[ITherapyProviderClaim] = []
        updated_provider_claims: list[ITherapyProviderClaim] = []

        claim_df = (
            chunk[CLAIM_MIGRATE_COLS]
            .drop_duplicates(subset=["CLAIM_ID", "DIAGNOSIS_CODE"], keep="first")
            .reset_index(drop=True)
        )

        claim_df.replace({np.nan: None}, inplace=True)

        service_line_df = (
            chunk[SERVICE_LINE_MIGRATE_COLS_EXCEL]
            .drop_duplicates(
                subset=[
                    "CODE",
                    "DATE_OF_SERVICE",
                    "MODIFIER1",
                    "MODIFIER2",
                    "MODIFIER3",
                    "MODIFIER4",
                    "DIAGNOSIS_CODE_INDEX1",
                    "DIAGNOSIS_CODE_INDEX2",
                    "DIAGNOSIS_CODE_INDEX3",
                    "DIAGNOSIS_CODE_INDEX4",
                    "DIAGNOSIS_CODE_INDEX5",
                    "DIAGNOSIS_CODE_INDEX6",
                    "DIAGNOSIS_CODE_INDEX7",
                    "DIAGNOSIS_CODE_INDEX8",
                ],
                keep="first",
            )
            .reset_index(drop=True)
        )
        service_line_df.replace({np.nan: None}, inplace=True)

        # provider claims from db
        query = {"CLAIM_ID": {"$in": claim_df["CLAIM_ID"].tolist()}}
        provider_claims_from_db = list[ITherapyProviderClaim](
            provider_claims_model.get_model().find(query)
        )

        for row in claim_df.to_dict(orient="records"):
            # index = getattr(row, "Index", None)
            ardb_claim_id = row.get("CLAIM_ID", None)

            provider_claim_from_db = None
            for provider_claim in provider_claims_from_db:
                if provider_claim["CLAIM_ID"] == ardb_claim_id:
                    provider_claim_from_db = provider_claim
                    break

            service_lines_from_ardb = (
                service_line_df[service_line_df["CLAIM_ID"] == ardb_claim_id]
            ).to_dict(orient="records")

            # insert
            if provider_claim_from_db is None:
                _id = generate_uuid()
                provider_claim_from_inserted_list: ITherapyProviderClaim | None = None
                provider_claim_inserted_index: int = -1

                # search in inserted provider claims
                for index, provider_claim in enumerate(inserted_provider_claims):
                    if provider_claim["CLAIM_ID"] == ardb_claim_id:
                        provider_claim_from_inserted_list = provider_claim
                        provider_claim_inserted_index = index
                        break

                # get service lines from inserted provider claims
                service_lines_from_inserted_list: list(IProviderClaimServiceLine) = (
                    []
                    if provider_claim_from_inserted_list is None
                    else provider_claim_from_inserted_list.get("SERVICE_LINES", [])
                )

                # only map claims information
                provider_claim_therapy_format = (
                    provider_claim_mapper.to_therapy_claim_format(
                        provider_claim=row,
                        _id=_id,
                        file_metadata=file_metadata,
                        old_provider_claim=provider_claim_from_inserted_list,
                    )
                )

                # map service lines
                mapped_service_lines_response = (
                    provider_claim_mapper.to_therapy_service_line_format(
                        provider_service_lines=service_lines_from_ardb,
                        old_provider_service_lines=service_lines_from_inserted_list,
                    )
                )

                provider_claim_therapy_format["SERVICE_LINES"] = (
                    mapped_service_lines_response
                )

                if provider_claim_from_inserted_list is None:
                    inserted_provider_claims.append(provider_claim_therapy_format)

                if provider_claim_from_inserted_list is not None:
                    inserted_provider_claims[provider_claim_inserted_index] = (
                        provider_claim_therapy_format
                    )

            # update
            if provider_claim_from_db is not None:
                _id = provider_claim_from_db.get("_id")
                provider_claim_from_updated_list: ITherapyProviderClaim | None = None
                provider_claim_updated_index: int = -1

                # search in inserted provider claims
                for index, provider_claim in enumerate(updated_provider_claims):
                    if provider_claim["CLAIM_ID"] == ardb_claim_id:
                        provider_claim_from_updated_list = provider_claim
                        provider_claim_updated_index = index
                        break

                # get service lines from updated provider claims
                service_lines_from_updated_list: list(IProviderClaimServiceLine) = (
                    []
                    if provider_claim_from_updated_list is None
                    else provider_claim_from_updated_list.get("SERVICE_LINES", [])
                )

                # only map claims information
                provider_claim_therapy_format = (
                    provider_claim_mapper.to_therapy_claim_format(
                        provider_claim=row,
                        _id=_id,
                        file_metadata=file_metadata,
                        old_provider_claim=(
                            provider_claim_from_updated_list
                            if provider_claim_from_updated_list is not None
                            else provider_claim_from_db
                        ),
                    )
                )

                # map service lines
                mapped_service_lines_response = (
                    provider_claim_mapper.to_therapy_service_line_format(
                        provider_service_lines=service_lines_from_ardb,
                        old_provider_service_lines=(
                            service_lines_from_updated_list
                            if provider_claim_from_updated_list is not None
                            else provider_claim_from_db.get("SERVICE_LINES", [])
                        ),
                    )
                )

                provider_claim_therapy_format["SERVICE_LINES"] = (
                    mapped_service_lines_response
                )

                if provider_claim_from_updated_list is None:
                    updated_provider_claims.append(provider_claim_therapy_format)

                if provider_claim_from_updated_list is not None:
                    updated_provider_claims[provider_claim_updated_index] = (
                        provider_claim_therapy_format
                    )

        # Db operations
        print(f"==========🛢 [START] [CLAIMS] Saving to ==========")
        if len(inserted_provider_claims) > 0:
            provider_claims_model.insert_many(inserted_provider_claims)

        if len(updated_provider_claims) > 0:
            provider_claims_model.get_model().bulk_write(
                [
                    UpdateOne(
                        {"_id": provider_claim["_id"]},
                        {
                            "$set": {
                                k: v
                                for k, v in provider_claim.items()
                                if k not in ("_id")
                            },
                            # "$push": {
                            #     "ardbDocuments": {
                            #         "$each": provider_claim["ardbDocuments"]
                            #     }
                            # },
                        },
                    )
                    for provider_claim in updated_provider_claims
                ]
            )

        print(f"==========🛢 [END] [CLAIMS] Saved to database successfully ==========")

        print("======== PROVIDER CLAIMS [DUMP RECORDS] ==========")
        chunk["ardbSourceDocument"] = file_metadata.get("ardb_file_name")
        chunk["ardbLastModifiedDate"] = file_metadata.get("ardb_file_processed_at")
        self.dump_records_model.insert_many(chunk.to_dict("records"))
