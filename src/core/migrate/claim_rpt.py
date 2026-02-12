from datetime import datetime
import time
from src.core.migrate.base_etl import BaseEtl
from src.core.service.dump_records.model import DumpRecordsModel
from src.shared.constant.collection_name import CollectionName
from src.shared.constant.constant import BATCH_SIZE
from src.shared.interface.migration import InputFileType
from src.shared.utils.date import format_duration
from src.shared.utils.path import get_input_files_path
from pathlib import Path
import pandas as pd
import numpy as np

from src.shared.utils.rpt import get_colspecs_from_rpt


class Claim_Rpt_Etl(BaseEtl):

    def __init__(self):
        super().__init__()

    def execute(self):

        all_files = get_input_files_path(
            input_file_path=Path("input-files/claim_rpt"), file_type=InputFileType.RPT
        )

        dump_records_model = DumpRecordsModel(
            collection_name=CollectionName.DUMP_PROVIDER_CLAIM
        )

        ardb_file_processed_at = datetime.now()
        ardb_file_path = "ETL_SCRIPTS"

        for file in all_files:
            start = time.perf_counter()
            print(f"========== [START] Processing file: {file.name} ==========")

            ardb_file_name = file.name

            chunk_iterator = pd.read_fwf(
                file,
                skiprows=[1],
                infer_nrows=1000,
                chunksize=BATCH_SIZE,
                colspecs=get_colspecs_from_rpt(file),
            )
            for i, chunk in enumerate(chunk_iterator):
                print(f"Processing {i + 1} chunk of {len(chunk)}")

                chunk["ardbSourceDocument"] = ardb_file_name
                chunk["ardbLastModifiedDate"] = ardb_file_processed_at

                dump_records_model.insert_many(
                    chunk.replace({np.nan: None}).astype(str).to_dict("records")
                )

            elapsed = time.perf_counter() - start
            print(
                f"========== [END] Processing file: {file.name} in {format_duration(elapsed)} =========="
            )
