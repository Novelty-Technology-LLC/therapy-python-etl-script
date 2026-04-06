from datetime import datetime
from pathlib import Path
from src.core.migrate.base_etl import BaseEtl
from src.core.migrate.claim_rpt.data_type.provider_claim_data_type import (
    PROVIDER_CLAIM_DATA_FRAME_TYPE,
)

from src.shared.utils.date import format_duration, timeStamp
from src.shared.constant.constant import BATCH_SIZE
from src.shared.interface.migration import InputFileType
from src.shared.utils.path import get_input_files_path
import time

from typing import Any, List, Optional
import numpy as np
import pandas as pd

from src.shared.utils.rpt import get_colspecs_from_rpt


class ProviderClaimRptChangeToExcel(BaseEtl):
    def __init__(self, input_file_path: Path):
        super().__init__()
        self.batch_size = 1000
        self.input_file_path = input_file_path

    def execute(self):
        all_files = get_input_files_path(
            input_file_path=self.input_file_path, file_type=InputFileType.RPT
        )

        for file in all_files:
            print(f"=========== [START] Processing file: {file.name} ===========")
            start = time.perf_counter()

            print("=========== [START] Reading file ===========")
            file_read_start = time.perf_counter()
            df = pd.read_fwf(
                file,
                skiprows=[1],
                # infer_nrows=self.batch_size,
                chunksize=self.batch_size,
                colspecs=get_colspecs_from_rpt(file),
                # dtype=PROVIDER_CLAIM_DATA_FRAME_TYPE,
            )
            print(
                f"=========== [END] Reading file at {format_duration(time.perf_counter() - file_read_start)} ==========="
            )

            for i, chunk in enumerate(df):
                print(f"=========== [START] Processing chunk {i + 1} ===========")
                chunk_process_start = time.perf_counter()
                file_name = f"{timeStamp(datetime.now())}_provider_claim_{i+1}.xlsx"
                chunk = chunk.fillna("NULL")

                chunk.to_excel(
                    Path(
                        f"/Users/rajan/Desktop/personal-practice/etl/therapy-python-etl/input-files/output/claims/{file_name}"
                    ),
                    index=False,
                )
                print(
                    f"=========== [END] Processing chunk {i + 1} at {format_duration(time.perf_counter() - chunk_process_start)} ==========="
                )

            print(
                f"=========== [END] Processing file: {file.name} at {format_duration(time.perf_counter() - start)} ==========="
            )
