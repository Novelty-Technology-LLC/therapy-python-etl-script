from pathlib import Path
import sys
import os
import re
import time

import numpy as np
import pandas as pd
from openpyxl import Workbook
from openpyxl.utils import get_column_letter

from src.core.migrate.base_etl import BaseEtl
from src.shared.interface.migration import InputFileType
from src.shared.utils.date import format_duration
from src.shared.utils.path import get_input_files_path


class ProviderClaimRptChangeToExcel(BaseEtl):
    def __init__(self, input_file_path: Path, output_file_path=None):
        super().__init__()
        self.input_file_path = input_file_path
        self.sheet_name = "CLAIMS"
        self.output_file_path = (
            "/Users/rajan/Desktop/personal-practice/etl/therapy-python-etl/input-files/output/claims/"
            if output_file_path is None
            else output_file_path
        )

    def execute(self):
        all_files = get_input_files_path(
            input_file_path=self.input_file_path, file_type=InputFileType.RPT
        )

        print(f"📄 Total files: {len(all_files)}")

        for file in all_files:
            print(f"📄 Reading file: {file.name}")
            start = time.perf_counter()
            df = self.read_rpt(file)
            print(f"🔢  Columns : {len(df.columns)}")
            print(f"📊  Rows    : {len(df)}")
            print(
                f"🔷  Numeric : {df.select_dtypes(include=[np.number]).shape[1]} columns auto-detected"
            )

            # print(f"💾  Writing : {out_path}")
            output_file_name = f"{file.stem}.xlsx"
            print(f"💾  Writing : {output_file_name}")
            self.write_excel(df, f"{self.output_file_path}{output_file_name}")
            print(
                f"✅  Done → {output_file_name} in {format_duration(time.perf_counter() - start)}"
            )

    # ─────────────────────────────────────────────────────────────────────────────
    # 1. Locate header / separator lines
    # ─────────────────────────────────────────────────────────────────────────────

    def find_structure(self, lines: list) -> tuple:
        """Return (header_line_index, separator_line_index)."""
        for i, line in enumerate(lines):
            if re.match(r"^[\s\-]+$", line.rstrip("\n")) and "-" in line:
                if i == 0:
                    raise ValueError("Separator on line 0 — no header line above it.")
                return i - 1, i
        raise ValueError("Could not find the dashes separator line in the .rpt file.")

    # ─────────────────────────────────────────────────────────────────────────────
    # 2. Column spans from the dashes row (numpy)
    # ─────────────────────────────────────────────────────────────────────────────

    def get_column_spans(self, separator_line: str) -> np.ndarray:
        """
        Returns (N, 2) numpy int32 array of [start, end] per column,
        derived from consecutive dash groups.
        The END position is used for DATA slicing only, not for header names.
        """
        spans = [(m.start(), m.end()) for m in re.finditer(r"-+", separator_line)]
        return np.array(spans, dtype=np.int32)

    # ─────────────────────────────────────────────────────────────────────────────
    # 3. Extract column NAMES — full word, not truncated to dash-group width
    #
    #    ROOT CAUSE of the truncation bug:
    #      Using header[span_start : span_end] cuts the name at the dash width.
    #      If "CLAIM_TYPE" (10 chars) sits over 9 dashes, the result is "CLAIM_TYP".
    #
    #    FIX:
    #      - Use span_start only as the search anchor.
    #      - Scan forward (up to the next span's start) to skip any leading spaces.
    #      - Then read the full word until the next space or end-of-line.
    #      This way the full column name is always captured regardless of dash width.
    # ─────────────────────────────────────────────────────────────────────────────

    def extract_column_names(self, header_line: str, spans: np.ndarray) -> list:
        names = []
        n = len(spans)
        for i in range(n):
            span_start = int(spans[i][0])
            # Search window ends at the start of the next span (or end of line)
            search_end = int(spans[i + 1][0]) if i + 1 < n else len(header_line)

            if span_start >= len(header_line):
                names.append("")
                continue

            # Skip leading spaces from span_start up to search_end
            scan = span_start
            while (
                scan < search_end
                and scan < len(header_line)
                and header_line[scan] == " "
            ):
                scan += 1

            if scan >= len(header_line) or scan >= search_end:
                names.append("")
                continue

            # Read full word (until next space or end-of-line)
            word_end = scan
            while word_end < len(header_line) and header_line[word_end] != " ":
                word_end += 1

            names.append(header_line[scan:word_end])

        return names

    # ─────────────────────────────────────────────────────────────────────────────
    # 4. Slice data lines into a 2-D numpy object array (strict span boundaries)
    #
    #    Data values use STRICT [start:end] boundaries — the dash width defines
    #    the exact field width so values from adjacent columns don't overlap.
    # ─────────────────────────────────────────────────────────────────────────────

    def lines_to_matrix(self, lines: list, spans: np.ndarray) -> np.ndarray:
        n_rows = len(lines)
        n_cols = len(spans)
        matrix = np.empty((n_rows, n_cols), dtype=object)
        for r, line in enumerate(lines):
            for c, (s, e) in enumerate(spans):
                matrix[r, c] = line[s:e].strip() if s < len(line) else ""
        return matrix

    # ─────────────────────────────────────────────────────────────────────────────
    # 5. Deduplicate column names
    # ─────────────────────────────────────────────────────────────────────────────

    def deduplicate_columns(self, names: list) -> list:
        seen, result = {}, []
        for name in names:
            key = name if name else "UNNAMED"
            seen[key] = seen.get(key, 0) + 1
            result.append(key if seen[key] == 1 else f"{key}_{seen[key]}")
        return result

    # ─────────────────────────────────────────────────────────────────────────────
    # 6. Convert any scalar to a safe Python value; missing → "NULL"
    # ─────────────────────────────────────────────────────────────────────────────

    def to_py(self, v) -> object:
        if v is None:
            return "NULL"
        if isinstance(v, str):
            return v if v.strip() else "NULL"
        if isinstance(v, (np.bool_,)):
            return bool(v)
        if isinstance(v, (np.integer,)):
            return int(v)
        if isinstance(v, (np.floating,)):
            return "NULL" if np.isnan(v) else float(v)
        if isinstance(v, float):
            return "NULL" if (v != v) else v  # NaN self-inequality check
        if isinstance(v, int):
            return v
        return str(v)

    # ─────────────────────────────────────────────────────────────────────────────
    # 7. Read .rpt → pandas DataFrame
    # ─────────────────────────────────────────────────────────────────────────────

    def read_rpt(self, filepath: str) -> pd.DataFrame:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            lines = [l.rstrip("\n") for l in f.readlines()]

        header_idx, sep_idx = self.find_structure(lines)
        spans = self.get_column_spans(lines[sep_idx])

        # Full-word names (not truncated to dash width) ← the key fix
        raw_names = self.extract_column_names(lines[header_idx], spans)
        columns = self.deduplicate_columns(raw_names)

        data_lines = [l for l in lines[sep_idx + 1 :] if l.strip()]
        if not data_lines:
            return pd.DataFrame(columns=columns)

        matrix = self.lines_to_matrix(data_lines, spans)

        n = len(columns)
        if matrix.shape[1] > n:
            matrix = matrix[:, :n]
        elif matrix.shape[1] < n:
            pad = np.full((matrix.shape[0], n - matrix.shape[1]), "", dtype=object)
            matrix = np.hstack([matrix, pad])

        df = pd.DataFrame(matrix, columns=columns)

        # Replace "NULL" strings with NaN (no inplace — avoids pandas 3.0 copy warning)
        df = df.replace("NULL", np.nan)

        # Auto-detect numeric columns.
        # pandas 3.0 infers StringDtype from object arrays, so assigning a numeric
        # Series back with df.loc[:, col] raises TypeError ("Invalid value for dtype 'str'").
        # Fix: build a fresh dict of Series and reconstruct the DataFrame.
        converted = {}
        for col in df.columns:
            try:
                converted[col] = pd.to_numeric(df[col], errors="raise")
            except (ValueError, TypeError):
                converted[col] = df[col]

        df = pd.DataFrame(converted)

        return df

    # ─────────────────────────────────────────────────────────────────────────────
    # 8. Write DataFrame → plain Excel, NaN/None → "NULL"
    # ─────────────────────────────────────────────────────────────────────────────

    def write_excel(self, df: pd.DataFrame, out_path: str) -> None:
        wb = Workbook()
        ws = wb.active
        ws.title = self.sheet_name

        # Header row (plain, no styling)
        ws.append(df.columns.tolist())

        # Data rows
        arr = df.to_numpy(dtype=object, na_value=None)
        for ri in range(arr.shape[0]):
            ws.append([self.to_py(arr[ri, ci]) for ci in range(arr.shape[1])])

        wb.save(out_path)
