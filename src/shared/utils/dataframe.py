from typing import Generator
import pandas as pd

from src.shared.constant.constant import BATCH_SIZE


def batch_iterator(
    df: pd.DataFrame, batch_size: int = BATCH_SIZE
) -> Generator[pd.DataFrame, None, None]:
    """Yield DataFrame in batches."""
    for i in range(0, len(df), batch_size):
        yield df.iloc[i : i + batch_size].copy()
