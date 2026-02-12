import pandas as pd

from src.shared.constant.constant import BATCH_SIZE


def get_total_batch(df: pd.DataFrame, batch_size: int = BATCH_SIZE):
    total_batches = (len(df) + batch_size - 1) // batch_size
    return total_batches
