import pandas as pd

from src.shared.constant.constant import BATCH_SIZE


def get_total_batch(df: pd.DataFrame, batch_size: int = BATCH_SIZE):
    total_batches = (len(df) + batch_size - 1) // batch_size
    return total_batches


def get_total_batch_count(total_count: int, batch_size: int = BATCH_SIZE):
    total_batches = (total_count + batch_size - 1) // batch_size
    return total_batches
