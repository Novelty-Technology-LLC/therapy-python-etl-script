from datetime import datetime

import pandas as pd


def to_datetime(val):  # noqa: ANN001
    """Convert value to datetime; returns None if val is None."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    return pd.to_datetime(val).to_pydatetime()


def format_duration(seconds: float) -> str:
    if seconds < 59:
        return "less than 1 min"
    minutes = seconds / 60
    return f"{minutes:.1f} min"  # or round(minutes, 1), or int(minutes) for whole mins
