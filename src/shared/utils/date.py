from datetime import datetime

import pandas as pd


def to_datetime(val, disable_convert=False):  # noqa: ANN001
    """Convert value to datetime; returns None if val is None."""
    if val is None:
        return None

    if disable_convert:
        if isinstance(val, datetime):
            return val
        return pd.to_datetime(val).to_pydatetime()

    if isinstance(val, datetime):
        return val.tz_localize("America/Chicago").tz_convert("UTC").to_pydatetime()

    return (
        pd.to_datetime(val)
        .tz_localize("America/Chicago")
        .tz_convert("UTC")
        .to_pydatetime()
    )


def to_utc_datetime(val):
    """Convert value to UTC datetime; returns None if val is None."""
    if val is None:
        return None
    return pd.to_datetime(val).tz_localize("UTC").to_pydatetime()


def format_duration(seconds: float) -> str:
    if seconds < 59:
        return "less than 1 min"
    minutes = seconds / 60
    return f"{minutes:.1f} min"  # or round(minutes, 1), or int(minutes) for whole mins


def timeStamp(date: datetime | None = None) -> int:
    return (
        int(date.timestamp() * 1000) if date else int(datetime.now().timestamp() * 1000)
    )


def to_formatted_date(date: datetime | None = None) -> str:
    return date.strftime("%m/%d/%Y") if date else None


def from_string_to_formatted_date(date: str | None = None) -> str:
    return to_formatted_date(to_datetime(date)) if date else None
