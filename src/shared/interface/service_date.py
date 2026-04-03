from datetime import datetime
from typing import Optional, TypedDict


class IDateRange(TypedDict):
    startDate: Optional[datetime]
    endDate: Optional[datetime]


class IFormattedDateRange(TypedDict):
    formattedStartDate: Optional[str]
    formattedEndDate: Optional[str]


class IMonthYear(TypedDict, total=False):
    month: str
    year: str


class IServiceDate(IDateRange, IFormattedDateRange, IMonthYear):
    pass
