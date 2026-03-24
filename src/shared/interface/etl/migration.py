from datetime import date
from typing import TypedDict, List
from .sheet_name import SheetName


class PrioritySheetItem(TypedDict):
    """Priority sheet item"""

    sheetName: SheetName
    priority: float


priority_sheet_list: List[PrioritySheetItem] = [
    {
        "sheetName": SheetName.MARKET_CLAIM,
        "priority": 1,
    },
    {
        "sheetName": SheetName.INSURER,
        "priority": 2,
    },
    {
        "sheetName": SheetName.PRODUCTS,
        "priority": 3,
    },
    {
        "sheetName": SheetName.BILLABLE_PRODUCTS,
        "priority": 3.5,
    },
    {
        "sheetName": SheetName.AUTHORIZATION,
        "priority": 4,
    },
    {
        "sheetName": SheetName.ELIGIBILITY,
        "priority": 6,
    },
    {
        "sheetName": SheetName.PROVIDER_CLAIM,
        "priority": 7,
    },
    {
        "sheetName": SheetName.INVOICE_BILLING,
        "priority": 8,
    },
    {
        "sheetName": SheetName.INVOICE_BILLING_DETAIL,
        "priority": 9,
    },
    {
        "sheetName": SheetName.RECEIPT,
        "priority": 10,
    },
    {
        "sheetName": SheetName.RECEIPT_DETAIL,
        "priority": 11,
    },
    {
        "sheetName": SheetName.EXPECTED_AMOUNT,
        "priority": 12,
    },
    {
        "sheetName": SheetName.BILLING_FILE,
        "priority": 13,
    },
    {
        "sheetName": SheetName.THIRD_PARTY_ADMINISTRATOR,
        "priority": 14,
    },
]


class FileMetadata(TypedDict):
    ardb_file_processed_at: date
    ardb_file_name: str
    ardb_file_path: str
    original_file_name: str
    document_id: str
    file_extension: str
    file_type: str
