from typing import TypedDict

from src.shared.interface.etl.sheet_name import SheetName


class PrioritySheetItem(TypedDict):
    sheet_name: SheetName
    priority: float


priority_sheet: list[PrioritySheetItem] = [
    {"sheet_name": SheetName.MARKET_CLAIM, "priority": 1},
    {"sheet_name": SheetName.INSURER, "priority": 2},
    {"sheet_name": SheetName.PRODUCTS, "priority": 3},
    {"sheet_name": SheetName.BILLABLE_PRODUCTS, "priority": 3.5},
    {"sheet_name": SheetName.AUTHORIZATION, "priority": 4},
    {"sheet_name": SheetName.ENROLLEES, "priority": 5},
    {"sheet_name": SheetName.ELIGIBILITY, "priority": 6},
    {"sheet_name": SheetName.PROVIDER_CLAIM, "priority": 7},
    {"sheet_name": SheetName.INVOICE_BILLING, "priority": 8},
    {"sheet_name": SheetName.INVOICE_BILLING_DETAIL, "priority": 9},
    {"sheet_name": SheetName.RECEIPT, "priority": 10},
    {"sheet_name": SheetName.RECEIPT_DETAIL, "priority": 11},
    {"sheet_name": SheetName.EXPECTED_AMOUNT, "priority": 12},
    {"sheet_name": SheetName.BILLING_FILE, "priority": 13},
    {"sheet_name": SheetName.THIRD_PARTY_ADMINISTRATOR, "priority": 14},
    {"sheet_name": SheetName.PD_PROVIDER_SUPPLEMENTAL, "priority": 15},
    {"sheet_name": SheetName.PD_PROVIDERS, "priority": 16},
    {"sheet_name": SheetName.PD_LOCATIONS, "priority": 17},
    {"sheet_name": SheetName.PD_PAYEE_DETAILS, "priority": 18},
    {"sheet_name": SheetName.PD_PAYEES, "priority": 19},
    {"sheet_name": SheetName.PD_NETWORK_REIMBURSEMENT, "priority": 20},
    {"sheet_name": SheetName.PD_REIMBURSEMENT_SCHEDULE, "priority": 21},
    {"sheet_name": SheetName.PD_FEE_SCHEDULE, "priority": 22},
    {"sheet_name": SheetName.PD_PAYEE_EFT_DETAILS, "priority": 23},
]
