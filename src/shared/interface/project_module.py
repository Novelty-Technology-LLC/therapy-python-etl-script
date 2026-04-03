from enum import StrEnum


class ProjectModule(StrEnum):
    """Project Module"""

    INVOICE_BILLING = "Invoice Billing"
    INVOICE_BILLING_DETAIL = "Invoice Billing Detail"
    RECEIPT = "Receipt"
    INVOICE_PAYMENT_RECEIPT = "Invoice Payment Receipt"
    RECEIPT_DETAIL = "Receipt Detail"
    TASK = "Task"
