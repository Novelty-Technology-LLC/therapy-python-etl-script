from typing import TypedDict


class IReceiptDetailArdbPayload(TypedDict):
    invoiceBillingNumber: str
    itemNumber: str
