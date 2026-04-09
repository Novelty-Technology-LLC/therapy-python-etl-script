from typing import TypedDict


IInvoiceBillingDetailQuery = TypedDict(
    "IInvoiceBillingDetailQuery",
    {
        "invoiceBillingNumber": str,
        "assignedNumber": int,
        "procedureCode": str,
        "serviceDate.formattedStartDate": str,
    },
)
