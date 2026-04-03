from src.shared.base.base_model import BaseModel
from src.shared.constant.collection_name import CollectionName


class InvoiceBillingDetailsModel(BaseModel):
    """Invoice Billing Details Model"""

    def __init__(self, collection_name: CollectionName):
        super().__init__(collection_name)

    def _ensure_indexes(self):
        pass


invoiceBillingDetailsModel = InvoiceBillingDetailsModel(
    CollectionName.INVOICE_BILLING_DETAILS
)
