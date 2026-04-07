from src.shared.base.base_model import BaseModel
from src.shared.constant.collection_name import CollectionName


class ArdbDumpInvoiceBillingsModel(BaseModel):
    """ARDB Dump Invoice Billings Model"""

    def __init__(self, collection_name: CollectionName):
        super().__init__(collection_name)

    def _ensure_indexes(self):
        pass


ardbDumpInvoiceBillingsModel = ArdbDumpInvoiceBillingsModel(
    CollectionName.ARDB_DUMP_INVOICE_BILLINGS
)
