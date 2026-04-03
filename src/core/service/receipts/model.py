from src.shared.base.base_model import BaseModel
from src.shared.constant.collection_name import CollectionName


class ReceiptsModel(BaseModel):
    """Receipts Model"""

    def __init__(self, collection_name: CollectionName):
        super().__init__(collection_name)

    def _ensure_indexes(self):
        pass


receiptsModel = ReceiptsModel(CollectionName.RECEIPTS)
