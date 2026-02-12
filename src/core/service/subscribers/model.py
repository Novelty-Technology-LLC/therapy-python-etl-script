from src.shared.base.base_model import BaseModel
from src.shared.constant.collection_name import CollectionName


class SubscribersModel(BaseModel):
    """Subscribers Model"""

    def __init__(self, collection_name: CollectionName) -> None:
        super().__init__(collection_name)


subscribersModel = SubscribersModel(collection_name=CollectionName.SUBSCRIBER)
