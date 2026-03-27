from pymongo import ASCENDING
from pymongo.operations import IndexModel
from src.shared.base.base_model import BaseModel
from src.shared.constant.collection_name import CollectionName


class SubscribersModel(BaseModel):
    """Subscribers Model"""

    def __init__(self, collection_name: CollectionName) -> None:
        super().__init__(collection_name)
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        self._model.create_indexes(
            [
                IndexModel(
                    [("subscriberNumber", ASCENDING)],
                    name="idx_subscriber_number",
                    background=True,
                ),
                IndexModel(
                    [("enrollee.referenceId", ASCENDING)],
                    name="idx_enrollee_reference_id",
                    background=True,
                ),
                IndexModel(
                    [
                        ("enrollee.referenceId", ASCENDING),
                        ("subscriberNumber", ASCENDING),
                    ],
                    name="idx_enrollee_reference_id_subscriber_number",
                    background=True,
                ),
            ]
        )


subscribersModel = SubscribersModel(collection_name=CollectionName.SUBSCRIBER)
