from pymongo import ASCENDING
from pymongo.operations import IndexModel
from src.shared.base.base_model import BaseModel
from src.shared.constant.collection_name import CollectionName


class EnrolleesModel(BaseModel):
    """Enrollees Model"""

    def __init__(self, collection_name: CollectionName) -> None:
        super().__init__(collection_name)
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        self._model.create_indexes(
            [
                IndexModel(
                    [("referenceId", ASCENDING)],
                    name="idx_enrollee_reference_id",
                    background=True,
                    unique=True,
                ),
            ],
        )


enrolleesModel = EnrolleesModel(collection_name=CollectionName.ENROLLEE)
