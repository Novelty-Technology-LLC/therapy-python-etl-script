from pymongo import ASCENDING
from pymongo.operations import IndexModel
from src.shared.base.base_model import BaseModel
from src.shared.constant.collection_name import CollectionName


class TherapyNotesModel(BaseModel):
    """Therapy Notes Model"""

    def __init__(self, collection_name: CollectionName) -> None:
        super().__init__(collection_name)
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        self._model.create_indexes(
            [
                IndexModel(
                    [("references.receiptDetailRef._id", ASCENDING)],
                    name="idx_references_receipt_detail_ref_id",
                    background=True,
                ),
            ]
        )


therapy_notes_model = TherapyNotesModel(CollectionName.THERAPY_NOTES)
