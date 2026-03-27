from pymongo import ASCENDING
from pymongo.operations import IndexModel

from src.shared.base.base_model import BaseModel
from src.shared.constant.collection_name import CollectionName


class ProviderClaimsModel(BaseModel):
    """Provider Claims Model"""

    def __init__(self, collection_name: CollectionName) -> None:
        super().__init__(collection_name)
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        self._model.create_indexes(
            [
                IndexModel(
                    [("CLAIM_ID", ASCENDING)],
                    name="idx_claim_id",
                    background=True,
                )
            ],
        )


provider_claims_model = ProviderClaimsModel(CollectionName.PROVIDER_CLAIMS)
