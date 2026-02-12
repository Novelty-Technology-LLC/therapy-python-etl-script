from src.shared.base.base_model import BaseModel
from src.shared.constant.collection_name import CollectionName


class ProviderClaimsModel(BaseModel):
    """Provider Claims Model"""

    def __init__(self, collection_name: CollectionName) -> None:
        super().__init__(collection_name)


provider_claims_model = ProviderClaimsModel(CollectionName.PROVIDER_CLAIMS)
