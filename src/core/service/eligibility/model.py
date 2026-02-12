from src.shared.base.base_model import BaseModel
from src.shared.constant.collection_name import CollectionName


class EligibilityModel(BaseModel):
    """Eligibility Model"""

    def __init__(self, collection_name: CollectionName) -> None:
        super().__init__(collection_name)


eligibilityModel = EligibilityModel(collection_name=CollectionName.ELIGIBILITY)
