from pymongo import ASCENDING
from pymongo.operations import IndexModel
from src.shared.base.base_model import BaseModel
from src.shared.constant.collection_name import CollectionName


class EligibilityModel(BaseModel):
    """Eligibility Model"""

    def __init__(self, collection_name: CollectionName) -> None:
        super().__init__(collection_name)

    def _ensure_indexes(self) -> None:
        self._model.create_indexes(
            [
                IndexModel(
                    [("enrollee.referenceId", ASCENDING)],
                    name="idx_enrollee_reference_id",
                    background=True,
                ),
                IndexModel(
                    [("subscriber.identificationCode", ASCENDING)],
                    name="idx_subscriber_identification_code",
                    background=True,
                ),
                IndexModel(
                    [("product.referenceId", ASCENDING)],
                    name="idx_product_reference_id",
                    background=True,
                ),
                IndexModel(
                    [("patient.memberId", ASCENDING)],
                    name="idx_patient_member_id",
                    background=True,
                ),
                IndexModel(
                    [
                        ("patient.memberId", ASCENDING),
                        ("product.referenceId", ASCENDING),
                        ("subscriber.identificationCode", ASCENDING),
                        ("enrollee.referenceId", ASCENDING),
                    ],
                    name="idx_patient_member_id_product_reference_id_subscriber_identification_code_enrollee_reference_id",
                    background=True,
                ),
            ],
        )


eligibilityModel = EligibilityModel(collection_name=CollectionName.ELIGIBILITY)
