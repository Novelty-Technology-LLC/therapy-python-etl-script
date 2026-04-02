from pymongo import ASCENDING
from pymongo.operations import IndexModel
from src.shared.base.base_model import BaseModel
from src.shared.constant.collection_name import CollectionName


class PatientsModel(BaseModel):
    """Patients Model"""

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
                    [("memberId", ASCENDING)],
                    name="idx_member_id",
                    background=True,
                ),
                IndexModel(
                    [
                        ("enrollee.referenceId", ASCENDING),
                        ("subscriber.identificationCode", ASCENDING),
                        ("memberId", ASCENDING),
                    ],
                    name="idx_patient_enrollee_subscriber_member_id",
                    background=True,
                ),
            ]
        )


patientsModel = PatientsModel(CollectionName.PATIENTS)
