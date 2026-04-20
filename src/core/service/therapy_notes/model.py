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
                    [("references.receiptDetailRef.refId", ASCENDING)],
                    name="references_receipt_detail_ref_id",
                    background=True,
                ),
                IndexModel(
                    [("references.invoiceBillingDetailRef.refId", ASCENDING)],
                    name="references_invoice_billing_detail_ref_id",
                    background=True,
                ),
                IndexModel(
                    [("references.invoiceBillingRef.refId", ASCENDING)],
                    name="references_invoice_billing_ref_id",
                    background=True,
                ),
                IndexModel(
                    [("references.module", ASCENDING)],
                    name="references_module",
                    background=True,
                ),
            ]
        )


therapy_notes_model = TherapyNotesModel(CollectionName.PYTHON_TEST_THERAPY_NOTES)
