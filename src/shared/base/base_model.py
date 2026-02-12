from pymongo.collection import Collection
from src.shared.base.model_interface import IModelInterface
from src.shared.constant.collection_name import CollectionName
from src.shared.helper.mongodb_helper import mongodb_helper


class BaseModel(IModelInterface):
    """Base Model"""

    _collection_name: CollectionName
    _model: Collection

    def __init__(self, collection_name: CollectionName) -> None:
        self._collection_name = collection_name
        self._model = mongodb_helper._get_database()[collection_name]

    def get_model(self) -> Collection:
        if self._model is None:
            self._model = mongodb_helper._get_database()[self._collection_name]

        return self._model

    def insert_many(self, records: list[dict]) -> None:
        """Insert many records into the model"""
        if self._model is None:
            self.get_model()

        self._model.insert_many(records)
