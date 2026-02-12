from pymongo.collection import Collection


class IModelInterface:
    def get_model(self) -> Collection:
        pass

    def insert_many(self, records: list[dict]) -> None:
        pass
