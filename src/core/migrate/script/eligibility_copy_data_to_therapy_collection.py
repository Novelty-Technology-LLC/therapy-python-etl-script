import time
from typing import Dict, List, TypedDict

from pymongo import ASCENDING
from src.core.migrate.base_etl import BaseEtl
from src.core.service.eligibility.model import eligibilityModel
from src.core.service.enrollees.model import enrolleesModel
from src.core.service.patients.model import patientsModel
from src.core.service.subscribers.model import subscribersModel
from src.shared.base.base_model import BaseModel
from src.shared.constant.collection_name import CollectionName
from src.shared.constant.constant import BATCH_SIZE
from src.shared.utils.batch import get_total_batch_count
from src.shared.utils.date import format_duration


class ICollectionModel(TypedDict):
    pythonModel: BaseModel
    therapyModel: BaseModel
    name: str


class EligibilityCopyDataToTherapyCollectionPatch(BaseEtl):
    def __init__(self):
        super().__init__()
        self.batch_size = BATCH_SIZE
        self.collections: List[ICollectionModel] = [
            {
                "name": "Enrollee",
                "pythonModel": enrolleesModel,
                "therapyModel": BaseModel(CollectionName.THERAPY_ENROLLEE),
            },
            {
                "name": "Patient",
                "pythonModel": patientsModel,
                "therapyModel": BaseModel(CollectionName.THERAPY_PATIENT),
            },
            {
                "name": "Subscriber",
                "pythonModel": subscribersModel,
                "therapyModel": BaseModel(CollectionName.THERAPY_SUBSCRIBER),
            },
            {
                "name": "Eligibility",
                "pythonModel": eligibilityModel,
                "therapyModel": BaseModel(CollectionName.THERAPY_ELIGIBILITY),
            },
        ]
        self.flush_data = False

    def execute(self):
        etl_start_time = time.perf_counter()
        for modelCollections in self.collections:
            pythonModel = modelCollections.get("pythonModel")
            therapyModel = modelCollections.get("therapyModel")
            name = modelCollections.get("name")
            print(f"🛢 Collection Model: {modelCollections.get('name')}")

            if self.flush_data:
                print(f"🛠️ Flushing data from {name}")
                therapyModel.get_model().delete_many({})

            total_count = pythonModel.get_model().count_documents(filter={})
            total_batches = get_total_batch_count(total_count, self.batch_size)

            print(f"📦 Batch Size: {self.batch_size}")
            print(f"📋 Total {name}: {total_count}")
            print(f"📦 Total batches: {total_batches}")

            last_visited_batch_id = None

            for batch_num in range(total_batches):
                batch_start_time = time.perf_counter()
                query = (
                    {"_id": {"$gt": last_visited_batch_id}}
                    if last_visited_batch_id
                    else {}
                )

                data_from_python_model = list(
                    pythonModel.get_model().find(
                        filter=query,
                        limit=self.batch_size,
                        sort=[("_id", ASCENDING)],
                    )
                )

                if len(data_from_python_model) <= 0:
                    break

                last_visited_batch_id = data_from_python_model[-1]["_id"]

                collect_python_data_ids_set = set[str]()
                for data in data_from_python_model:
                    if data.get("_id"):
                        collect_python_data_ids_set.add(data.get("_id"))

                therapy_data_from_db = list(
                    therapyModel.get_model().find(
                        filter={"_id": {"$in": list(collect_python_data_ids_set)}},
                        projection={"_id": 1},
                    )
                )

                # O(1) access therapy data
                # therapy_data_by_id: Dict[str, dict] = {
                #     data["_id"]: data
                #     for data in therapy_data_from_db
                #     if data.get("_id")
                # }
                therapy_data_by_id_set = set[str]()
                for therapy_data in therapy_data_from_db:
                    if therapy_data.get("_id"):
                        therapy_data_by_id_set.add(therapy_data.get("_id"))

                collect_inserted_therapy_data = list[dict]()
                for data in data_from_python_model:
                    data_id = data.get("_id")
                    if data_id is None:
                        continue

                    if data_id not in therapy_data_by_id_set:
                        collect_inserted_therapy_data.append(data)

                if collect_inserted_therapy_data:
                    print(
                        f"🔄 Total {name} inserting for Batch {batch_num + 1}: {len(data_from_python_model)}"
                    )
                    therapyModel.get_model().insert_many(data_from_python_model)

                print(
                    f"✅ Batch {batch_num + 1} completed in {format_duration(time.perf_counter() - batch_start_time)}"
                )

        print(
            f"✅ ETL completed in {format_duration(time.perf_counter() - etl_start_time)}"
        )
