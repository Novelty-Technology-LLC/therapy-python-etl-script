from typing import Any, Dict, List, TypedDict
from pymongo.collection import Collection

from pymongo import ASCENDING, MongoClient
from pymongo.database import Database
from src.config.config import Config
from src.core.migrate.base_etl import BaseEtl

from src.core.service.documents.model import documentsModel
from src.core.service.eligibility.model import eligibilityModel
from src.core.service.enrollees.model import enrolleesModel
from src.core.service.patients.model import patientsModel
from src.core.service.subscribers.model import subscribersModel
from src.shared.base.base_model import BaseModel
from src.shared.constant.collection_name import CollectionName
from src.shared.constant.constant import BATCH_SIZE
import time

from src.shared.utils.batch import get_total_batch_count
from src.shared.utils.date import format_duration


class ICollectionModel(TypedDict):
    sourceModel: BaseModel
    destinationModel: Collection
    name: str
    baseQuery: Dict[str, Any]


class MoveToDestinationDb(BaseEtl):
    """Move to destination db"""

    def __init__(self):
        super().__init__()
        self.batch_size = BATCH_SIZE

        self.destination_database = MongoClient(
            Config.resolve_destination_uri(),
            serverSelectionTimeoutMS=5000,  # 5 second timeout
            connectTimeoutMS=10000,  # 10 second connection timeout
        )[Config.get_destination_db().get("database")]

        print(
            f"Connected to {Config.get_destination_db().get("database")} successfully"
        )

        self.collections: List[ICollectionModel] = [
            # {
            #     "name": "Enrollee",
            #     "sourceModel": enrolleesModel,
            #     "destinationModel": self.destination_database[CollectionName.ENROLLEE],
            #     "baseQuery": {},
            # },
            # {
            #     "name": "Patient",
            #     "sourceModel": patientsModel,
            #     "destinationModel": self.destination_database[CollectionName.PATIENTS],
            #     "baseQuery": {},
            # },
            # {
            #     "name": "Subscriber",
            #     "sourceModel": subscribersModel,
            #     "destinationModel": self.destination_database[
            #         CollectionName.SUBSCRIBER
            #     ],
            #     "baseQuery": {},
            # },
            # {
            #     "name": "Eligibility",
            #     "sourceModel": eligibilityModel,
            #     "destinationModel": self.destination_database[
            #         CollectionName.ELIGIBILITY
            #     ],
            #     "baseQuery": {},
            # },
            # {
            #     "name": "Enrollee Dump",
            #     "sourceModel": BaseModel(CollectionName.DUMP_ENROLLEES),
            #     "destinationModel": self.destination_database[
            #         CollectionName.DUMP_ENROLLEES
            #     ],
            #     "baseQuery": {},
            # },
            # {
            #     "name": "Eligibility Dump",
            #     "sourceModel": BaseModel(CollectionName.DUMP_ELIGIBILITY),
            #     "destinationModel": self.destination_database[
            #         CollectionName.DUMP_ELIGIBILITY
            #     ],
            #     "baseQuery": {},
            # },
            # {
            #     "name": "ARDB Billing",
            #     "sourceModel": BaseModel(CollectionName.ARDB_DUMP_INVOICE_BILLINGS),
            #     "destinationModel": self.destination_database[
            #         CollectionName.ARDB_DUMP_INVOICE_BILLINGS
            #     ],
            #     "baseQuery": {},
            # },
            # {
            #     "name": "ARDB Billing Detail",
            #     "sourceModel": BaseModel(
            #         CollectionName.ARDB_DUMP_INVOICE_BILLING_DETAILS
            #     ),
            #     "destinationModel": self.destination_database[
            #         CollectionName.ARDB_DUMP_INVOICE_BILLING_DETAILS
            #     ],
            #     "baseQuery": {},
            # },
            # {
            #     "name": "ARDB Receipts",
            #     "sourceModel": BaseModel(CollectionName.ARDB_DUMP_RECEIPTS),
            #     "destinationModel": self.destination_database[
            #         CollectionName.ARDB_DUMP_RECEIPTS
            #     ],
            #     "baseQuery": {},
            # },
            # {
            #     "name": "ARDB Receipts Detail",
            #     "sourceModel": BaseModel(CollectionName.ARDB_DUMP_RECEIPT_DETAILS),
            #     "destinationModel": self.destination_database[
            #         CollectionName.ARDB_DUMP_RECEIPT_DETAILS
            #     ],
            #     "baseQuery": {},
            # },
            # {
            #     "name": "ARDB Authorizations",
            #     "sourceModel": BaseModel(CollectionName.ARDB_DUMP_AUTHORIZATIONS),
            #     "destinationModel": self.destination_database[
            #         CollectionName.ARDB_DUMP_AUTHORIZATIONS
            #     ],
            #     "baseQuery": {},
            # },
            {
                "name": "Python Test Documents",
                "sourceModel": documentsModel,
                "destinationModel": self.destination_database[CollectionName.DOCUMENTS],
                "baseQuery": {},
            },
        ]

    def execute(self):
        etl_start_time = time.perf_counter()

        for modelCollections in self.collections:
            modelTime = time.perf_counter()
            sourceModel = modelCollections.get("sourceModel")
            destinationModel = modelCollections.get("destinationModel")
            name = modelCollections.get("name")
            print(f"🛢 Collection Model: {modelCollections.get('name')}")

            total_count = sourceModel.get_model().count_documents(filter={})
            total_batches = get_total_batch_count(total_count, self.batch_size)
            print(f"📦 Batch Size: {self.batch_size}")
            print(f"📋 Total {name}: {total_count}")
            print(f"📦 Total batches: {total_batches}")

            last_visited_batch_id = None

            for batch_num in range(total_batches):
                batch_start_time = time.perf_counter()
                query = {
                    **(
                        {"_id": {"$gt": last_visited_batch_id}}
                        if last_visited_batch_id
                        else {}
                    ),
                    **modelCollections.get("baseQuery"),
                }

                source_data = list(
                    sourceModel.get_model().find(
                        filter=query,
                        limit=self.batch_size,
                        sort=[("_id", ASCENDING)],
                    )
                )

                if not source_data:
                    break

                last_visited_batch_id = source_data[-1]["_id"]

                destinationModel.insert_many(source_data)

                print(
                    f"✅ Batch {batch_num + 1} completed in {format_duration(time.perf_counter() - batch_start_time)}"
                )

            print(
                f"✅ {name} completed in {format_duration(time.perf_counter() - modelTime)}"
            )
        print(
            f"✅ Move to destination db completed in {format_duration(time.perf_counter() - etl_start_time)}"
        )
