import time

from pymongo import ASCENDING
from src.core.migrate.base_etl import BaseEtl
from src.core.service.therapy_notes.model import therapy_notes_model
from src.shared.base.base_model import BaseModel
from src.shared.constant.collection_name import CollectionName
from src.shared.constant.constant import BATCH_SIZE
from src.shared.utils.batch import get_total_batch_count
from src.shared.utils.date import format_duration


class TherapyNoteMoveFromPythonCollectionPatch(BaseEtl):
    def __init__(self):
        super().__init__()
        self.batch_size = BATCH_SIZE
        self.therapy_notes_model = BaseModel(CollectionName.THERAPY_NOTES)
        self.last_visited_batch_id = None

    def execute(self):
        print("🔄 Starting Therapy Note Move from Python Collection Patch")
        etl_start_time = time.perf_counter()
        base_query = [
            {
                "$lookup": {
                    "from": "therapyNotes",
                    "localField": "_id",
                    "foreignField": "_id",
                    "as": "therapy_notes",
                }
            },
            {
                "$match": {"$expr": {"$eq": [{"$size": "$therapy_notes"}, 0]}},
            },
        ]
        total_count = list(
            therapy_notes_model.get_model().aggregate(
                [*base_query, {"$count": "count"}]
            )
        )
        total_count = total_count[0].get("count") if total_count else 0
        total_batches = get_total_batch_count(total_count, self.batch_size)

        print(f"📦 Batch Size: {self.batch_size}")
        print(f"📋 Total Therapy Notes: {total_count}")
        print(f"📦 Total batches: {total_batches}")

        for batch_num in range(total_batches):
            batch_start_time = time.perf_counter()

            query = [
                *(
                    []
                    if self.last_visited_batch_id is None
                    else [{"$match": {"_id": {"$gt": self.last_visited_batch_id}}}]
                ),
                *(base_query),
                {"$sort": {"_id": ASCENDING}},
                {"$limit": self.batch_size},
            ]

            therapy_notes_from_db = list(
                therapy_notes_model.get_model().aggregate(query)
            )
            if len(therapy_notes_from_db) <= 0:
                break

            self.last_visited_batch_id = therapy_notes_from_db[-1]["_id"]

            collect_inserted_therapy_data = list[dict]()
            for therapy_note in therapy_notes_from_db:
                collect_inserted_therapy_data.append(therapy_note)

            if collect_inserted_therapy_data:
                self.therapy_notes_model.get_model().insert_many(
                    collect_inserted_therapy_data
                )

            print(
                f"✅ Batch {batch_num + 1} completed in {format_duration(time.perf_counter() - batch_start_time)}"
            )

        print(
            f"✅ ETL completed in {format_duration(time.perf_counter() - etl_start_time)}"
        )
