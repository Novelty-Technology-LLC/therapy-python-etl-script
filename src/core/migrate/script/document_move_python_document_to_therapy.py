from pymongo import ASCENDING
from src.core.migrate.base_etl import BaseEtl
from src.core.service.documents.model import documentsModel
from src.shared.base.base_model import BaseModel
from src.shared.constant.collection_name import CollectionName
from src.shared.constant.constant import BATCH_SIZE
from src.shared.interface.document import DocumentStatusEnum
from src.shared.utils.batch import get_total_batch_count
from src.shared.utils.date import format_duration
import time


class DocumentMovePythonDocumentToTherapy(BaseEtl):
    def __init__(self):
        super().__init__()
        self.batch_size = BATCH_SIZE
        self.therapy_documents_model = BaseModel(CollectionName.THERAPY_DOCUMENTS)

    def execute(self):
        etl_start_time = time.perf_counter()
        base_query = {
            "$or": [
                {
                    "originalName": {"$regex": ".*Eligibility.*", "$options": "i"},
                },
                {"metadata.etlType": "ELIGIBILITY"},
            ],
            "status": DocumentStatusEnum.COMPLETED,
        }
        total_count = documentsModel.get_model().count_documents(filter=base_query)

        total_batches = get_total_batch_count(total_count, self.batch_size)

        print(f"📦 Total batches: {total_batches}")
        print(f"📋 Total documents: {total_count}")
        print(f"📦 Batch Size: {self.batch_size}")

        last_visited_batch_id = None

        for batch_num in range(total_batches):
            print(f"⏳ Processing batch {batch_num + 1} of {total_batches}")
            batch_start_time = time.perf_counter()
            query = (
                {"_id": {"$gt": last_visited_batch_id}} if last_visited_batch_id else {}
            )

            documentsFromDb = list(
                documentsModel.get_model().find(
                    filter={**query, **base_query},
                    limit=self.batch_size,
                    sort=[("_id", ASCENDING)],
                )
            )

            if len(documentsFromDb) <= 0:
                break

            last_visited_batch_id = documentsFromDb[-1]["_id"]

            # therapy_documents_from_db = list
            documents_id_set = set[str]()
            for document in documentsFromDb:
                document_id = document.get("_id")
                if document_id:
                    documents_id_set.add(document_id)

            therapy_documents_from_db = list(
                self.therapy_documents_model.get_model().find(
                    filter={"_id": {"$in": list(documents_id_set)}},
                    projection={"_id": 1},
                )
            )

            collect_inserted_therapy_documents = list[dict]()

            for document in documentsFromDb:
                document_id = document.get("_id")

                therapy_document = next(
                    (
                        therapy_doc
                        for therapy_doc in therapy_documents_from_db
                        if therapy_doc.get("_id") == document_id
                    ),
                    None,
                )

                if therapy_document is None:
                    collect_inserted_therapy_documents.append(document)

            if collect_inserted_therapy_documents:
                self.therapy_documents_model.get_model().insert_many(
                    collect_inserted_therapy_documents
                )

            print(
                f"✅ Batch {batch_num + 1} completed in {format_duration(time.perf_counter() - batch_start_time)}"
            )

        print(
            f"✅ ETL completed in {format_duration(time.perf_counter() - etl_start_time)}"
        )
