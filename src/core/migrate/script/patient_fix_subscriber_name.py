from pymongo import ASCENDING
from pymongo.operations import UpdateOne
from src.core.migrate.base_etl import BaseEtl
from src.core.service.patients.entity import ITherapyPatient
from src.core.service.patients.model import patientsModel
from src.core.service.subscribers.entity import ITherapySubscriber
from src.shared.constant.constant import BATCH_SIZE
from src.shared.utils.batch import get_total_batch_count
from src.core.service.subscribers.model import subscribersModel
from src.shared.utils.name import get_name
from src.shared.utils.obj import get_obj_value


class PatientFixSubscriberName(BaseEtl):
    def __init__(self):
        super().__init__()
        self.batch_size = BATCH_SIZE

    def execute(self):
        total_count = patientsModel.get_model().count_documents(filter={})
        total_batches = get_total_batch_count(total_count, self.batch_size)
        print(f"Total batches: {total_batches}")

        last_visited_batch_id = None

        for batch_num in range(total_batches):
            print(f"Processing batch {batch_num + 1} of {total_batches}")

            query = (
                {"_id": {"$gt": last_visited_batch_id}} if last_visited_batch_id else {}
            )

            patients = list[ITherapyPatient](
                patientsModel.get_model().find(
                    filter=query,
                    limit=self.batch_size,
                    sort=[("_id", ASCENDING)],
                    projection={"_id": 1, "subscriber": 1},
                )
            )

            if len(patients) <= 0:
                break

            last_visited_batch_id = patients[-1]["_id"]

            subscriber_ids = set[str]()
            for patient in patients:
                ref_id = get_obj_value(patient, "subscriber", "refId")
                if ref_id:
                    subscriber_ids.add(ref_id)

            if len(subscriber_ids) <= 0:
                continue

            subscribers = list[ITherapySubscriber](
                subscribersModel.get_model().find(
                    filter={"_id": {"$in": list(subscriber_ids)}},
                    projection={
                        "demographic": {"firstName": 1, "middleName": 1, "lastName": 1}
                    },
                )
            )

            if len(subscribers) <= 0:
                continue

            subscriber_map = {sub["_id"]: sub for sub in subscribers}

            update_ops = []

            for patient in patients:
                ref_id = get_obj_value(patient, "subscriber", "refId")
                if not ref_id:
                    continue

                subscriber = subscriber_map.get(ref_id)
                if not subscriber:
                    continue

                name = get_name(
                    {
                        "firstName": get_obj_value(
                            subscriber, "demographic", "firstName"
                        ),
                        "middleName": get_obj_value(
                            subscriber, "demographic", "middleName"
                        ),
                        "lastName": get_obj_value(
                            subscriber, "demographic", "lastName"
                        ),
                    }
                )

                if len(name) <= 0:
                    continue

                update_ops.append(
                    UpdateOne(
                        {"_id": patient["_id"]},
                        {"$set": {"subscriber.name": name}},
                    )
                )

            if len(update_ops) > 0:
                patientsModel.get_model().bulk_write(update_ops)

            print(
                f"Updated {len(update_ops)} patients in batch {batch_num + 1} of {total_batches}"
            )
