"""
Backfill / normalize formatted date fields to MM/DD/YYYY (e.g. 05/25/1976) on
enrollee, patient, subscriber, and eligibility documents.

Field mapping (see service mappers under src/core/service/*/mapper.py):
  enrollees:   demographic.formattedDob <- demographic.dob
               additionalInformation.formattedDeathDate <- additionalInformation.deathDate
  patients:    demographic.formattedDob <- demographic.dob
  subscribers: demographic.formattedDob, employment.formattedStartDate,
               employment.formattedTerminationDate
  eligibility: patient.formattedDob, serviceDate.formattedStartDate/EndDate,
               additionalInformation.paidThrough.formattedDate,
               additionalInformation.terminationReason.formattedEventDate

Collections:
  therapy: enrollees, patients, subscribers, eligibilities (CollectionName.THERAPY_*)
  test:    PYTHON_TEST_ENROLLEE, PYTHON_TEST_PATIENT, PYTHON_TEST_SUBSCRIBER,
           PYTHON_TEST_ELIGIBILITY (CollectionName.*)

Run:
  python main.py --execute UPDATE_FORMATTED_DATES
  python -m src.core.migrate.script.update_formatted_dates_patch --mode therapy
  python -m src.core.migrate.script.update_formatted_dates_patch --mode test --dry-run
"""

import time
from typing import Any, List

from pymongo import ASCENDING, UpdateOne
from src.core.migrate.base_etl import BaseEtl
from src.core.migrate.script.eligibility_copy_data_to_therapy_collection import (
    ICollectionModel,
)
from src.core.service.eligibility.model import eligibilityModel
from src.core.service.enrollees.entity import ITherapyEnrollee
from src.core.service.enrollees.model import enrolleesModel
from src.core.service.patients.model import patientsModel
from src.core.service.subscribers.model import subscribersModel
from src.shared.base.base_model import BaseModel
from src.shared.constant.collection_name import CollectionName
from src.shared.constant.constant import BATCH_SIZE
from src.shared.utils.batch import get_total_batch_count
from src.shared.utils.date import format_duration, from_string_to_formatted_date
from src.shared.utils.obj import get_obj_value


class IFormattedDatesModel(ICollectionModel):
    base_query: Any


class EligibilityUpdateFormattedDatesPatch(BaseEtl):
    def __init__(self):
        super().__init__()
        self.batch_size = BATCH_SIZE
        self.collections: List[IFormattedDatesModel] = [
            {
                "name": "Enrollee",
                "pythonModel": enrolleesModel,
                "therapyModel": BaseModel(CollectionName.THERAPY_ENROLLEE),
                "base_query": {
                    "$or": [
                        {"demographic.dob": {"$exists": True, "$ne": None}},
                        {
                            "additionalInformation.deathDate": {
                                "$exists": True,
                                "$ne": None,
                            }
                        },
                    ]
                },
            },
            {
                "name": "Patient",
                "pythonModel": patientsModel,
                "therapyModel": BaseModel(CollectionName.THERAPY_PATIENT),
                "base_query": {"demographic.dob": {"$exists": True, "$ne": None}},
            },
            {
                "name": "Subscriber",
                "pythonModel": subscribersModel,
                "therapyModel": BaseModel(CollectionName.THERAPY_SUBSCRIBER),
                "base_query": {
                    "$or": [
                        {"demographic.dob": {"$exists": True, "$ne": None}},
                        {"employment.startDate": {"$exists": True, "$ne": None}},
                        {"employment.endDate": {"$exists": True, "$ne": None}},
                    ]
                },
            },
            {
                "name": "Eligibility",
                "pythonModel": eligibilityModel,
                "therapyModel": BaseModel(CollectionName.THERAPY_ELIGIBILITY),
                "base_query": {
                    "$or": [
                        {"patient.dob": {"$exists": True, "$ne": None}},
                        {"serviceDate.startDate": {"$exists": True, "$ne": None}},
                        {"serviceDate.endDate": {"$exists": True, "$ne": None}},
                        {
                            "additionalInformation.paidThrough.date": {
                                "$exists": True,
                                "$ne": None,
                            }
                        },
                        {
                            "additionalInformation.terminationReason.eventDate": {
                                "$exists": True,
                                "$ne": None,
                            }
                        },
                    ]
                },
            },
        ]

    def execute(self):
        etl_start_time = time.perf_counter()

        for modelCollections in self.collections:
            pythonModel = modelCollections.get("pythonModel")
            name = modelCollections.get("name")
            print(f"🛢 Collection Model: {modelCollections.get('name')}")

            total_count = pythonModel.get_model().count_documents(
                filter=modelCollections.get("base_query")
            )
            total_batches = get_total_batch_count(total_count, self.batch_size)

            print(f"📦 Batch Size: {self.batch_size}")
            print(f"📋 Total {name}: {total_count}")
            print(f"📦 Total batches: {total_batches}")

            match name:
                case "Enrollee":
                    self._process_enrollee(modelCollections, total_batches)
                case "Patient":
                    self._process_patient(modelCollections, total_batches)
                case "Subscriber":
                    self._process_subscriber(modelCollections, total_batches)
                case "Eligibility":
                    self._process_eligibility(modelCollections, total_batches)

                case _:
                    print(f"🚨 Invalid name: {name}")

        print(
            f"✅ ETL completed in {format_duration(time.perf_counter() - etl_start_time)}"
        )

    def _process_enrollee(
        self, collection_model: IFormattedDatesModel, total_batches: int
    ):
        last_visited_batch_id = None
        pythonModel = collection_model.get("pythonModel")
        therapyModel = collection_model.get("therapyModel")
        base_query = collection_model.get("base_query")
        name = collection_model.get("name")
        for batch_num in range(total_batches):
            batch_start_time = time.perf_counter()

            query = {
                **base_query,
                **(
                    {"_id": {"$gt": last_visited_batch_id}}
                    if last_visited_batch_id
                    else {}
                ),
            }

            ardb_enrollees_from_db = list[ITherapyEnrollee](
                pythonModel.get_model().find(
                    filter=query,
                    limit=self.batch_size,
                    sort=[("_id", ASCENDING)],
                    projection={
                        "_id": 1,
                        "demographic.dob": 1,
                        "additionalInformation.deathDate": 1,
                    },
                )
            )

            if not ardb_enrollees_from_db:
                break

            last_visited_batch_id = ardb_enrollees_from_db[-1]["_id"]

            update_ardb_ops = list[UpdateOne]()

            for ardb_enrollee in ardb_enrollees_from_db:
                ardb_enrollee_id = ardb_enrollee.get("_id")
                if not ardb_enrollee_id:
                    continue

                dob = get_obj_value(ardb_enrollee, "demographic", "dob")
                deathDate = get_obj_value(
                    ardb_enrollee, "additionalInformation", "deathDate"
                )

                if dob is None and deathDate is None:
                    continue

                formatted_dob = from_string_to_formatted_date(dob)
                formatted_death_date = from_string_to_formatted_date(deathDate)

                update_ardb_ops.append(
                    UpdateOne(
                        filter={"_id": ardb_enrollee_id},
                        update={
                            "$set": {
                                **(
                                    {"demographic.formattedDob": formatted_dob}
                                    if formatted_dob is not None
                                    else {}
                                ),
                                **(
                                    {
                                        "additionalInformation.formattedDeathDate": formatted_death_date
                                    }
                                    if formatted_death_date is not None
                                    else {}
                                ),
                            }
                        },
                    )
                )

            if update_ardb_ops:
                print(f"🔄 Updating Ardb {name}: {len(update_ardb_ops)}")
                pythonModel.get_model().bulk_write(update_ardb_ops)
                therapyModel.get_model().bulk_write(update_ardb_ops)

            print(
                f"✅ Batch {batch_num + 1} completed in {format_duration(time.perf_counter() - batch_start_time)}"
            )

    def _process_patient(
        self, collection_model: IFormattedDatesModel, total_batches: int
    ):
        last_visited_batch_id = None
        pythonModel = collection_model.get("pythonModel")
        therapyModel = collection_model.get("therapyModel")
        base_query = collection_model.get("base_query")
        name = collection_model.get("name")
        for batch_num in range(total_batches):
            batch_start_time = time.perf_counter()

            query = {
                **base_query,
                **(
                    {"_id": {"$gt": last_visited_batch_id}}
                    if last_visited_batch_id
                    else {}
                ),
            }

            ardb_enrollees_from_db = list[ITherapyEnrollee](
                pythonModel.get_model().find(
                    filter=query,
                    limit=self.batch_size,
                    sort=[("_id", ASCENDING)],
                    projection={
                        "_id": 1,
                        "demographic.dob": 1,
                    },
                )
            )

            if not ardb_enrollees_from_db:
                break

            last_visited_batch_id = ardb_enrollees_from_db[-1]["_id"]

            update_ardb_ops = list[UpdateOne]()

            for ardb_enrollee in ardb_enrollees_from_db:
                ardb_enrollee_id = ardb_enrollee.get("_id")
                if not ardb_enrollee_id:
                    continue

                dob = get_obj_value(ardb_enrollee, "demographic", "dob")

                if dob is None:
                    continue

                formatted_dob = from_string_to_formatted_date(dob)

                update_ardb_ops.append(
                    UpdateOne(
                        filter={"_id": ardb_enrollee_id},
                        update={"$set": {"demographic.formattedDob": formatted_dob}},
                    )
                )

            if update_ardb_ops:
                print(f"🔄 Updating Ardb {name}: {len(update_ardb_ops)}")
                pythonModel.get_model().bulk_write(update_ardb_ops)
                therapyModel.get_model().bulk_write(update_ardb_ops)

            print(
                f"✅ Batch {batch_num + 1} completed in {format_duration(time.perf_counter() - batch_start_time)}"
            )

    def _process_subscriber(
        self, collection_model: IFormattedDatesModel, total_batches: int
    ):
        last_visited_batch_id = None
        pythonModel = collection_model.get("pythonModel")
        therapyModel = collection_model.get("therapyModel")
        base_query = collection_model.get("base_query")
        name = collection_model.get("name")
        for batch_num in range(total_batches):
            batch_start_time = time.perf_counter()

            query = {
                **base_query,
                **(
                    {"_id": {"$gt": last_visited_batch_id}}
                    if last_visited_batch_id
                    else {}
                ),
            }

            ardb_enrollees_from_db = list[ITherapyEnrollee](
                pythonModel.get_model().find(
                    filter=query,
                    limit=self.batch_size,
                    sort=[("_id", ASCENDING)],
                    projection={
                        "_id": 1,
                        "demographic.dob": 1,
                        "employment.startDate": 1,
                        "employment.endDate": 1,
                    },
                )
            )

            if not ardb_enrollees_from_db:
                break

            last_visited_batch_id = ardb_enrollees_from_db[-1]["_id"]

            update_ardb_ops = list[UpdateOne]()

            for ardb_enrollee in ardb_enrollees_from_db:
                ardb_enrollee_id = ardb_enrollee.get("_id")
                if not ardb_enrollee_id:
                    continue

                dob = get_obj_value(ardb_enrollee, "demographic", "dob")
                startDate = get_obj_value(ardb_enrollee, "employment", "startDate")
                endDate = get_obj_value(ardb_enrollee, "employment", "endDate")

                if dob is None and startDate is None and endDate is None:
                    continue

                formatted_dob = from_string_to_formatted_date(dob)
                formatted_start_date = from_string_to_formatted_date(startDate)
                formatted_end_date = from_string_to_formatted_date(endDate)

                update_ardb_ops.append(
                    UpdateOne(
                        filter={"_id": ardb_enrollee_id},
                        update={
                            "$set": {
                                **(
                                    {"demographic.formattedDob": formatted_dob}
                                    if formatted_dob is not None
                                    else {}
                                ),
                                **(
                                    {
                                        "employment.formattedStartDate": formatted_start_date
                                    }
                                    if formatted_start_date is not None
                                    else {}
                                ),
                                **(
                                    {"employment.formattedEndDate": formatted_end_date}
                                    if formatted_end_date is not None
                                    else {}
                                ),
                            }
                        },
                    )
                )

            if update_ardb_ops:
                print(f"🔄 Updating Ardb {name}: {len(update_ardb_ops)}")
                pythonModel.get_model().bulk_write(update_ardb_ops)
                therapyModel.get_model().bulk_write(update_ardb_ops)

            print(
                f"✅ Batch {batch_num + 1} completed in {format_duration(time.perf_counter() - batch_start_time)}"
            )

    def _process_eligibility(
        self, collection_model: IFormattedDatesModel, total_batches: int
    ):
        last_visited_batch_id = None
        pythonModel = collection_model.get("pythonModel")
        therapyModel = collection_model.get("therapyModel")
        base_query = collection_model.get("base_query")
        name = collection_model.get("name")
        for batch_num in range(total_batches):
            batch_start_time = time.perf_counter()

            query = {
                **base_query,
                **(
                    {"_id": {"$gt": last_visited_batch_id}}
                    if last_visited_batch_id
                    else {}
                ),
            }

            ardb_enrollees_from_db = list[ITherapyEnrollee](
                pythonModel.get_model().find(
                    filter=query,
                    limit=self.batch_size,
                    sort=[("_id", ASCENDING)],
                    projection={
                        "_id": 1,
                        "patient.dob": 1,
                        "serviceDate.startDate": 1,
                        "serviceDate.endDate": 1,
                        "additionalInformation.paidThrough.date": 1,
                        "additionalInformation.terminationReason.eventDate": 1,
                    },
                )
            )

            if not ardb_enrollees_from_db:
                break

            last_visited_batch_id = ardb_enrollees_from_db[-1]["_id"]

            update_ardb_ops = list[UpdateOne]()

            for ardb_enrollee in ardb_enrollees_from_db:
                ardb_enrollee_id = ardb_enrollee.get("_id")
                if not ardb_enrollee_id:
                    continue

                dob = get_obj_value(ardb_enrollee, "patient", "dob")
                startDate = get_obj_value(ardb_enrollee, "serviceDate", "startDate")
                endDate = get_obj_value(ardb_enrollee, "serviceDate", "endDate")
                paidThroughDate = get_obj_value(
                    ardb_enrollee, "additionalInformation", "paidThrough", "date"
                )
                terminationReasonEventDate = get_obj_value(
                    ardb_enrollee,
                    "additionalInformation",
                    "terminationReason",
                    "eventDate",
                )

                if (
                    dob is None
                    and startDate is None
                    and endDate is None
                    and paidThroughDate is None
                    and terminationReasonEventDate is None
                ):
                    continue

                formatted_dob = from_string_to_formatted_date(dob)
                formatted_start_date = from_string_to_formatted_date(startDate)
                formatted_end_date = from_string_to_formatted_date(endDate)
                formatted_paid_through_date = from_string_to_formatted_date(
                    paidThroughDate
                )
                formatted_termination_reason_event_date = from_string_to_formatted_date(
                    terminationReasonEventDate
                )

                update_ardb_ops.append(
                    UpdateOne(
                        filter={"_id": ardb_enrollee_id},
                        update={
                            "$set": {
                                **(
                                    {"patient.formattedDob": formatted_dob}
                                    if formatted_dob is not None
                                    else {}
                                ),
                                **(
                                    {
                                        "serviceDate.formattedStartDate": formatted_start_date
                                    }
                                    if formatted_start_date is not None
                                    else {}
                                ),
                                **(
                                    {"serviceDate.formattedEndDate": formatted_end_date}
                                    if formatted_end_date is not None
                                    else {}
                                ),
                                **(
                                    {
                                        "additionalInformation.paidThrough.formattedDate": formatted_paid_through_date
                                    }
                                    if formatted_paid_through_date is not None
                                    else {}
                                ),
                                **(
                                    {
                                        "additionalInformation.terminationReason.formattedEventDate": formatted_termination_reason_event_date
                                    }
                                    if formatted_termination_reason_event_date
                                    is not None
                                    else {}
                                ),
                            },
                            "$unset": {"demographic": 1},
                        },
                    )
                )

            if update_ardb_ops:
                print(f"🔄 Updating Ardb {name}: {len(update_ardb_ops)}")
                pythonModel.get_model().bulk_write(update_ardb_ops)
                therapyModel.get_model().bulk_write(update_ardb_ops)

            print(
                f"✅ Batch {batch_num + 1} completed in {format_duration(time.perf_counter() - batch_start_time)}"
            )
