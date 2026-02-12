from pathlib import Path
from typing import Generator
import time
from pymongo.operations import UpdateOne
import uuid_utils as uuid
import pandas as pd
import numpy as np
from datetime import datetime

from src.core.migrate.base_etl import BaseEtl
from src.core.service.dump_records.model import DumpRecordsModel
from src.shared.utils.batch import get_total_batch
from src.shared.utils.dataframe import batch_iterator
from src.core.data_frame_type.eligibility import (
    ELIGIBILITY_COLS,
    ELIGIBILITY_ETL_DATA_FRAME_TYPE,
    ENROLLEE_COLS,
    PATIENT_COLS,
    SUBSCRIBER_COLS,
)
from src.core.service.eligibility.entity import ITherapyEligibility
from src.core.service.eligibility.mapper import eligibility_mapper
from src.core.service.eligibility.model import eligibilityModel
from src.core.service.enrollees.adpater import enrollee_adapter
from src.core.service.enrollees.entity import ITherapyEnrollee
from src.core.service.enrollees.mapper import enrollee_mapper
from src.core.service.enrollees.model import enrolleesModel
from src.core.service.patients.entity import ITherapyPatient
from src.core.service.patients.mapper import patient_mapper
from src.core.service.patients.model import patientsModel
from src.core.service.subscribers.entity import ITherapySubscriber
from src.core.service.subscribers.mapper import subscriber_mapper
from src.core.service.subscribers.model import subscribersModel
from src.shared.constant.collection_name import CollectionName
from src.shared.interface.etl.migration import FileMetadata
from src.shared.interface.migration import InputFileType
from src.shared.utils.date import format_duration
from src.shared.utils.obj import get_obj_value
from src.shared.utils.path import get_input_files_path


class Eligibility_Etl(BaseEtl):

    def __init__(self):
        super().__init__()

    def execute(self):
        all_files = get_input_files_path(
            input_file_path=Path("input-files/eligibility"),
            file_type=InputFileType.EXCEL,
        )

        dump_records_model = DumpRecordsModel(
            collection_name=CollectionName.DUMP_ELIGIBILITY
        )

        ardb_file_processed_at = datetime.now()
        ardb_file_path = "ETL_SCRIPTS"

        # Create data frame
        for file in all_files:
            start = time.perf_counter()
            print(f"========== [START] Processing file: {file.name} ==========")

            ardb_file_name = file.name
            file_metadata = FileMetadata(
                ardb_file_processed_at=ardb_file_processed_at,
                ardb_file_name=ardb_file_name,
                ardb_file_path=ardb_file_path,
            )

            print(file)
            df = pd.read_excel(
                file, sheet_name="ELIGIBILITY", dtype=ELIGIBILITY_ETL_DATA_FRAME_TYPE
            )

            df.replace({np.nan: None}, inplace=True)

            # Batch processing
            total_batches = get_total_batch(df)
            print(f"Total batches: {total_batches}")

            for batch_num, chunk in enumerate(batch_iterator(df)):
                print(f"Processing batch {batch_num + 1} of {total_batches}")

                self.load_enrollee(chunk, file_metadata)

                self.load_subscriber(chunk, file_metadata)

                self.load_patient(chunk, file_metadata)

                self.load_eligibility(chunk, file_metadata)

                print("========= DUMP RECORDS ==========")
                df["ardbSourceDocument"] = ardb_file_name
                df["ardbLastModifiedDate"] = ardb_file_processed_at

                dump_records_model.insert_many(df.to_dict("records"))

            elapsed = time.perf_counter() - start
            print(
                f"========== [END] Processing file: {file.name} in {format_duration(elapsed)} =========="
            )

    def load_enrollee(self, df: pd.DataFrame, file_metadata: FileMetadata):
        print("=========== [START] Loading enrollees ===========")

        enrollee_df = (
            df[ENROLLEE_COLS]
            .drop_duplicates(subset=["EN_ENROLLEE_ID"], keep="first")
            .reset_index(drop=True)
        )

        # fetch enrollee from db
        query = {"referenceId": {"$in": enrollee_df["EN_ENROLLEE_ID"].tolist()}}
        enrollee_from_db = enrollee_adapter.to_ardb_format(
            list[ITherapyEnrollee](enrolleesModel.get_model().find(query))
        )

        for row in enrollee_df.itertuples(index=True):
            index = getattr(row, "Index", None)
            ardb_enrollee_id = getattr(row, "EN_ENROLLEE_ID", None)

            therapy_enrollee_from_db = None
            for enrollee in enrollee_from_db:
                if enrollee["EN_ENROLLEE_ID"] == ardb_enrollee_id:
                    therapy_enrollee_from_db = enrollee
                    break

            # insert
            if therapy_enrollee_from_db is None:
                enrollee_df.at[index, "_id"] = str(uuid.uuid7())
                enrollee_df.at[index, "type"] = "insert"
                enrollee_df.at[index, "hasCompleteInfo"] = True

            # update
            if therapy_enrollee_from_db is not None:
                enrollee_df.at[index, "_id"] = therapy_enrollee_from_db.get("_id")
                date_ardb = pd.to_datetime(
                    enrollee_df.at[index, "EN_LAST_MODIFIED_DATE_TIME"],
                    errors="coerce",
                )
                date_therapy = pd.to_datetime(
                    therapy_enrollee_from_db.get("EN_LAST_MODIFIED_DATE_TIME"),
                    errors="coerce",
                )
                has_complete_info = therapy_enrollee_from_db.get(
                    "hasCompleteInfo", False
                )

                # ARDB missing date -> no_action; therapy missing date or ARDB newer or hasCompleteInfo -> update
                if pd.isna(date_ardb):
                    enrollee_df.at[index, "type"] = "no_action"
                elif (
                    pd.isna(date_therapy)
                    or (date_ardb > date_therapy)
                    or not has_complete_info
                ):
                    enrollee_df.at[index, "type"] = "update"
                    enrollee_df.at[index, "hasCompleteInfo"] = True
                else:
                    enrollee_df.at[index, "type"] = "no_action"

        # 4. Db operations
        inserted_enrollees = enrollee_df[enrollee_df["type"] == "insert"].to_dict(
            "records"
        )
        if len(inserted_enrollees) > 0:
            enrolleesModel.insert_many(
                enrollee_adapter.to_therapy_format(inserted_enrollees, file_metadata)
            )

        updated_enrollee = enrollee_adapter.to_therapy_format(
            enrollee_df[enrollee_df["type"] == "update"].to_dict("records"),
            file_metadata,
        )

        if len(updated_enrollee) > 0:
            enrolleesModel.get_model().bulk_write(
                [
                    UpdateOne(
                        {"_id": enrollee["_id"]},
                        {
                            "$set": {
                                k: v
                                for k, v in enrollee.items()
                                if k not in ("_id", "ardbDocuments")
                            },
                            "$push": {
                                "ardbDocuments": {"$each": enrollee["ardbDocuments"]}
                            },
                        },
                    )
                    for enrollee in updated_enrollee
                ]
            )

        print("=========== [END] Loading enrollees ===========")

    def load_subscriber(self, df: pd.DataFrame, file_metadata: FileMetadata):
        print("=========== [START] Loading subscribers ===========")

        subscriber_df = (
            df[SUBSCRIBER_COLS]
            .drop_duplicates(
                subset=["SUBSCRIBER_ID", "EL_INSURED_ENROLLEE_ID"], keep="first"
            )
            .reset_index(drop=True)
        )

        subscriber_query = {
            "$or": subscriber_df[["SUBSCRIBER_ID", "EL_INSURED_ENROLLEE_ID"]]
            .rename(
                columns={
                    "SUBSCRIBER_ID": "subscriberNumber",
                    "EL_INSURED_ENROLLEE_ID": "enrollee.referenceId",
                }
            )
            .to_dict("records")
        }
        enrollee_query = {
            "referenceId": {"$in": subscriber_df["EL_INSURED_ENROLLEE_ID"].tolist()}
        }

        # fetch enrollee from db
        enrollees_from_db = list[ITherapyEnrollee](
            enrolleesModel.get_model().find(enrollee_query)
        )

        subscribers_from_db = list[ITherapySubscriber](
            subscribersModel.get_model().find(subscriber_query)
        )

        inserted_enrollees = []
        inserted_subscribers = []
        updated_subscribers = []

        for subscriber_row in subscriber_df.itertuples(index=True):
            index = getattr(subscriber_row, "Index", None)
            subscriber_id = getattr(subscriber_row, "SUBSCRIBER_ID", None)
            insured_enrollee_id = getattr(
                subscriber_row, "EL_INSURED_ENROLLEE_ID", None
            )

            therapy_enrollee_from_db = None
            therapy_subscriber_from_db = None

            for enrollee in enrollees_from_db:
                if enrollee["referenceId"] == insured_enrollee_id:
                    therapy_enrollee_from_db = enrollee
                    break

            for enrollee in inserted_enrollees:
                if enrollee["referenceId"] == insured_enrollee_id:
                    therapy_enrollee_from_db = enrollee
                    break

            if therapy_enrollee_from_db is None:
                therapy_enrollee_from_db = enrollee_mapper.to_therapy(
                    {
                        "_id": str(uuid.uuid7()),
                        "EN_ENROLLEE_ID": insured_enrollee_id,
                        "hasCompleteInfo": False,
                    },
                    file_metadata,
                )
                inserted_enrollees.append(therapy_enrollee_from_db)

            for subscriber in subscribers_from_db:
                if (
                    subscriber["subscriberNumber"] == subscriber_id
                    and subscriber["enrollee"]["referenceId"] == insured_enrollee_id
                ):
                    therapy_subscriber_from_db = subscriber
                    break

            # Insert
            if therapy_subscriber_from_db is None:
                subscriber_df.at[index, "_id"] = str(uuid.uuid7())
                subscriber_df.at[index, "hasCompleteInfo"] = (
                    False
                    if therapy_enrollee_from_db is None
                    or therapy_enrollee_from_db.get("hasCompleteInfo") is False
                    else True
                )

                row_dict = subscriber_df.loc[index].to_dict()

                inserted_subscribers.append(
                    subscriber_mapper.to_therapy_subscriber_enrollee(
                        row_dict, therapy_enrollee_from_db, file_metadata
                    )
                )

            # update
            if therapy_subscriber_from_db is not None:
                subscriber_df.at[index, "_id"] = therapy_subscriber_from_db.get("_id")

                date_ardb = pd.to_datetime(
                    subscriber_df.at[index, "EN_LAST_MODIFIED_DATE_TIME"],
                    errors="coerce",
                )
                date_therapy = pd.to_datetime(
                    get_obj_value(therapy_subscriber_from_db, "updated", "at"),
                    errors="coerce",
                )
                has_complete_info = (
                    False
                    if therapy_enrollee_from_db is None
                    or therapy_enrollee_from_db.get("hasCompleteInfo") is False
                    else True
                )

                subscriber_df.at[index, "hasCompleteInfo"] = has_complete_info

                if (
                    pd.isna(date_therapy)
                    or (date_ardb > date_therapy)
                    or not has_complete_info
                ):
                    row_dict = subscriber_df.loc[index].to_dict()
                    updated_subscribers.append(
                        subscriber_mapper.to_therapy_subscriber_enrollee(
                            row_dict,
                            therapy_enrollee_from_db,
                            file_metadata,
                        )
                    )

        # 4. Db operations
        # 4.1. insert enrollee
        if len(inserted_enrollees) > 0:
            enrolleesModel.insert_many(inserted_enrollees)

        # 4.2. insert subscriber
        if len(inserted_subscribers) > 0:
            subscribersModel.insert_many(inserted_subscribers)

        # 4.3. update subscriber
        if len(updated_subscribers) > 0:
            subscribersModel.get_model().bulk_write(
                [
                    UpdateOne(
                        {"_id": subscriber["_id"]},
                        {
                            "$set": {
                                k: v
                                for k, v in subscriber.items()
                                if k not in ("_id", "ardbDocuments")
                            },
                            "$push": {
                                "ardbDocuments": {"$each": subscriber["ardbDocuments"]}
                            },
                        },
                    )
                    for subscriber in updated_subscribers
                ]
            )

        print("=========== [END] Loading subscribers ===========")

    def load_patient(self, df: pd.DataFrame, file_metadata: FileMetadata):
        print("=========== [START] Loading patients ===========")
        patient_df = (
            df[PATIENT_COLS]
            .drop_duplicates(
                subset=["EN_ENROLLEE_ID", "SUBSCRIBER_ID", "MEMBER_ID"],
                keep="first",
            )
            .reset_index(drop=True)
        )

        # build query
        patient_query = {
            "$or": patient_df[["EN_ENROLLEE_ID", "SUBSCRIBER_ID", "MEMBER_ID"]]
            .rename(
                columns={
                    "EN_ENROLLEE_ID": "enrollee.referenceId",
                    "SUBSCRIBER_ID": "subscriber.identificationCode",
                    "MEMBER_ID": "memberId",
                }
            )
            .to_dict("records")
        }

        enrollee_query = {"referenceId": {"$in": patient_df["EN_ENROLLEE_ID"].tolist()}}
        subscriber_query = {
            "subscriberNumber": {"$in": patient_df["SUBSCRIBER_ID"].tolist()}
        }

        # fetch data
        enrollees_from_db = list[ITherapyEnrollee](
            enrolleesModel.get_model().find(enrollee_query)
        )
        subscribers_from_db = list[ITherapySubscriber](
            subscribersModel.get_model().find(subscriber_query)
        )
        patients_from_db = list[ITherapyPatient](
            patientsModel.get_model().find(patient_query)
        )

        inserted_patients = []
        updated_patients = []

        for patient_row in patient_df.itertuples(index=True):

            index = getattr(patient_row, "Index", None)
            patient_enrollee_id = getattr(patient_row, "EN_ENROLLEE_ID", None)
            patient_subscriber_id = getattr(patient_row, "SUBSCRIBER_ID", None)
            patient_member_id = getattr(patient_row, "MEMBER_ID", None)

            raw_patient_from_df = patient_df.loc[index].to_dict()

            therapy_enrollee_from_db = None
            therapy_subscriber_from_db = None
            therapy_patient_from_db = None

            for enrollee in enrollees_from_db:
                if enrollee["referenceId"] == patient_enrollee_id:
                    therapy_enrollee_from_db = enrollee
                    break

            for subscriber in subscribers_from_db:
                if subscriber["subscriberNumber"] == patient_subscriber_id:
                    therapy_subscriber_from_db = subscriber
                    break

            for patient in patients_from_db:
                if (
                    patient["enrollee"]["referenceId"] == patient_enrollee_id
                    and patient["subscriber"]["identificationCode"]
                    == patient_subscriber_id
                    and patient["memberId"] == patient_member_id
                ):
                    therapy_patient_from_db = patient
                    break

            hasCompleteInfo = False

            if therapy_enrollee_from_db.get(
                "hasCompleteInfo"
            ) and therapy_subscriber_from_db.get("hasCompleteInfo"):
                hasCompleteInfo = True

            # Insert
            if therapy_patient_from_db is None:
                raw_patient_from_df["_id"] = str(uuid.uuid7())
                raw_patient_from_df["hasCompleteInfo"] = hasCompleteInfo
                patient_to_therapy = patient_mapper.to_therapy(
                    raw_patient_from_df,
                    therapy_enrollee_from_db,
                    therapy_subscriber_from_db,
                    file_metadata,
                )
                inserted_patients.append(patient_to_therapy)

            # Update
            if therapy_patient_from_db is not None:
                raw_patient_from_df["_id"] = therapy_patient_from_db.get("_id")
                raw_patient_from_df["hasCompleteInfo"] = hasCompleteInfo

                date_ardb = pd.to_datetime(
                    patient_df.at[index, "EN_LAST_MODIFIED_DATE_TIME"],
                    errors="coerce",
                )
                date_therapy = pd.to_datetime(
                    get_obj_value(therapy_subscriber_from_db, "updated", "at"),
                    errors="coerce",
                )

                if (
                    pd.isna(date_therapy)
                    or (date_ardb > date_therapy)
                    or not hasCompleteInfo
                ):
                    patient_to_therapy = patient_mapper.to_therapy(
                        raw_patient_from_df,
                        therapy_enrollee_from_db,
                        therapy_subscriber_from_db,
                        file_metadata,
                    )
                    updated_patients.append(patient_to_therapy)

        # 4. Db operations
        # 4.1. insert patient
        if len(inserted_patients) > 0:
            patientsModel.insert_many(inserted_patients)

        # 4.2. update patient
        if len(updated_patients) > 0:
            patientsModel.get_model().bulk_write(
                [
                    UpdateOne(
                        {"_id": patient["_id"]},
                        {
                            "$set": {
                                k: v
                                for k, v in patient.items()
                                if k not in ("_id", "ardbDocuments")
                            },
                            "$push": {"ardbDocuments": {"$each": ["ardbDocuments"]}},
                        },
                    )
                    for patient in updated_patients
                ]
            )

        print("=========== [END] Loading patients ===========")

    def load_eligibility(self, df: pd.DataFrame, file_metadata: FileMetadata):
        print("=========== [START] Loading eligibility ===========")
        eligibility_df = df[ELIGIBILITY_COLS].copy()

        # build query
        enrollee_query = {
            "referenceId": {"$in": eligibility_df["EL_ENROLLEE_ID"].tolist()}
        }
        subscriber_query = {
            "$or": eligibility_df[["EL_INSURED_ENROLLEE_ID", "SUBSCRIBER_ID"]]
            .rename(
                columns={
                    "EL_INSURED_ENROLLEE_ID": "enrollee.referenceId",
                    "SUBSCRIBER_ID": "subscriberNumber",
                }
            )
            .to_dict("records")
        }
        patient_query = {
            "$or": eligibility_df[["EL_ENROLLEE_ID", "SUBSCRIBER_ID", "MEMBER_ID"]]
            .rename(
                columns={
                    "EL_ENROLLEE_ID": "enrollee.referenceId",
                    "SUBSCRIBER_ID": "subscriber.identificationCode",
                    "MEMBER_ID": "memberId",
                }
            )
            .to_dict("records")
        }

        eligibility_query = {
            "$or": eligibility_df[
                ["EL_ENROLLEE_ID", "PRODUCT_ID", "SUBSCRIBER_ID", "MEMBER_ID"]
            ]
            .rename(
                columns={
                    "EL_ENROLLEE_ID": "enrollee.referenceId",
                    "PRODUCT_ID": "product.referenceId",
                    "SUBSCRIBER_ID": "subscriber.identificationCode",
                    "MEMBER_ID": "patient.memberId",
                }
            )
            .to_dict("records")
        }

        inserted_eligibilities = []
        updated_eligibilities = []

        # fetch data
        enrollees_from_db = list[ITherapyEnrollee](
            enrolleesModel.get_model().find(enrollee_query)
        )
        subscribers_from_db = list[ITherapySubscriber](
            subscribersModel.get_model().find(subscriber_query)
        )
        patients_from_db = list[ITherapyPatient](
            patientsModel.get_model().find(patient_query)
        )
        eligibility_from_db = list[ITherapyEligibility](
            eligibilityModel.get_model().find(eligibility_query)
        )

        for eligibility_row in eligibility_df.itertuples(index=True):
            index = getattr(eligibility_row, "Index", None)
            eligibility_enrollee_id = getattr(eligibility_row, "EL_ENROLLEE_ID", None)
            eligibility_insured_enrollee_id = getattr(
                eligibility_row, "EL_INSURED_ENROLLEE_ID", None
            )
            eligibility_product_id = getattr(eligibility_row, "PRODUCT_ID", None)
            eligibility_subscriber_id = getattr(eligibility_row, "SUBSCRIBER_ID", None)
            eligibility_member_id = getattr(eligibility_row, "MEMBER_ID", None)

            eligibility_effectiveDate = getattr(eligibility_row, "EFFECTIVE_DATE", None)
            eligibility_terminationDate = getattr(
                eligibility_row, "TERMINATION_DATE", None
            )

            raw_eligibility_from_df = eligibility_df.loc[index].to_dict()

            therapy_enrollee_from_db = None
            therapy_subscriber_from_db = None
            therapy_patient_from_db = None
            therapy_eligibility_from_db = None

            for enrollee in enrollees_from_db:
                if enrollee["referenceId"] == eligibility_enrollee_id:
                    therapy_enrollee_from_db = enrollee
                    break

            for subscriber in subscribers_from_db:
                if (
                    subscriber["subscriberNumber"] == eligibility_subscriber_id
                    and subscriber["enrollee"]["referenceId"]
                    == eligibility_insured_enrollee_id
                ):
                    therapy_subscriber_from_db = subscriber
                    break

            for patient in patients_from_db:
                if (
                    patient["enrollee"]["referenceId"] == eligibility_enrollee_id
                    and patient["subscriber"]["identificationCode"]
                    == eligibility_subscriber_id
                    and patient["memberId"] == eligibility_member_id
                ):
                    therapy_patient_from_db = patient
                    break

            for eligibility in eligibility_from_db:
                if (
                    eligibility["enrollee"]["referenceId"] == eligibility_enrollee_id
                    and (
                        eligibility["product"]["referenceId"] == eligibility_product_id
                    )
                    and (
                        eligibility["subscriber"]["identificationCode"]
                        == eligibility_subscriber_id
                    )
                    and eligibility["patient"]["memberId"] == eligibility_member_id
                    and (
                        eligibility["serviceDate"]["formattedStartDate"]
                        == eligibility_effectiveDate
                    )
                    and (
                        eligibility["serviceDate"]["formattedEndDate"]
                        == eligibility_terminationDate
                    )
                ):
                    therapy_eligibility_from_db = eligibility
                    break

            hasCompleteInfo = False

            if (
                therapy_enrollee_from_db.get("hasCompleteInfo")
                and therapy_subscriber_from_db.get("hasCompleteInfo")
                and therapy_patient_from_db.get("hasCompleteInfo")
            ):
                hasCompleteInfo = True

            # Insert
            if therapy_eligibility_from_db is None:
                raw_eligibility_from_df["_id"] = str(uuid.uuid7())
                raw_eligibility_from_df["hasCompleteInfo"] = hasCompleteInfo
                eligibility_to_therapy = eligibility_mapper.to_therapy(
                    raw_eligibility_from_df,
                    therapy_enrollee_from_db,
                    therapy_subscriber_from_db,
                    therapy_patient_from_db,
                    file_metadata,
                )

                inserted_eligibilities.append(eligibility_to_therapy)

            # Update
            if therapy_eligibility_from_db is not None:
                raw_eligibility_from_df["_id"] = therapy_eligibility_from_db.get("_id")
                raw_eligibility_from_df["hasCompleteInfo"] = hasCompleteInfo

                date_ardb = pd.to_datetime(
                    eligibility_df.at[index, "EL_LAST_MODIFIED_DATE_TIME"],
                    errors="coerce",
                )
                date_therapy = pd.to_datetime(
                    get_obj_value(therapy_eligibility_from_db, "updated", "at"),
                    errors="coerce",
                )

                if (
                    pd.isna(date_therapy)
                    or (date_ardb > date_therapy)
                    or not hasCompleteInfo
                ):
                    eligibility_to_therapy = eligibility_mapper.to_therapy(
                        raw_eligibility_from_df,
                        therapy_enrollee_from_db,
                        therapy_subscriber_from_db,
                        therapy_patient_from_db,
                        file_metadata,
                    )
                    updated_eligibilities.append(eligibility_to_therapy)

        # 4. Db operations
        # 4.1. insert eligibility
        if len(inserted_eligibilities) > 0:
            eligibilityModel.insert_many(inserted_eligibilities)

        # 4.2. update eligibility
        if len(updated_eligibilities) > 0:
            eligibilityModel.get_model().bulk_write(
                [
                    UpdateOne(
                        {"_id": eligibility["_id"]},
                        {
                            "$set": {
                                k: v
                                for k, v in eligibility.items()
                                if k not in ("_id", "ardbDocuments")
                            },
                            "$push": {"ardbDocuments": {"$each": ["ardbDocuments"]}},
                        },
                    )
                    for eligibility in updated_eligibilities
                ]
            )

        print("=========== [END] Loading eligibility ===========")
