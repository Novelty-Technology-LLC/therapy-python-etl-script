from pathlib import Path
import time
from typing import Optional

from pymongo import UpdateOne
from src.config.config import Config
from src.core.migrate.base_etl import BaseEtl
from src.core.migrate.script.data_frame.eligibility import (
    MISSING_ELIGIBILITY_MIGRATE_ETL_DATA_FRAME_TYPE,
    SELECTED_ELIGIBILITY_COLS,
    SELECTED_ELIGIBILITY_COLS_RENAMED,
    SELECTED_ENROLLEE_COLS,
    SELECTED_ENROLLEE_COLS_RENAMED,
    SELECTED_PATIENT_COLS,
    SELECTED_PATIENT_COLS_RENAMED,
    SELECTED_SUBSCRIBER_COLS,
    SELECTED_SUBSCRIBER_COLS_RENAMED,
)
from src.core.service.documents.model import documentsModel
from src.core.service.dump_records.model import DumpRecordsModel
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
from src.core.service.products.entity import ITherapyProduct
from src.core.service.products.model import productsModel
from src.core.service.subscribers.entity import ITherapySubscriber
from src.core.service.subscribers.mapper import subscriber_mapper
from src.core.service.subscribers.model import subscribersModel
from src.shared.constant.collection_name import CollectionName
from src.shared.constant.constant import BATCH_SIZE
from src.shared.interface.document import DocumentStatusEnum
from src.shared.interface.etl.migration import FileMetadata
from src.shared.interface.migration import InputFileType
from src.shared.utils.batch import get_total_batch
from src.shared.utils.dataframe import batch_iterator
from src.shared.utils.date import format_duration
from src.shared.utils.migration import generate_uuid, verify_and_generate_document
from src.shared.utils.obj import get_obj_value
from src.shared.utils.path import get_input_files_path
import pandas as pd
import numpy as np


class EligibilityMapMissingEnrolleePatientSubscriberAndEligibilityDataPatch(BaseEtl):
    def __init__(self, input_file_path: Path):
        super().__init__()
        self.batch_size = BATCH_SIZE
        self.input_file_path = input_file_path

        self.eligibility_dump_records_model = DumpRecordsModel(
            collection_name=CollectionName.DUMP_ELIGIBILITY
        )

        self.support_duplicate_documents = Config.get_documents().get(
            "support_duplicate_documents"
        )

        self.sheet_name = "ELIGIBILITY"
        self.enable_backup = False
        self.s3_module = "ardb-backup/eligibility"
        self.etl_type = "ELIGIBILITY"

    def execute(self):
        all_files = get_input_files_path(
            input_file_path=self.input_file_path,
            file_type=InputFileType.EXCEL,
        )

        print(f"📁 Total files: {len(all_files)}")

        for file in all_files:
            documentId: Optional[str] = None

            try:
                start = time.perf_counter()

                document_response = verify_and_generate_document(
                    file=file,
                    support_duplicate_documents=self.support_duplicate_documents,
                    s3_module=self.s3_module,
                    file_type=InputFileType.EXCEL,
                    enable_backup=self.enable_backup,
                    etl_type=self.etl_type,
                )

                if document_response is None:
                    continue

                documentId = document_response.get("documentId")
                file_metadata = document_response.get("file_metadata")

                print(f"Processing file: {file.name}")

                if documentId:
                    documentsModel.get_model().update_one(
                        {"_id": documentId},
                        {"$set": {"status": DocumentStatusEnum.PROCESSING}},
                    )

                df = pd.read_excel(
                    file,
                    sheet_name=self.sheet_name,
                    dtype=MISSING_ELIGIBILITY_MIGRATE_ETL_DATA_FRAME_TYPE,
                )
                df.replace({np.nan: None}, inplace=True)

                total_batches = get_total_batch(df)
                print(f"Total batches: {total_batches}")

                for batch_num, chunk in enumerate(batch_iterator(df)):
                    batch_start = time.perf_counter()
                    print(f"Processing batch {batch_num + 1} of {total_batches}")

                    self.load_data(chunk, file_metadata)

                    print(
                        f"Processing batch {batch_num + 1} of {total_batches} in {format_duration(time.perf_counter() - batch_start)}"
                    )

                elapsed = time.perf_counter() - start

                if documentId:
                    documentsModel.get_model().update_one(
                        {"_id": documentId},
                        {"$set": {"status": DocumentStatusEnum.COMPLETED}},
                    )
                print(
                    f"========== [END] Processing file: {file.name} in {format_duration(elapsed)} =========="
                )

            except Exception as e:
                print(f"Error processing file: {file.name} - {e}")
                if documentId:
                    documentsModel.get_model().update_one(
                        {"_id": documentId},
                        {
                            "$set": {
                                "status": DocumentStatusEnum.FAILED,
                                "reason": str(e),
                            }
                        },
                    )
                print(f"Error processing file: {file.name} - {e}")

    def load_data(self, chunk: pd.DataFrame, file_metadata: FileMetadata):
        self.load_enrollee(chunk, file_metadata)
        self.load_eligibility(chunk, file_metadata)

        chunk["ardbSourceDocument"] = file_metadata.get("ardb_file_name")
        chunk["ardbLastModifiedDate"] = file_metadata.get("ardb_file_processed_at")

        self.eligibility_dump_records_model.insert_many(chunk.to_dict("records"))

    def load_eligibility(self, chunk: pd.DataFrame, file_metadata: FileMetadata):

        # prepare data for eligibility(subscriber, patient, eligibility);
        self._execute_subscriber(chunk, file_metadata)
        self._execute_patient(chunk, file_metadata)
        self._execute_eligibility(chunk, file_metadata)

    def _execute_subscriber(self, chunk: pd.DataFrame, file_metadata: FileMetadata):
        print("=========== [START] Loading subscribers ===========")
        subscriber_df = (
            chunk[SELECTED_SUBSCRIBER_COLS]
            .drop_duplicates(
                subset=["SUBSCRIBER_ID", "EL_INSURED_ENROLLEE_ID"], keep="first"
            )
            .reset_index(drop=True)
            .rename(columns=SELECTED_SUBSCRIBER_COLS_RENAMED)
        )

        subscriber_query = {
            "$or": subscriber_df[["SUBSCRIBER_ID", "INSURED_ENROLLEE_ID"]]
            .rename(
                columns={
                    "SUBSCRIBER_ID": "subscriberNumber",
                    "INSURED_ENROLLEE_ID": "enrollee.referenceId",
                }
            )
            .to_dict("records")
        }
        enrollee_query = {
            "referenceId": {"$in": subscriber_df["INSURED_ENROLLEE_ID"].tolist()}
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
            insured_enrollee_id = getattr(subscriber_row, "INSURED_ENROLLEE_ID", None)

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
                        "_id": generate_uuid(),
                        "ENROLLEE_ID": insured_enrollee_id,
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
                subscriber_df.at[index, "_id"] = generate_uuid()
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
                    subscriber_df.at[index, "LAST_MODIFIED_DATE_TIME"],
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

    def _execute_patient(self, chunk: pd.DataFrame, file_metadata: FileMetadata):
        print("=========== [START] Loading patients ===========")
        patient_df = (
            chunk[SELECTED_PATIENT_COLS]
            .drop_duplicates(
                subset=["EL_ENROLLEE_ID", "SUBSCRIBER_ID", "MEMBER_ID"],
                keep="first",
            )
            .rename(columns=SELECTED_PATIENT_COLS_RENAMED)
            .reset_index(drop=True)
        )

        # build query
        patient_query = {
            "$or": patient_df[["ENROLLEE_ID", "SUBSCRIBER_ID", "MEMBER_ID"]]
            .rename(
                columns={
                    "ENROLLEE_ID": "enrollee.referenceId",
                    "SUBSCRIBER_ID": "subscriber.identificationCode",
                    "MEMBER_ID": "memberId",
                }
            )
            .to_dict("records")
        }

        enrollee_query = {"referenceId": {"$in": patient_df["ENROLLEE_ID"].tolist()}}
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
            patient_enrollee_id = getattr(patient_row, "ENROLLEE_ID", None)
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
                raw_patient_from_df["_id"] = generate_uuid()
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
                    patient_df.at[index, "LAST_MODIFIED_DATE_TIME"],
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

    def _execute_eligibility(self, chunk: pd.DataFrame, file_metadata: FileMetadata):
        print("=========== [START] Loading eligibility ===========")
        eligibility_df = (
            chunk[SELECTED_ELIGIBILITY_COLS]
            .rename(columns=SELECTED_ELIGIBILITY_COLS_RENAMED)
            .copy()
        )

        # build query
        enrollee_query = {
            "referenceId": {"$in": eligibility_df["ENROLLEE_ID"].tolist()}
        }
        subscriber_query = {
            "$or": eligibility_df[["INSURED_ENROLLEE_ID", "SUBSCRIBER_ID"]]
            .rename(
                columns={
                    "INSURED_ENROLLEE_ID": "enrollee.referenceId",
                    "SUBSCRIBER_ID": "subscriberNumber",
                }
            )
            .to_dict("records")
        }
        patient_query = {
            "$or": eligibility_df[["ENROLLEE_ID", "SUBSCRIBER_ID", "MEMBER_ID"]]
            .rename(
                columns={
                    "ENROLLEE_ID": "enrollee.referenceId",
                    "SUBSCRIBER_ID": "subscriber.identificationCode",
                    "MEMBER_ID": "memberId",
                }
            )
            .to_dict("records")
        }

        eligibility_query = {
            "$or": eligibility_df[
                ["ENROLLEE_ID", "PRODUCT_ID", "SUBSCRIBER_ID", "MEMBER_ID"]
            ]
            .rename(
                columns={
                    "ENROLLEE_ID": "enrollee.referenceId",
                    "PRODUCT_ID": "product.referenceId",
                    "SUBSCRIBER_ID": "subscriber.identificationCode",
                    "MEMBER_ID": "patient.memberId",
                }
            )
            .to_dict("records")
        }

        product_reference_ids = eligibility_df["PRODUCT_ID"].drop_duplicates().tolist()

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

        products_from_db = (
            list[ITherapyProduct](
                productsModel.get_model().find(
                    filter={"product.referenceId": {"$in": product_reference_ids}},
                    projection={"_id": 1, "name": 1, "product": {"referenceId": 1}},
                )
            )
            if product_reference_ids
            else []
        )

        for eligibility_row in eligibility_df.itertuples(index=True):
            index = getattr(eligibility_row, "Index", None)
            eligibility_enrollee_id = getattr(eligibility_row, "ENROLLEE_ID", None)
            eligibility_insured_enrollee_id = getattr(
                eligibility_row, "INSURED_ENROLLEE_ID", None
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
            therapy_product_from_db: Optional[ITherapyProduct] = None

            for product in products_from_db:
                if (
                    get_obj_value(product, "product", "referenceId")
                    == eligibility_product_id
                ):
                    therapy_product_from_db = product
                    break

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
                raw_eligibility_from_df["_id"] = generate_uuid()
                raw_eligibility_from_df["hasCompleteInfo"] = hasCompleteInfo
                eligibility_to_therapy = eligibility_mapper.to_therapy(
                    raw_eligibility_from_df,
                    therapy_enrollee_from_db,
                    therapy_subscriber_from_db,
                    therapy_patient_from_db,
                    therapy_product_from_db,
                    file_metadata,
                )

                inserted_eligibilities.append(eligibility_to_therapy)

            # Update
            if therapy_eligibility_from_db is not None:
                raw_eligibility_from_df["_id"] = therapy_eligibility_from_db.get("_id")
                raw_eligibility_from_df["hasCompleteInfo"] = hasCompleteInfo

                date_ardb = pd.to_datetime(
                    eligibility_df.at[index, "LAST_MODIFIED_DATE_TIME"],
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
                        therapy_product_from_db,
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

    def load_enrollee(self, chunk: pd.DataFrame, file_metadata: FileMetadata):
        # prepare data fdr enrollee
        print("=========== [START] Loading enrollees ===========")
        enrollee_df = (
            chunk[SELECTED_ENROLLEE_COLS]
            .drop_duplicates(subset=["EN_ENROLLEE_ID"], keep="first")
            .reset_index(drop=True)
            .rename(columns=SELECTED_ENROLLEE_COLS_RENAMED)
        )
        enrollee_df.replace({np.nan: None}, inplace=True)

        # fetch enrollee from db
        query = {"referenceId": {"$in": enrollee_df["ENROLLEE_ID"].tolist()}}
        enrollee_from_db = enrollee_adapter.to_ardb_format(
            list[ITherapyEnrollee](enrolleesModel.get_model().find(query))
        )

        for row in enrollee_df.itertuples(index=True):
            index = getattr(row, "Index", None)
            ardb_enrollee_id = getattr(row, "ENROLLEE_ID", None)

            therapy_enrollee_from_db = None
            for enrollee in enrollee_from_db:
                if enrollee["ENROLLEE_ID"] == ardb_enrollee_id:
                    therapy_enrollee_from_db = enrollee
                    break

            # insert
            if therapy_enrollee_from_db is None:
                enrollee_df.at[index, "_id"] = generate_uuid()
                enrollee_df.at[index, "type"] = "insert"
                enrollee_df.at[index, "hasCompleteInfo"] = True

            # update
            if therapy_enrollee_from_db is not None:
                enrollee_df.at[index, "_id"] = therapy_enrollee_from_db.get("_id")
                date_ardb = pd.to_datetime(
                    enrollee_df.at[index, "LAST_MODIFIED_DATE_TIME"],
                    errors="coerce",
                )
                date_therapy = pd.to_datetime(
                    therapy_enrollee_from_db.get("LAST_MODIFIED_DATE_TIME"),
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
