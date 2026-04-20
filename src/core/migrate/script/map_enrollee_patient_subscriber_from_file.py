from pathlib import Path
import time
from typing import Optional, Set

from pymongo import UpdateMany, UpdateOne
from src.core.migrate.base_etl import BaseEtl
from src.core.migrate.script.interface.invoice_billing_map_enrollee import (
    IPatientQuery,
    ISubscriberQuery,
)
from src.core.service.documents.model import documentsModel
from src.core.service.enrollees.entity import ITherapyEnrollee
from src.core.service.enrollees.model import enrolleesModel
from src.core.service.invoice_billings.model import invoiceBillingsModel
from src.core.service.invoice_payment_receipts.model import invoicePaymentReceiptsModel
from src.core.service.patients.entity import ITherapyPatient
from src.core.service.patients.model import patientsModel
from src.core.service.receipt_details.model import receiptDetailsModel
from src.core.service.subscribers.entity import ITherapySubscriber
from src.core.service.subscribers.model import subscribersModel
from src.shared.constant.constant import BATCH_SIZE
from src.shared.interface.document import DocumentStatusEnum
from src.shared.interface.migration import InputFileType
from src.shared.utils.batch import get_total_batch
from src.shared.utils.dataframe import batch_iterator
from src.shared.utils.date import format_duration
from src.shared.utils.migration import verify_and_generate_document
from src.shared.utils.name import get_name
from src.shared.utils.path import get_input_files_path
import pandas as pd
import numpy as np


class MapEnrolleePatientSubscriberFromFile(BaseEtl):
    def __init__(self, input_file_path: Path):
        super().__init__()
        self.batch_size = BATCH_SIZE
        self.input_file_path = input_file_path

        self.support_duplicate_documents = False
        self.sheet_name = "BILLING"
        self.enable_backup = False
        self.s3_module = "ardb-backup/eligibility"
        self.etl_type = "MAP_ENROLLEE_PATIENT_SUBSCRIBER"
        self.file_type = InputFileType.EXCEL

    def execute(self):
        print(f"🔄 [START] Map Enrollee Patient Subscriber From File")
        print(f"🔧 [START] Processing map enrollee patient subscriber from file")
        start_time = time.perf_counter()

        all_files = get_input_files_path(
            input_file_path=self.input_file_path,
            file_type=self.file_type,
        )

        print(f"📁 Total files: {len(all_files)}")

        for file in all_files:
            documentId: Optional[str] = None
            file_start_time = time.perf_counter()

            try:
                document_response = verify_and_generate_document(
                    file,
                    self.support_duplicate_documents,
                    self.s3_module,
                    self.file_type,
                    self.enable_backup,
                    self.etl_type,
                )
                if document_response is None:
                    continue

                documentId = document_response.get("documentId")
                file_metadata = document_response.get("file_metadata")

                if documentId:
                    documentsModel.get_model().update_one(
                        {"_id": documentId},
                        {"$set": {"status": DocumentStatusEnum.PROCESSING}},
                    )

                print("📊 [START] Loading on data frame")
                df = pd.read_excel(
                    file,
                    sheet_name=self.sheet_name,
                    dtype=str,
                    usecols=[
                        "INVOICE_BILLING_ID",
                        "ENROLLEE_ID",
                        "SUBSCRIBER_ID",
                        "MEMBER_ID",
                        "INSURED_ENROLLEE_ID",
                    ],
                )

                total_batches = get_total_batch(df)
                print(f"📦📦📦 Total batches: {total_batches}")

                for batch_num, chunk in enumerate(batch_iterator(df)):
                    print(f"Processing batch {batch_num + 1} of {total_batches}")
                    batch_start_time = time.perf_counter()

                    chunk.replace({np.nan: None}, inplace=True)

                    collect_enrollee_ids: Set[str] = set[str]()
                    collect_subscriber_query: list[ISubscriberQuery] = []
                    collect_patient_query: list[IPatientQuery] = []

                    for row in chunk.to_dict("records"):
                        enrollee_id = row.get("ENROLLEE_ID")
                        subscriber_id = row.get("SUBSCRIBER_ID")
                        member_id = row.get("MEMBER_ID")
                        insured_enrollee_id = row.get("INSURED_ENROLLEE_ID")

                        if enrollee_id:
                            collect_enrollee_ids.add(enrollee_id)

                        if subscriber_id and insured_enrollee_id:
                            collect_subscriber_query.append(
                                {
                                    "subscriberNumber": subscriber_id,
                                    "insuredEnrollee.referenceId": insured_enrollee_id,
                                }
                            )

                        if enrollee_id and subscriber_id and member_id:
                            collect_patient_query.append(
                                {
                                    "enrollee.referenceId": enrollee_id,
                                    "subscriber.identificationCode": subscriber_id,
                                    "memberId": member_id,
                                }
                            )

                    enrollees_from_db = (
                        list[ITherapyEnrollee](
                            enrolleesModel.get_model().find(
                                {"referenceId": {"$in": list(collect_enrollee_ids)}}
                            )
                        )
                        if collect_enrollee_ids
                        else []
                    )

                    subscribers_from_db = (
                        list[ITherapySubscriber](
                            subscribersModel.get_model().find(
                                {"$or": collect_subscriber_query}
                            )
                        )
                        if collect_subscriber_query
                        else []
                    )

                    patients_from_db = (
                        list[ITherapyPatient](
                            patientsModel.get_model().find(
                                {"$or": collect_patient_query}
                            )
                        )
                        if collect_patient_query
                        else []
                    )

                    # variables to collect db operations
                    update_invoice_billings_operations: list[UpdateOne] = []
                    update_invoice_payment_receipts_operations: list[UpdateMany] = []
                    update_receipt_details_operations: list[UpdateMany] = []

                    for row in chunk.to_dict("records"):
                        enrollee_id = row.get("ENROLLEE_ID")
                        subscriber_id = row.get("SUBSCRIBER_ID")
                        member_id = row.get("MEMBER_ID")
                        insured_enrollee_id = row.get("INSURED_ENROLLEE_ID")

                        invoice_billing_number = row.get("INVOICE_BILLING_ID")

                        enrollee_from_db = next(
                            (
                                enrollee
                                for enrollee in enrollees_from_db
                                if enrollee.get("referenceId") == enrollee_id
                            ),
                            None,
                        )
                        subscriber_from_db = next(
                            (
                                subscriber
                                for subscriber in subscribers_from_db
                                if subscriber.get("subscriberNumber") == subscriber_id
                                and subscriber.get("insuredEnrollee").get("referenceId")
                                == insured_enrollee_id
                            ),
                            None,
                        )
                        patient_from_db = next(
                            (
                                patient
                                for patient in patients_from_db
                                if patient.get("enrollee").get("referenceId")
                                == enrollee_id
                                and patient.get("subscriber").get("identificationCode")
                                == subscriber_id
                                and patient.get("memberId") == member_id
                            ),
                            None,
                        )

                        if (
                            not enrollee_from_db
                            and not subscriber_from_db
                            and not patient_from_db
                        ):
                            continue

                        update_enrollee = (
                            {
                                "referenceId": enrollee_from_db.get("referenceId"),
                                "refId": enrollee_from_db.get("_id"),
                            }
                            if enrollee_from_db
                            else None
                        )

                        update_subscriber = (
                            {
                                "refId": subscriber_from_db.get("_id"),
                                "identificationCode": subscriber_from_db.get(
                                    "subscriberNumber"
                                ),
                                "name": get_name(
                                    {
                                        "firstName": subscriber_from_db.get(
                                            "demographic"
                                        ).get("firstName"),
                                        "middleName": subscriber_from_db.get(
                                            "demographic"
                                        ).get("middleName"),
                                        "lastName": subscriber_from_db.get(
                                            "demographic"
                                        ).get("lastName"),
                                    }
                                ),
                                "insuredEnrollee": {
                                    "referenceId": subscriber_from_db.get(
                                        "insuredEnrollee"
                                    ).get("referenceId"),
                                    "refId": subscriber_from_db.get(
                                        "insuredEnrollee"
                                    ).get("refId"),
                                },
                            }
                            if subscriber_from_db
                            else None
                        )

                        update_patient = (
                            {
                                "refId": patient_from_db.get("_id"),
                                "firstName": patient_from_db.get("demographic").get(
                                    "firstName"
                                ),
                                "middleName": patient_from_db.get("demographic").get(
                                    "middleName"
                                ),
                                "lastName": patient_from_db.get("demographic").get(
                                    "lastName"
                                ),
                                "dob": patient_from_db.get("demographic").get("dob"),
                                "formattedDob": patient_from_db.get("demographic").get(
                                    "formattedDob"
                                ),
                                "relationship": patient_from_db.get("relationship"),
                                "memberId": patient_from_db.get("memberId"),
                                "identificationCode": patient_from_db.get("ssn"),
                                "name": get_name(
                                    {
                                        "firstName": patient_from_db.get(
                                            "demographic"
                                        ).get("firstName"),
                                        "middleName": patient_from_db.get(
                                            "demographic"
                                        ).get("middleName"),
                                        "lastName": patient_from_db.get(
                                            "demographic"
                                        ).get("lastName"),
                                    }
                                ),
                            }
                            if patient_from_db
                            else None
                        )

                        update_invoice_billings_operations.append(
                            UpdateOne(
                                {"invoiceBillingNumber": invoice_billing_number},
                                {
                                    "$set": {
                                        **(
                                            {"enrollee": update_enrollee}
                                            if update_enrollee
                                            else {}
                                        ),
                                        **(
                                            {"subscriber": update_subscriber}
                                            if update_subscriber
                                            else {}
                                        ),
                                        **(
                                            {"patient": update_patient}
                                            if update_patient
                                            else {}
                                        ),
                                    }
                                },
                            )
                        )

                        update_invoice_payment_receipts_operations.append(
                            UpdateMany(
                                {"invoiceNumber": invoice_billing_number},
                                {
                                    "$set": {
                                        **(
                                            {"enrollee": update_enrollee}
                                            if update_enrollee
                                            else {}
                                        ),
                                        **(
                                            {"subscriber": update_subscriber}
                                            if update_subscriber
                                            else {}
                                        ),
                                        **(
                                            {"patient": update_patient}
                                            if update_patient
                                            else {}
                                        ),
                                    }
                                },
                            )
                        )

                        if update_subscriber or update_patient:
                            update_receipt_details_operations.append(
                                UpdateMany(
                                    {"invoiceBillingNumber": invoice_billing_number},
                                    {
                                        "$set": {
                                            **(
                                                {"subscriber": update_subscriber}
                                                if update_subscriber
                                                else {}
                                            ),
                                            **(
                                                {"patient": update_patient}
                                                if update_patient
                                                else {}
                                            ),
                                        }
                                    },
                                )
                            )

                    print(
                        f"🔄 Updating invoice billings operations: {len(update_invoice_billings_operations)}"
                    )
                    print(
                        f"🔄 Updating invoice payment receipts operations: {len(update_invoice_payment_receipts_operations)}"
                    )
                    print(
                        f"🔄 Updating receipt details operations: {len(update_receipt_details_operations)}"
                    )

                    if update_invoice_billings_operations:
                        print(f"🛠️ Updating invoice billings operations")
                        invoiceBillingsModel.get_model().bulk_write(
                            update_invoice_billings_operations
                        )

                    if update_invoice_payment_receipts_operations:
                        print(f"🛠️ Updating invoice payment receipts operations")
                        invoicePaymentReceiptsModel.get_model().bulk_write(
                            update_invoice_payment_receipts_operations
                        )

                    if update_receipt_details_operations:
                        print(f"🛠️ Updating receipt details operations")
                        receiptDetailsModel.get_model().bulk_write(
                            update_receipt_details_operations
                        )

                    print(
                        f"✅ Batch {batch_num + 1} completed in {format_duration(time.perf_counter() - batch_start_time)}"
                    )

                if documentId:
                    documentsModel.get_model().update_one(
                        {"_id": documentId},
                        {"$set": {"status": DocumentStatusEnum.COMPLETED}},
                    )

                print(
                    f"✅[END] Map Enrollee Patient Subscriber From File in {format_duration(time.perf_counter() - file_start_time)}"
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

                print(
                    f"📊 [END] Failed to process file: {file.name} in {format_duration(time.perf_counter() - file_start_time)}"
                )

        print(
            f"✅[END] Map Enrollee Patient Subscriber From File in {format_duration(time.perf_counter() - start_time)}"
        )
