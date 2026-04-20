import time
from typing import TypedDict
from pymongo import ASCENDING, UpdateMany, UpdateOne
from src.core.migrate.base_etl import BaseEtl
from src.core.migrate.script.interface.invoice_billing_map_enrollee import (
    IPatientQuery,
    ISubscriberQuery,
)
from src.core.service.ardb_dump_invoice_billings.entity import IArdbDumpInvoiceBilling
from src.core.service.ardb_dump_invoice_billings.model import (
    ardbDumpInvoiceBillingsModel,
)
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
from src.shared.utils.batch import get_total_batch_count
from src.shared.utils.date import format_duration
from src.shared.utils.name import get_name


class InvoiceBillingMapEnrolleeSubscriberPatientPatch(BaseEtl):
    def __init__(self):
        super().__init__()
        self.batch_size = BATCH_SIZE
        self.last_visited_batch_id = "0194bbdc-e00d-7041-a465-52402b5e2348"

    def execute(self):
        print("🔄 Invoice Billing Map Enrollee Subscriber Patient Patch")
        etl_start_time = time.perf_counter()
        count_query = [
            {
                "$match": {
                    **(
                        {"_id": {"$gt": self.last_visited_batch_id}}
                        if self.last_visited_batch_id
                        else {}
                    ),
                    "enrollee.refId": {"$exists": True},
                }
            },
            # {
            #     "$lookup": {
            #         "from": "PYTHON_TEST_ENROLLEE",
            #         "localField": "enrollee.refId",
            #         "foreignField": "_id",
            #         "as": "enrollees",
            #         "pipeline": [{"$project": {"_id": 1, "references": 1}}],
            #     }
            # },
            # {"$match": {"$expr": {"$eq": [{"$size": "$enrollees"}, 0]}}},
            # {"$project": {"_id": 1, "enrollee": 1, "enrollees": 1}},
            {"$count": "count"},
        ]
        # total_count = invoiceBillingsModel.get_model().count_documents(filter={})
        total_count = list(invoiceBillingsModel.get_model().aggregate(count_query))
        total_count = total_count[0].get("count") if total_count else 0
        total_batches = get_total_batch_count(total_count, self.batch_size)
        print(f"📦 Batch Size: {self.batch_size}")
        print(f"📋 Total Invoice Billings: {total_count}")
        print(f"📦 Total batches: {total_batches}")

        # last_visited_batch_id = None

        for batch_num in range(total_batches):
            batch_start_time = time.perf_counter()
            print(f"⏳ Processing batch {batch_num + 1} of {total_batches}")

            query = (
                {"_id": {"$gt": self.last_visited_batch_id}}
                if self.last_visited_batch_id
                else {}
            )

            base_query = [
                {"$match": {**query, "enrollee.refId": {"$exists": True}}},
                {
                    "$sort": {
                        "_id": ASCENDING,
                    }
                },
                # {
                #     "$lookup": {
                #         "from": "PYTHON_TEST_ENROLLEE",
                #         "localField": "enrollee.redId",
                #         "foreignField": "_id",
                #         "as": "enrollees",
                #         "pipeline": [{"$project": {"_id": 1, "references": 1}}],
                #     }
                # },
                # {"$match": {"$expr": {"$eq": [{"$size": "$enrollees"}, 0]}}},
                {
                    "$limit": self.batch_size,
                },
                {
                    "$project": {
                        "_id": 1,
                        "invoiceBillingNumber": 1,
                    }
                },
            ]

            invoice_billings_from_db = list(
                invoiceBillingsModel.get_model().aggregate(pipeline=base_query)
            )

            print(f"🔄 Invoice Billings from DB: {len(invoice_billings_from_db)}")

            if len(invoice_billings_from_db) <= 0:
                break

            self.last_visited_batch_id = invoice_billings_from_db[-1]["_id"]
            print(
                f"🔄 Last visited batch ID: {self.last_visited_batch_id} with invoice billing number: {invoice_billings_from_db[-1]['invoiceBillingNumber']}"
            )

            invoice_billing_numbers = set[str]()

            for invoice_billing in invoice_billings_from_db:
                if invoice_billing.get("invoiceBillingNumber"):
                    invoice_billing_numbers.add(
                        invoice_billing.get("invoiceBillingNumber")
                    )

            print(
                f"🔄 Total Unique Invoice Billing Numbers: {len(invoice_billing_numbers)}"
            )

            # fetch ARDB dump invoice billings
            print(f"🔄 Fetching ARDB dump invoice billings from DB")
            ardb_dump_invoice_billings_from_db = (
                list[IArdbDumpInvoiceBilling](
                    ardbDumpInvoiceBillingsModel.get_model().find(
                        # filter={
                        #     "INVOICE_BILLING_ID": {"$in": list(invoice_billing_numbers)}
                        # },
                        filter={
                            "$expr": {
                                "$in": [
                                    {"$toString": "$INVOICE_BILLING_ID"},
                                    list(invoice_billing_numbers),
                                ]
                            }
                        },
                        projection={
                            "_id": 1,
                            "INVOICE_BILLING_ID": {"$toString": "$INVOICE_BILLING_ID"},
                            "ENROLLEE_ID": {"$toString": "$ENROLLEE_ID"},
                            "INSURED_ENROLLEE_ID": {
                                "$toString": "$INSURED_ENROLLEE_ID"
                            },
                            "SUBSCRIBER_ID": {"$toString": "$SUBSCRIBER_ID"},
                            "MEMBER_ID": {"$toString": "$MEMBER_ID"},
                        },
                    )
                )
                if invoice_billing_numbers
                else []
            )

            if not ardb_dump_invoice_billings_from_db:
                continue

            enrollee_ids = set[str]()
            insured_enrollee_ids = set[str]()
            ardb_dump_invoice_billing_map: dict[str, dict] = (
                {}
            )  # key => invoiceBillingNumber, value => ardb_dump_invoice_billing

            subscriber_query: list[ISubscriberQuery] = []
            patient_query: list[IPatientQuery] = []

            for ardb_dump_invoice_billing in ardb_dump_invoice_billings_from_db:
                enrollee_id = ardb_dump_invoice_billing.get("ENROLLEE_ID")
                insured_enrollee_id = ardb_dump_invoice_billing.get(
                    "INSURED_ENROLLEE_ID"
                )
                subscriber_id = ardb_dump_invoice_billing.get("SUBSCRIBER_ID")
                member_id = ardb_dump_invoice_billing.get("MEMBER_ID")
                invoice_billing_number = ardb_dump_invoice_billing.get(
                    "INVOICE_BILLING_ID"
                )

                if enrollee_id:
                    enrollee_ids.add(enrollee_id)

                if insured_enrollee_id:
                    insured_enrollee_ids.add(insured_enrollee_id)

                if invoice_billing_number:
                    ardb_dump_invoice_billing_map[invoice_billing_number] = (
                        ardb_dump_invoice_billing
                    )

                if subscriber_id and insured_enrollee_id:
                    subscriber_query.append(
                        {
                            "subscriberNumber": subscriber_id,
                            "insuredEnrollee.referenceId": insured_enrollee_id,
                        }
                    )

                if enrollee_id and subscriber_id and member_id:
                    patient_query.append(
                        {
                            "enrollee.referenceId": enrollee_id,
                            "subscriber.identificationCode": subscriber_id,
                            "memberId": member_id,
                        }
                    )

            # fetch enrollees
            enrollees_from_db = (
                list[ITherapyEnrollee](
                    enrolleesModel.get_model().find(
                        filter={"referenceId": {"$in": list(enrollee_ids)}},
                        projection={
                            "_id": 1,
                            "referenceId": 1,
                        },
                    )
                )
                if enrollee_ids
                else []
            )

            # fetch subscribers
            subscribers_from_db = (
                list[ITherapySubscriber](
                    subscribersModel.get_model().find(
                        filter={"$or": subscriber_query},
                        projection={
                            "_id": 1,
                            "referenceId": 1,
                            "subscriberNumber": 1,
                            "insuredEnrollee": 1,
                            "demographic": {
                                "firstName": 1,
                                "middleName": 1,
                                "lastName": 1,
                            },
                        },
                    )
                )
                if subscriber_query
                else []
            )

            # fetch patients
            patients_from_db = (
                list[ITherapyPatient](
                    patientsModel.get_model().find(
                        filter={"$or": patient_query},
                        projection={
                            "_id": 1,
                            "enrollee": 1,
                            "subscriber": {
                                "identificationCode": 1,
                            },
                            "demographic": {
                                "firstName": 1,
                                "middleName": 1,
                                "lastName": 1,
                                "dob": 1,
                                "formattedDob": 1,
                            },
                            "relationship": 1,
                            "memberId": 1,
                            "ssn": 1,
                        },
                    )
                )
                if patient_query
                else []
            )

            # variables to collect db operations
            update_invoice_billings_operations: list[UpdateOne] = []
            update_invoice_payment_receipts_operations: list[UpdateMany] = []
            update_receipt_details_operations: list[UpdateMany] = []

            for invoice_billing in invoice_billings_from_db:
                invoice_billing_number = invoice_billing.get("invoiceBillingNumber")

                if not invoice_billing_number:
                    continue

                ardb_dump_invoice_billing = ardb_dump_invoice_billing_map.get(
                    invoice_billing_number
                )

                if not ardb_dump_invoice_billing:
                    continue

                enrollee_id_from_ardb = ardb_dump_invoice_billing.get("ENROLLEE_ID")
                insured_enrollee_id_from_ardb = ardb_dump_invoice_billing.get(
                    "INSURED_ENROLLEE_ID"
                )
                subscriber_number_from_ardb = ardb_dump_invoice_billing.get(
                    "SUBSCRIBER_ID"
                )
                member_id_from_ardb = ardb_dump_invoice_billing.get("MEMBER_ID")

                enrollee_from_db = next(
                    (
                        enrollee
                        for enrollee in enrollees_from_db
                        if enrollee.get("referenceId") == enrollee_id_from_ardb
                    ),
                    None,
                )
                subscriber_from_db = next(
                    (
                        subscriber
                        for subscriber in subscribers_from_db
                        if subscriber.get("subscriberNumber")
                        == subscriber_number_from_ardb
                        and subscriber.get("insuredEnrollee").get("referenceId")
                        == insured_enrollee_id_from_ardb
                    ),
                    None,
                )
                patient_from_db = next(
                    (
                        patient
                        for patient in patients_from_db
                        if patient.get("enrollee").get("referenceId")
                        == enrollee_id_from_ardb
                        and patient.get("subscriber").get("identificationCode")
                        == subscriber_number_from_ardb
                        and patient.get("memberId") == member_id_from_ardb
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
                                "firstName": subscriber_from_db.get("demographic").get(
                                    "firstName"
                                ),
                                "middleName": subscriber_from_db.get("demographic").get(
                                    "middleName"
                                ),
                                "lastName": subscriber_from_db.get("demographic").get(
                                    "lastName"
                                ),
                            }
                        ),
                        "insuredEnrollee": {
                            "referenceId": subscriber_from_db.get(
                                "insuredEnrollee"
                            ).get("referenceId"),
                            "refId": subscriber_from_db.get("insuredEnrollee").get(
                                "refId"
                            ),
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
                        "lastName": patient_from_db.get("demographic").get("lastName"),
                        "dob": patient_from_db.get("demographic").get("dob"),
                        "formattedDob": patient_from_db.get("demographic").get(
                            "formattedDob"
                        ),
                        "relationship": patient_from_db.get("relationship"),
                        "memberId": patient_from_db.get("memberId"),
                        "identificationCode": patient_from_db.get("ssn"),
                        "name": get_name(
                            {
                                "firstName": patient_from_db.get("demographic").get(
                                    "firstName"
                                ),
                                "middleName": patient_from_db.get("demographic").get(
                                    "middleName"
                                ),
                                "lastName": patient_from_db.get("demographic").get(
                                    "lastName"
                                ),
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

        print(
            f"✅ ETL completed in {format_duration(time.perf_counter() - etl_start_time)}"
        )
