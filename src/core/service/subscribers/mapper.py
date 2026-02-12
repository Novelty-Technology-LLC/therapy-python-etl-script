from src.core.service.enrollees.entity import ITherapyEnrollee
from src.core.service.subscribers.entity import IArdbSubscriber, ITherapySubscriber
from src.shared.interface.etl.migration import FileMetadata
from src.shared.utils.date import to_datetime
from src.shared.utils.gender import to_ardb_gender, to_therapy_gender
from src.shared.utils.obj import get_obj_value


class SubscriberMapper:
    """Subscriber Mapper"""

    def to_ardb(self, subscriber: ITherapySubscriber) -> IArdbSubscriber:
        """Convert the subscriber therapy to ardb format"""
        return {
            "_id": get_obj_value(subscriber, "_id"),
            "hasCompleteInfo": get_obj_value(
                subscriber, "hasCompleteInfo", default=True
            ),
            "EL_ENROLLEE_ID": get_obj_value(subscriber, "enrollee", "referenceId"),
            "EL_INSURED_ENROLLEE_ID": get_obj_value(
                subscriber, "enrollee", "referenceId"
            ),
            "SUBSCRIBER_ID": get_obj_value(subscriber, "subscriberNumber"),
            "PREMIUM_GROUP_ID": get_obj_value(
                subscriber, "premiumGroup", "referenceId"
            ),
            "PREMIUM_GROUP_DEPARTMENT_ID": get_obj_value(
                subscriber, "premiumGroup", "department", "referenceId"
            ),
            "EMPLOYMENT_STATUS": get_obj_value(subscriber, "employment", "status"),
            "EFFECTIVE_DATE": get_obj_value(subscriber, "employment", "startDate"),
            "TERMINATION_DATE": get_obj_value(subscriber, "employment", "endDate"),
            "LAST_NAME": get_obj_value(subscriber, "lastName"),
            "FIRST_NAME": get_obj_value(subscriber, "firstName"),
            "MIDDLE_NAME": get_obj_value(subscriber, "middleName"),
            "DOB": get_obj_value(subscriber, "dob"),
            "GENDER": to_ardb_gender(get_obj_value(subscriber, "gender")),
            "EMAIL": get_obj_value(subscriber, "email"),
            "ADDRESS1": get_obj_value(subscriber, "address", "addressLine1"),
            "ADDRESS2": get_obj_value(subscriber, "address", "addressLine2"),
            "CITY": get_obj_value(subscriber, "address", "city"),
            "STATE": get_obj_value(subscriber, "address", "state"),
            "ZIP": get_obj_value(subscriber, "address", "zipCode"),
            "ZIP_4": get_obj_value(subscriber, "address", "zipCode4"),
        }

    def to_therapy(
        self,
        subscriber: IArdbSubscriber,
        file_metadata: FileMetadata,
    ) -> ITherapySubscriber:
        """Convert the subscriber ardb to therapy format"""

        enrollee_dob = get_obj_value(subscriber, "DOB")
        effective_date = get_obj_value(subscriber, "EFFECTIVE_DATE")
        termination_date = get_obj_value(subscriber, "TERMINATION_DATE")
        return {
            "_id": get_obj_value(subscriber, "_id"),
            "hasCompleteInfo": get_obj_value(
                subscriber, "hasCompleteInfo", default=True
            ),
            "created": {
                "by": "system",
                "at": to_datetime(get_obj_value(subscriber, "EN_CREATION_DATE")),
            },
            "updated": {
                "by": "system",
                "at": to_datetime(
                    get_obj_value(subscriber, "EN_LAST_MODIFIED_DATE_TIME")
                ),
            },
            "enrollee": {
                "referenceId": get_obj_value(subscriber, "EN_ENROLLEE_ID"),
                "refId": get_obj_value(subscriber, "enrollee_ref_to_therapy"),
            },
            "subscriberNumber": get_obj_value(subscriber, "SUBSCRIBER_ID"),
            "premiumGroup": {
                "referenceId": get_obj_value(subscriber, "PREMIUM_GROUP_ID"),
                "department": {
                    "referenceId": get_obj_value(
                        subscriber, "PREMIUM_GROUP_DEPARTMENT_ID"
                    ),
                },
            },
            "employment": {
                "status": get_obj_value(subscriber, "EMPLOYMENT_STATUS"),
                "startDate": to_datetime(effective_date),
                "formattedStartDate": effective_date,
                "endDate": to_datetime(termination_date),
                "formattedTerminationDate": termination_date,
            },
            "lastName": get_obj_value(subscriber, "LAST_NAME"),
            "firstName": get_obj_value(subscriber, "FIRST_NAME"),
            "middleName": get_obj_value(subscriber, "MIDDLE_NAME"),
            "dob": to_datetime(enrollee_dob),
            "formattedDob": enrollee_dob,
            "gender": to_therapy_gender(get_obj_value(subscriber, "GENDER")),
            "email": get_obj_value(subscriber, "EMAIL"),
            "phone": get_obj_value(subscriber, "PHONE_NUMBER"),
            "address": {
                "addressLine1": get_obj_value(subscriber, "ADDRESS1"),
                "addressLine2": get_obj_value(subscriber, "ADDRESS2"),
                "city": get_obj_value(subscriber, "CITY"),
                "state": get_obj_value(subscriber, "STATE"),
                "zipCode": get_obj_value(subscriber, "ZIP"),
                "zipCode4": get_obj_value(subscriber, "ZIP_4"),
            },
            "policyNumber": get_obj_value(subscriber, "POLICY_NUMBER"),
            "histories": [],
            "ardbDocuments": [
                {
                    "refId": None,
                    "fileName": get_obj_value(file_metadata, "ardb_file_name"),
                    "filePath": get_obj_value(file_metadata, "ardb_file_path"),
                    "isReconciled": False,
                    "updatedAt": get_obj_value(file_metadata, "ardb_file_processed_at"),
                }
            ],
            "ardbSourceDocument": get_obj_value(file_metadata, "ardb_file_name"),
            "ardbLastModifiedDate": get_obj_value(
                file_metadata, "ardb_file_processed_at"
            ),
        }

    def to_therapy_subscriber_enrollee(
        self,
        subscriber: IArdbSubscriber,
        enrollee: ITherapyEnrollee,
        file_metadata: FileMetadata,
    ) -> ITherapySubscriber:
        enrollee_dob = get_obj_value(enrollee, "dob")
        effective_date = get_obj_value(subscriber, "EFFECTIVE_DATE")
        termination_date = get_obj_value(subscriber, "TERMINATION_DATE")
        return {
            "_id": get_obj_value(subscriber, "_id"),
            "hasCompleteInfo": get_obj_value(
                subscriber, "hasCompleteInfo", default=True
            ),
            "created": {
                "by": "system",
                "at": to_datetime(get_obj_value(subscriber, "EN_CREATION_DATE")),
            },
            "updated": {
                "by": "system",
                "at": to_datetime(
                    get_obj_value(subscriber, "EN_LAST_MODIFIED_DATE_TIME")
                ),
            },
            "enrollee": {
                "referenceId": get_obj_value(enrollee, "referenceId"),
                "refId": get_obj_value(enrollee, "_id"),
            },
            "subscriberNumber": get_obj_value(subscriber, "SUBSCRIBER_ID"),
            "premiumGroup": {
                "referenceId": get_obj_value(subscriber, "PREMIUM_GROUP_ID"),
                "department": {
                    "referenceId": get_obj_value(
                        subscriber, "PREMIUM_GROUP_DEPARTMENT_ID"
                    ),
                },
            },
            "employment": {
                "status": get_obj_value(subscriber, "EMPLOYMENT_STATUS"),
                "startDate": to_datetime(effective_date),
                "formattedStartDate": effective_date,
                "endDate": to_datetime(termination_date),
                "formattedTerminationDate": termination_date,
            },
            "lastName": get_obj_value(enrollee, "lastName"),
            "firstName": get_obj_value(enrollee, "firstName"),
            "middleName": get_obj_value(enrollee, "middleName"),
            "dob": to_datetime(enrollee_dob),
            "formattedDob": enrollee_dob,
            "gender": get_obj_value(enrollee, "gender"),
            "email": get_obj_value(enrollee, "email"),
            "phone": get_obj_value(enrollee, "phone"),
            "address": {
                "addressLine1": get_obj_value(enrollee, "address", "addressLine1"),
                "addressLine2": get_obj_value(enrollee, "address", "addressLine2"),
                "city": get_obj_value(enrollee, "address", "city"),
                "state": get_obj_value(enrollee, "address", "state"),
                "zipCode": get_obj_value(enrollee, "address", "zipCode"),
                "zipCode4": get_obj_value(enrollee, "address", "zipCode4"),
            },
            "policyNumber": get_obj_value(subscriber, "POLICY_NUMBER"),
            "histories": [],
            "ardbDocuments": [
                {
                    "refId": None,
                    "fileName": get_obj_value(file_metadata, "ardb_file_name"),
                    "filePath": get_obj_value(file_metadata, "ardb_file_path"),
                    "isReconciled": False,
                    "updatedAt": get_obj_value(file_metadata, "ardb_file_processed_at"),
                }
            ],
            "ardbSourceDocument": get_obj_value(file_metadata, "ardb_file_name"),
            "ardbLastModifiedDate": get_obj_value(
                file_metadata, "ardb_file_processed_at"
            ),
        }


subscriber_mapper = SubscriberMapper()
