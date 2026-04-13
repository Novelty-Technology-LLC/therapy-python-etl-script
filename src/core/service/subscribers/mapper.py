from src.core.service.enrollees.entity import ITherapyEnrollee
from src.core.service.subscribers.entity import IArdbSubscriber, ITherapySubscriber
from src.shared.interface.etl.migration import FileMetadata
from src.shared.utils.date import from_string_to_formatted_date, to_datetime
from src.shared.utils.gender import to_ardb_gender, to_therapy_gender
from src.shared.utils.migration import generate_file_metadata
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
            "LAST_NAME": get_obj_value(subscriber, "demographic", "lastName"),
            "FIRST_NAME": get_obj_value(subscriber, "demographic", "firstName"),
            "MIDDLE_NAME": get_obj_value(subscriber, "demographic", "middleName"),
            "DOB": get_obj_value(subscriber, "demographic", "dob"),
            "GENDER": to_ardb_gender(
                get_obj_value(subscriber, "demographic", "gender")
            ),
            "EMAIL": get_obj_value(subscriber, "demographic", "email"),
            "ADDRESS1": get_obj_value(
                subscriber, "demographic", "address", "addressLine1"
            ),
            "ADDRESS2": get_obj_value(
                subscriber, "demographic", "address", "addressLine2"
            ),
            "CITY": get_obj_value(subscriber, "demographic", "address", "city"),
            "STATE": get_obj_value(subscriber, "demographic", "address", "state"),
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
                "referenceId": get_obj_value(subscriber, "ENROLLEE_ID"),
                "refId": get_obj_value(subscriber, "enrollee_ref_to_therapy"),
            },
            "insuredEnrollee": {
                "referenceId": get_obj_value(subscriber, "INSURED_ENROLLEE_ID"),
                "refId": get_obj_value(subscriber, "insuredEnrollee_ref_to_therapy"),
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
                "formattedStartDate": from_string_to_formatted_date(effective_date),
                "endDate": to_datetime(termination_date),
                "formattedTerminationDate": from_string_to_formatted_date(termination_date),
            },
            "demographic": {
                "lastName": get_obj_value(subscriber, "LAST_NAME"),
                "firstName": get_obj_value(subscriber, "FIRST_NAME"),
                "middleName": get_obj_value(subscriber, "MIDDLE_NAME"),
                "dob": to_datetime(enrollee_dob),
                "formattedDob": from_string_to_formatted_date(enrollee_dob),
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
        enrollee_dob = get_obj_value(enrollee, "demographic", "dob")
        effective_date = get_obj_value(subscriber, "EFFECTIVE_DATE")
        termination_date = get_obj_value(subscriber, "TERMINATION_DATE")
        return {
            "_id": get_obj_value(subscriber, "_id"),
            "hasCompleteInfo": get_obj_value(
                subscriber, "hasCompleteInfo", default=True
            ),
            "created": {
                "by": "system",
                "at": to_datetime(get_obj_value(subscriber, "CREATION_DATE")),
            },
            "updated": {
                "by": "system",
                "at": to_datetime(get_obj_value(subscriber, "LAST_MODIFIED_DATE_TIME")),
            },
            "enrollee": {
                "referenceId": get_obj_value(enrollee, "referenceId"),
                "refId": get_obj_value(enrollee, "_id"),
            },
            "insuredEnrollee": {
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
                "formattedStartDate": from_string_to_formatted_date(effective_date),
                "endDate": to_datetime(termination_date),
                "formattedTerminationDate": from_string_to_formatted_date(termination_date),
            },
            "demographic": {
                "lastName": get_obj_value(enrollee, "demographic", "lastName"),
                "firstName": get_obj_value(enrollee, "demographic", "firstName"),
                "middleName": get_obj_value(enrollee, "demographic", "middleName"),
                "dob": to_datetime(enrollee_dob),
                "formattedDob": from_string_to_formatted_date(enrollee_dob),
                "gender": get_obj_value(enrollee, "demographic", "gender"),
                "email": get_obj_value(enrollee, "demographic", "email"),
                "phone": get_obj_value(enrollee, "demographic", "phone"),
                "address": {
                    "addressLine1": get_obj_value(
                        enrollee, "demographic", "address", "addressLine1"
                    ),
                    "addressLine2": get_obj_value(
                        enrollee, "demographic", "address", "addressLine2"
                    ),
                    "city": get_obj_value(enrollee, "demographic", "address", "city"),
                    "state": get_obj_value(enrollee, "demographic", "address", "state"),
                    "zipCode": get_obj_value(
                        enrollee, "demographic", "address", "zipCode"
                    ),
                    "zipCode4": get_obj_value(
                        enrollee, "demographic", "address", "zipCode4"
                    ),
                },
            },
            "policyNumber": get_obj_value(subscriber, "POLICY_NUMBER"),
            "histories": [],
            "ardbDocuments": [
                generate_file_metadata(file_metadata),
            ],
            "ardbSourceDocument": get_obj_value(file_metadata, "ardb_file_name"),
            "ardbLastModifiedDate": get_obj_value(
                file_metadata, "ardb_file_processed_at"
            ),
        }


subscriber_mapper = SubscriberMapper()
