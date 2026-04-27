from src.core.service.eligibility.entity import IArdbEligibility, ITherapyEligibility
from src.core.service.enrollees.entity import ITherapyEnrollee
from src.core.service.patients.entity import ITherapyPatient
from src.core.service.products.entity import ITherapyProduct
from src.core.service.subscribers.entity import ITherapySubscriber
from src.shared.constant.constant import SYSTEM_USER
from src.shared.interface.etl.migration import FileMetadata
from src.shared.utils.date import from_string_to_formatted_date, to_datetime
from src.shared.utils.migration import generate_file_metadata
from src.shared.utils.name import get_name
from src.shared.utils.obj import get_obj_value
from src.shared.utils.qualifiers import resolve_relationship


class EligibilityMapper:
    """Eligibility Mapper"""

    def to_ardb(self, eligibility: ITherapyEligibility) -> IArdbEligibility:
        """Convert the eligibility therapy to ardb format"""

        return {
            "_id": get_obj_value(eligibility, "_id"),
            "hasCompleteInfo": get_obj_value(
                eligibility, "hasCompleteInfo", default=True
            ),
            "ENROLLEE_ID": eligibility["enrollee"]["referenceId"],
            "INSURED_ENROLLEE_ID": eligibility["enrollee"]["referenceId"],
            "PRODUCT_ID": eligibility["product"]["referenceId"],
            "EFFECTIVE_DATE": eligibility["serviceDate"]["startDate"],
            "TERMINATION_DATE": eligibility["serviceDate"]["endDate"],
            "SUBSCRIBER_ID": eligibility["subscriber"]["identificationCode"],
            "BENEFIT_STATUS_CODE": eligibility["benefitStatusCode"],
            "MARITAL_STATUS": eligibility["maritalStatus"],
            "STUDENT_STATUS_CODE": eligibility["studentStatusCode"],
            "HANDICAP_FLAG": eligibility["additionalInformation"]["isHandicapped"],
            "LEVEL_OF_CARE_ID": eligibility["additionalInformation"]["levelOfCareId"],
        }

    def to_therapy(
        self,
        eligibility: IArdbEligibility,
        enrollee: ITherapyEnrollee,
        subscriber: ITherapySubscriber,
        patient: ITherapyPatient,
        product: ITherapyProduct,
        file_metadata: FileMetadata,
    ) -> ITherapyEligibility:
        subscriber_name = get_name(
            {
                "firstName": get_obj_value(subscriber, "demographic", "firstName"),
                "middleName": get_obj_value(subscriber, "demographic", "middleName"),
                "lastName": get_obj_value(subscriber, "demographic", "lastName"),
            }
        )

        paid_through_raw = get_obj_value(eligibility, "PAID_THROUGH_DATE")
        termination_event_raw = get_obj_value(
            eligibility, "TERMINATION_REASON_EVENT_DATE"
        )

        return {
            "_id": get_obj_value(eligibility, "_id"),
            "hasCompleteInfo": get_obj_value(
                eligibility, "hasCompleteInfo", default=True
            ),
            "created": {
                "by": SYSTEM_USER,
                "at": to_datetime(get_obj_value(eligibility, "CREATION_DATE")),
            },
            "updated": {
                "by": SYSTEM_USER,
                "at": to_datetime(
                    get_obj_value(eligibility, "LAST_MODIFIED_DATE_TIME")
                ),
            },
            "enrollee": {
                "referenceId": get_obj_value(enrollee, "referenceId"),
                "refId": get_obj_value(enrollee, "_id"),
            },
            "product": {
                "referenceId": get_obj_value(eligibility, "PRODUCT_ID"),
                "name": get_obj_value(product, "name"),
            },
            "serviceDate": {
                "startDate": to_datetime(get_obj_value(eligibility, "EFFECTIVE_DATE")),
                "formattedStartDate": from_string_to_formatted_date(
                    get_obj_value(eligibility, "EFFECTIVE_DATE")
                ),
                "endDate": to_datetime(get_obj_value(eligibility, "TERMINATION_DATE")),
                "formattedEndDate": from_string_to_formatted_date(
                    get_obj_value(eligibility, "TERMINATION_DATE")
                ),
            },
            "subscriber": {
                "refId": get_obj_value(subscriber, "_id"),
                "identificationCode": get_obj_value(subscriber, "subscriberNumber"),
                "name": subscriber_name,
            },
            "patient": {
                "refId": get_obj_value(patient, "_id"),
                "memberId": get_obj_value(patient, "memberId"),
                "firstName": get_obj_value(patient, "demographic", "firstName"),
                "middleName": get_obj_value(patient, "demographic", "middleName"),
                "lastName": get_obj_value(patient, "demographic", "lastName"),
                "dob": get_obj_value(patient, "demographic", "dob"),
                "formattedDob": get_obj_value(patient, "demographic", "formattedDob"),
                "relationship": get_obj_value(patient, "relationship"),
                "name": get_name(
                    {
                        "firstName": get_obj_value(patient, "demographic", "firstName"),
                        "middleName": get_obj_value(
                            patient, "demographic", "middleName"
                        ),
                        "lastName": get_obj_value(patient, "demographic", "lastName"),
                    }
                ),
                "identificationCode": get_obj_value(patient, "ssn"),
            },
            "maritalStatus": get_obj_value(eligibility, "MARITAL_STATUS"),
            "benefitStatusCode": get_obj_value(eligibility, "BENEFIT_STATUS_CODE"),
            "studentStatusCode": get_obj_value(eligibility, "STUDENT_STATUS_CODE"),
            "additionalInformation": {
                "isHandicapped": get_obj_value(eligibility, "HANDICAP_FLAG"),
                "levelOfCareId": get_obj_value(eligibility, "LEVEL_OF_CARE_ID"),
                "addReasonCode": get_obj_value(eligibility, "ADD_REASON_CODE"),
                "client": {
                    "mcoId": get_obj_value(eligibility, "CLIENT_MCO_ID"),
                    "programCode": get_obj_value(eligibility, "CLIENT_PROGRAM_CODE"),
                    "rateCode": get_obj_value(eligibility, "CLIENT_RATE_CODE"),
                },
                "domainSourceId": get_obj_value(eligibility, "EL_DOMAIN_SOURCE_ID"),
                "isLateEnrollee": get_obj_value(eligibility, "LATE_ENROLLEE_FLAG"),
                "paidThrough": {
                    "date": to_datetime(paid_through_raw),
                    "formattedDate": from_string_to_formatted_date(paid_through_raw),
                    "gracePeriod": get_obj_value(
                        eligibility, "PAID_THROUGH_GRACE_PERIOD"
                    ),
                },
                "terminationReason": {
                    "code": get_obj_value(eligibility, "TERMINATION_REASON_CODE"),
                    "eventDate": to_datetime(termination_event_raw),
                    "formattedEventDate": from_string_to_formatted_date(
                        termination_event_raw
                    ),
                },
                "waitingPeriodCredit": get_obj_value(
                    eligibility, "WAITING_PERIOD_CREDIT"
                ),
                "otherInformation": list(
                    filter(
                        lambda x: x is not None,
                        [
                            get_obj_value(eligibility, "OTHER_INFO1"),
                            get_obj_value(eligibility, "OTHER_INFO2"),
                            get_obj_value(eligibility, "OTHER_INFO3"),
                        ],
                    )
                ),
            },
            "ardbDocuments": [
                generate_file_metadata(file_metadata),
            ],
            "ardbSourceDocument": get_obj_value(file_metadata, "ardb_file_name"),
        }


eligibility_mapper = EligibilityMapper()
