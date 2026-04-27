from src.core.service.enrollees.entity import IArdbEnrollee, ITherapyEnrollee
from src.shared.constant.constant import SYSTEM_USER
from src.shared.interface.etl.migration import FileMetadata
from src.shared.utils.date import from_string_to_formatted_date, to_datetime
from src.shared.utils.gender import to_ardb_gender, to_therapy_gender
from src.shared.utils.migration import generate_file_metadata
from src.shared.utils.obj import get_obj_value


class EnrolleeMapper:
    """Enrollee Mapper"""

    def to_ardb(self, enrollee: ITherapyEnrollee) -> IArdbEnrollee:
        """Convert the enrollee therapy to ardb format"""
        return {
            "_id": get_obj_value(enrollee, "_id"),
            "hasCompleteInfo": get_obj_value(enrollee, "hasCompleteInfo", default=True),
            "ENROLLEE_ID": get_obj_value(enrollee, "referenceId"),
            "LAST_NAME": get_obj_value(enrollee, "demographic", "lastName"),
            "FIRST_NAME": get_obj_value(enrollee, "demographic", "firstName"),
            "MIDDLE_NAME": get_obj_value(enrollee, "demographic", "middleName"),
            "DOB": get_obj_value(enrollee, "demographic", "dob"),
            "GENDER": to_ardb_gender(get_obj_value(enrollee, "demographic", "gender")),
            "EMAIL": get_obj_value(enrollee, "demographic", "email"),
            "SS_NUMBER": get_obj_value(enrollee, "demographic", "ssn"),
            "ADDRESS1": get_obj_value(
                enrollee, "demographic", "address", "addressLine1"
            ),
            "ADDRESS2": get_obj_value(
                enrollee, "demographic", "address", "addressLine2"
            ),
            "CITY": get_obj_value(enrollee, "demographic", "address", "city"),
            "STATE": get_obj_value(enrollee, "demographic", "address", "state"),
            "ZIP": get_obj_value(enrollee, "demographic", "address", "zipCode"),
            "ZIP_4": get_obj_value(enrollee, "demographic", "address", "zipCode4"),
            "NAME_PREFIX": get_obj_value(
                enrollee, "additionalInformation", "namePrefix"
            ),
            "NAME_SUFFIX": get_obj_value(
                enrollee, "additionalInformation", "nameSuffix"
            ),
            "COUNTY_ID": get_obj_value(enrollee, "additionalInformation", "countyId"),
            "ADDRESS_CODE": get_obj_value(
                enrollee, "additionalInformation", "addressCode"
            ),
            "BIRTH_SEQUENCE": get_obj_value(
                enrollee, "additionalInformation", "birthSequence"
            ),
            "DEATH_DATE": get_obj_value(enrollee, "additionalInformation", "deathDate"),
            "ETHNICITY_CODE": get_obj_value(
                enrollee, "additionalInformation", "ethnicityCode"
            ),
            "CITIZENSHIP_STATUS_CODE": get_obj_value(
                enrollee, "additionalInformation", "citizenshipStatusCode"
            ),
            "RACE_ETHNICITY_CODE": get_obj_value(
                enrollee, "additionalInformation", "raceEthnicityCode"
            ),
            "PREFERRED_CONTACT_METHOD": get_obj_value(
                enrollee, "communicationPreference", "preferredContactMethod"
            ),
            "EOB_COMMUNICATION_METHOD": get_obj_value(
                enrollee, "communicationPreference", "eobCommunicationMethod"
            ),
            "ID_CARD_COMMUNICATION_METHOD": get_obj_value(
                enrollee, "communicationPreference", "idCardCommunicationMethod"
            ),
            "LETTER_COMMUNICATION_METHOD": get_obj_value(
                enrollee, "communicationPreference", "letterCommunicationMethod"
            ),
            "CREATION_DATE": (
                None
                if get_obj_value(enrollee, "created", "at") is None
                else get_obj_value(enrollee, "created", "at")
            ),
            "LAST_MODIFIED_DATE_TIME": (
                None
                if get_obj_value(enrollee, "updated", "at") is None
                else get_obj_value(enrollee, "updated", "at")
            ),
        }

    def to_therapy(
        self, enrollee: IArdbEnrollee, file_metadata: FileMetadata
    ) -> ITherapyEnrollee:
        """Convert the enrollee ardb to therapy format"""

        enrollee_dob = get_obj_value(enrollee, "DOB")
        death_date = get_obj_value(enrollee, "DEATH_DATE")

        return {
            "_id": get_obj_value(enrollee, "_id"),
            "hasCompleteInfo": get_obj_value(enrollee, "hasCompleteInfo", default=True),
            "created": {
                "by": SYSTEM_USER,
                "at": to_datetime(get_obj_value(enrollee, "CREATION_DATE")),
            },
            "updated": {
                "by": SYSTEM_USER,
                "at": to_datetime(get_obj_value(enrollee, "LAST_MODIFIED_DATE_TIME")),
            },
            "demographic": {
                "firstName": get_obj_value(enrollee, "FIRST_NAME"),
                "middleName": get_obj_value(enrollee, "MIDDLE_NAME"),
                "lastName": get_obj_value(enrollee, "LAST_NAME"),
                "gender": to_therapy_gender(get_obj_value(enrollee, "GENDER")),
                "dob": to_datetime(enrollee_dob),
                "formattedDob": from_string_to_formatted_date(enrollee_dob),
                "email": get_obj_value(enrollee, "EMAIL"),
                "phone": get_obj_value(enrollee, "PHONE_NUMBER"),
                "address": {
                    "addressLine1": get_obj_value(enrollee, "ADDRESS1"),
                    "addressLine2": get_obj_value(enrollee, "ADDRESS2"),
                    "city": get_obj_value(enrollee, "CITY"),
                    "state": get_obj_value(enrollee, "STATE"),
                    "zipCode": get_obj_value(enrollee, "ZIP"),
                    "zipCode4": get_obj_value(enrollee, "ZIP_4"),
                },
            },
            "ssn": get_obj_value(enrollee, "SS_NUMBER"),
            "referenceId": get_obj_value(enrollee, "ENROLLEE_ID"),
            "additionalInformation": {
                "namePrefix": get_obj_value(enrollee, "NAME_PREFIX"),
                "nameSuffix": get_obj_value(enrollee, "NAME_SUFFIX"),
                "countyId": get_obj_value(enrollee, "COUNTY_ID"),
                "addressCode": get_obj_value(enrollee, "ADDRESS_CODE"),
                "birthSequence": get_obj_value(enrollee, "BIRTH_SEQUENCE"),
                "deathDate": to_datetime(death_date),
                "formattedDeathDate": from_string_to_formatted_date(death_date),
                "ethnicityCode": get_obj_value(enrollee, "ETHNICITY_CODE"),
                "raceEthnicityCode": get_obj_value(enrollee, "RACE_ETHNICITY_CODE"),
                "citizenshipStatusCode": get_obj_value(
                    enrollee, "CITIZENSHIP_STATUS_CODE"
                ),
                "isPregnant": get_obj_value(enrollee, "IS_PREGNANT"),
                "domainSourceId": get_obj_value(enrollee, "DOMAIN_SOURCE_ID"),
                "weight": {
                    "value": get_obj_value(enrollee, "WEIGHT_VALUE"),
                    "unit": get_obj_value(enrollee, "WEIGHT_UNIT"),
                },
            },
            "communicationPreference": {
                "preferredContactMethod": get_obj_value(
                    enrollee, "PREFERRED_CONTACT_METHOD"
                ),
                "eobCommunicationMethod": get_obj_value(
                    enrollee, "EOB_COMMUNICATION_METHOD"
                ),
                "idCardCommunicationMethod": get_obj_value(
                    enrollee, "ID_CARD_COMMUNICATION_METHOD"
                ),
                "letterCommunicationMethod": get_obj_value(
                    enrollee, "LETTER_COMMUNICATION_METHOD"
                ),
            },
            "ardbDocuments": [
                generate_file_metadata(file_metadata),
            ],
            "ardbSourceDocument": get_obj_value(file_metadata, "ardb_file_name"),
            "ardbLastModifiedDate": get_obj_value(
                file_metadata, "ardb_file_processed_at"
            ),
            "histories": [],
        }


enrollee_mapper = EnrolleeMapper()
