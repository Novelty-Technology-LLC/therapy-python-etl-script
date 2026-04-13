from datetime import date, datetime
from typing import Optional, TypedDict

from src.shared.interface.common import (
    IARDBDocumentReference,
    IBaseEntity,
    IHistory,
    IOldSystemReference,
)
from src.shared.interface.entity import IMemberDemographic


class IArdbEnrollee(TypedDict):
    EN_ENROLLEE_ID: str

    LAST_NAME: str
    FIRST_NAME: str
    MIDDLE_NAME: str
    DOB: str
    GENDER: str
    EMAIL: str
    SS_NUMBER: str

    ADDRESS1: str
    ADDRESS2: str
    CITY: str
    STATE: str
    ZIP: str
    ZIP_4: str

    NAME_PREFIX: str
    NAME_SUFFIX: str
    COUNTY_ID: str
    ADDRESS_CODE: str
    BIRTH_SEQUENCE: str
    DEATH_DATE: str
    ETHNICITY_CODE: str
    CITIZENSHIP_STATUS_CODE: str
    RACE_ETHNICITY_CODE: str

    PREFERRED_CONTACT_METHOD: str
    EOB_COMMUNICATION_METHOD: str
    ID_CARD_COMMUNICATION_METHOD: str
    LETTER_COMMUNICATION_METHOD: str

    EN_CREATION_DATE: str
    EN_LAST_MODIFIED_DATE_TIME: str


class ITherapyEnrolleeAdditionalInformationWeight(TypedDict, total=False):
    """Weight information"""

    value: Optional[int]  # or Optional[float] if decimal values
    unit: str


class ITherapyEnrolleeAdditionalInformation(TypedDict, total=False):
    namePrefix: Optional[str]
    nameSuffix: Optional[str]

    countyId: Optional[str]
    domainSourceId: Optional[str]
    birthSequence: Optional[str]

    addressCode: Optional[str]
    ethnicityCode: Optional[str]
    raceEthnicityCode: Optional[str]
    citizenshipStatusCode: Optional[str]

    isPregnant: Optional[bool]
    weight: ITherapyEnrolleeAdditionalInformationWeight

    deathDate: Optional[date]
    formattedDeathDate: Optional[str]


class ITherapyEnrolleeCommunicationPreference(TypedDict, total=False):
    preferredContactMethod: Optional[str]
    eobCommunicationMethod: Optional[str]
    idCardCommunicationMethod: Optional[str]
    letterCommunicationMethod: Optional[str]


class ITherapyEnrolleeDemographic(TypedDict, total=False):
    demographic: IMemberDemographic


class ITherapyEnrollee(
    IBaseEntity,
    IOldSystemReference,
    IARDBDocumentReference,
    ITherapyEnrolleeDemographic,
    IHistory[ITherapyEnrolleeDemographic],
):
    additionalInformation: Optional[ITherapyEnrolleeAdditionalInformation]
    communicationPreference: Optional[ITherapyEnrolleeCommunicationPreference]


enrollee: ITherapyEnrollee = {
    "_id": "1",
    "created": {"by": "system", "at": datetime.now()},
    "updated": {"by": "system", "at": datetime.now()},
    "firstName": "John",
    "middleName": "Doe",
    "lastName": "Smith",
    "gender": "Male",
    "dob": datetime.now(),
    "email": "john.doe@example.com",
    "phone": "1234567890",
    "address": {
        "addressLine1": "123 Main St",
        "addressLine2": "Apt 1",
        "city": "Anytown",
        "state": "CA",
        "zipCode": "12345",
        "zipCode4": "1234",
    },
    "referenceId": "1234567890",
    "additionalInformation": {
        "namePrefix": "Mr.",
        "nameSuffix": "Jr.",
        "countyId": "1234567890",
        "domainSourceId": "1234567890",
        "birthSequence": "1234567890",
        "addressCode": "1234567890",
        "ethnicityCode": "1234567890",
        "raceEthnicityCode": "1234567890",
        "citizenshipStatusCode": "1234567890",
        "isPregnant": True,
        "weight": {"value": 100, "unit": "kg"},
        "deathDate": datetime.now(),
    },
    "communicationPreference": {
        "preferredContactMethod": "email",
        "eobCommunicationMethod": "email",
        "idCardCommunicationMethod": "email",
        "letterCommunicationMethod": "email",
    },
    "ardbDocuments": [
        {
            "refId": "1234567890",
            "fileName": "1234567890",
            "filePath": "1234567890",
            "isReconciled": False,
        }
    ],
    "ardbSourceDocument": "1234567890",
    "ardbLastModifiedDate": datetime.now(),
    "histories": [
        {
            "updated": {"by": "system", "at": datetime.now()},
            "history": {
                "firstName": "John",
                "middleName": "Doe",
                "lastName": "Smith",
                "gender": "Male",
                "dob": datetime.now(),
                "email": "john.doe@example.com",
                "phone": "1234567890",
                "address": {
                    "addressLine1": "123 Main St",
                    "addressLine2": "Apt 1",
                    "city": "Anytown",
                    "state": "CA",
                    "zipCode": "12345",
                    "zipCode4": "1234",
                },
            },
        }
    ],
}

# print(enrollee)
