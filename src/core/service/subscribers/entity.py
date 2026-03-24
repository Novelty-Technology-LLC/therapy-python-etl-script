from datetime import datetime
from typing import Optional, TypedDict

from src.shared.interface.common import (
    IARDBDocumentReference,
    IBaseEntity,
    IDateRange,
    IDemographicWithEmail,
    IEnrolleeReference,
    IHistory,
    IInsuredEnrolleeReference,
    IOldSystemReference,
)


class IArdbSubscriber(TypedDict):
    ENROLLEE_ID: str
    INSURED_ENROLLEE_ID: str

    SUBSCRIBER_ID: str
    PREMIUM_GROUP_ID: str
    PREMIUM_GROUP_DEPARTMENT_ID: str

    EMPLOYMENT_STATUS: str
    EFFECTIVE_DATE: str
    TERMINATION_DATE: str

    LAST_NAME: str
    FIRST_NAME: str
    MIDDLE_NAME: str

    DOB: str
    GENDER: str
    EMAIL: str

    ADDRESS1: str
    ADDRESS2: str
    CITY: str
    STATE: str
    ZIP: str
    ZIP_4: str

    CREATION_DATE: str
    LAST_MODIFIED_DATE_TIME: str


class ITherapySubscriberPremiumGroup(IOldSystemReference):
    name: Optional[str]
    department: Optional[IOldSystemReference]


class ITherapySubscriberDemographic(TypedDict, total=False):
    demographic: IDemographicWithEmail


class ITherapySubscriberInfo(
    TypedDict,
):
    subscriberNumber: str
    premiumGroup: Optional[ITherapySubscriberPremiumGroup]
    demographic: ITherapySubscriberDemographic


class ITherapySubscriberEmployment(IDateRange):
    status: Optional[str]


class ITherapySubscriber(
    IBaseEntity,
    ITherapySubscriberInfo,
    IARDBDocumentReference,
    IEnrolleeReference,
    IInsuredEnrolleeReference,
    IHistory[ITherapySubscriberInfo],
):
    policyNumber: str
    payerResponsibilityCode: str
    claimFilingIndicatorCode: str

    employment: Optional[ITherapySubscriberEmployment]


subscriber: ITherapySubscriber = {
    "_id": "1",
    "created": {"by": "system", "at": datetime.now()},
    "updated": {"by": "system", "at": datetime.now()},
    "subscriberNumber": "1234567890",
    "premiumGroup": {
        "name": "Premium Group",
        "department": {"refId": "1234567890", "referenceId": "1234567890"},
    },
    "policyNumber": "1234567890",
    "payerResponsibilityCode": "1234567890",
    "claimFilingIndicatorCode": "1234567890",
    "employment": {
        "status": "Active",
        "startDate": datetime.now(),
        "endDate": datetime.now(),
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
                "subscriberNumber": "1234567890",
                "premiumGroup": {
                    "name": "Premium Group",
                    "department": {"refId": "1234567890", "referenceId": "1234567890"},
                },
            },
        }
    ],
    "demographic": {
        "dob": datetime.now(),
        "gender": "Male",
        "email": "john.doe@example.com",
        "firstName": "John",
        "middleName": "Doe",
        "lastName": "Smith",
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
    "enrollee": {"referenceId": "1234567890", "referenceId": "1234567890"},
    "insuredEnrollee": {"referenceId": "1234567890", "referenceId": "1234567890"},
}
