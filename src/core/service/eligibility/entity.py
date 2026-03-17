from datetime import date
from typing import List, Optional, TypedDict

from src.core.service.patients.entity import IPatientRelationship
from src.shared.interface.common import (
    IARDBDocumentReference,
    IBaseEntity,
    IDateRange,
    IEnrolleeReference,
    IIdentifierReference,
    IName,
    IOldSystemReference,
)


class IArdbEligibility(TypedDict):
    ENROLLEE_ID: str
    INSURED_ENROLLEE_ID: str
    PRODUCT_ID: str
    EFFECTIVE_DATE: str
    TERMINATION_DATE: str

    SUBSCRIBER_ID: str
    MEMBER_ID: str
    BENEFIT_STATUS_CODE: str
    RELATIONSHIP_CODE: str

    MARITAL_STATUS: str
    STUDENT_STATUS_CODE: str
    HANDICAP_FLAG: str
    LEVEL_OF_CARE_ID: str
    LATE_ENROLLEE_FLAG: str
    WAITING_PERIOD_CREDIT: str
    ADD_REASON_CODE: str
    ADD_REASON_EVENT_DATE: str
    TERMINATION_REASON_CODE: str
    TERMINATION_REASON_EVENT_DATE: str
    CLIENT_MCO_ID: str
    CLIENT_PROGRAM_CODE: str
    CLIENT_RATE_CODE: str
    OTHER_INFO1: str
    OTHER_INFO2: str
    OTHER_INFO3: str
    EL_DOMAIN_SOURCE_ID: str
    PAID_THROUGH_DATE: str
    PAID_THROUGH_GRACE_PERIOD: str

    CREATION_DATE: str
    LAST_MODIFIED_DATE_TIME: str


class IEligibilityAddReasonCode(TypedDict):
    code: str
    eventDate: date


class IEligibilityTerminationReasonCode(TypedDict):
    code: str
    eventDate: date


class IEligibilityClient(TypedDict):
    mcoId: str
    programCode: str
    rateCode: str


class IEligibilityPaidThrough(TypedDict):
    date: date
    gracePeriod: int


class ITherapyEligibilityAdditionalInformation(TypedDict, total=False):
    addReasonCode: IEligibilityAddReasonCode
    terminationReason: IEligibilityTerminationReasonCode
    client: IEligibilityClient
    otherInformation: List[str]
    paidThrough: IEligibilityPaidThrough

    isLateEnrollee: bool
    isHandicapped: bool

    levelOfCareId: str
    waitingPeriodCredit: str
    domainSourceId: str


class IPatientDob(TypedDict):
    dob: date
    formattedDob: str


class ITherapyPatientReference(
    IName, IIdentifierReference, IPatientRelationship, IPatientDob
):
    pass


class ITherapyEligibility(IBaseEntity, IEnrolleeReference, IARDBDocumentReference):
    product: IOldSystemReference
    serviceDate: IDateRange

    subscriber: IIdentifierReference
    patient: ITherapyPatientReference

    maritalStatus: Optional[str]
    benefitStatusCode: Optional[str]
    studentStatusCode: Optional[str]

    additionalInformation: ITherapyEligibilityAdditionalInformation
