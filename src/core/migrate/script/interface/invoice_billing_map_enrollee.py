from typing import TypedDict

ISubscriberQuery = TypedDict(
    "ISubscriberQuery",
    {
        "subscriberNumber": str,
        "insuredEnrollee.referenceId": str,
    },
)

IPatientQuery = TypedDict(
    "IPatientQuery",
    {
        "enrollee.referenceId": str,
        "subscriber.identificationCode": str,
        "memberId": str,
    },
)
