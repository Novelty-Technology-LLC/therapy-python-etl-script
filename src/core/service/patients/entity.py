from typing import Optional, TypedDict

from src.shared.interface.common import (
    IARDBDocumentReference,
    IBaseEntity,
    IDemographic,
    IEnrolleeReference,
    IHistory,
    IIdentifierReference,
    IQualifier,
)


class IArdbPatient(TypedDict):
    ENROLLEE_ID: str
    INSURED_ENROLLEE_ID: str
    SUBSCRIBER_ID: str
    MEMBER_ID: str
    RELATIONSHIP_CODE: str

    # LAST_NAME: str
    # FIRST_NAME: str
    # MIDDLE_NAME: str
    # DOB: str
    # GENDER: str
    # EMAIL: str
    # SS_NUMBER: str
    # ADDRESS1: str
    # ADDRESS2: str
    # CITY: str
    # STATE: str
    # ZIP: str
    # ZIP_4: str

    # Subscriber
    # LAST_NAME_SUBSCRIBER: str
    # FIRST_NAME_SUBSCRIBER: str
    # MIDDLE_NAME_SUBSCRIBER: str
    # SUBSCRIBER_NAME: str
    CREATION_DATE: str
    LAST_MODIFIED_DATE_TIME: str


class IPatientDemographic(IDemographic):
    email: Optional[str]


class IPatientSubscriber(IIdentifierReference):
    pass


class IPatientInfo(TypedDict):
    subscriber: IPatientSubscriber
    ssn: Optional[str]
    demographic: IPatientDemographic


class IPatientRelationship(TypedDict):
    memberId: str
    relationship: IQualifier


class ITherapyPatient(
    IPatientInfo,
    IBaseEntity,
    IARDBDocumentReference,
    IEnrolleeReference,
    IHistory[IPatientInfo],
    IPatientRelationship,
):
    # _id: str
    # enrollee: IEnrolleeReference
    # histories: List[IHistory[IPatientInfo]]
    pass


# patient: IPatientTherapy = {
#     '_id': '123',
#     'demographic': {
#         'address': {
#             'addressLine1': '123 Main St',
#             'addressLine2': 'Apt 1',
#             'city': 'Anytown',
#             'state': 'CA',
#             'zipCode': '12345',
#             'zipCode4': '1234',
#         },
#         'email': 'test@example.com',
#         'firstName': 'John',
#         'lastName': 'Doe',
#         'middleName': 'Smith',
#         'gender': 'Male',
#         'dob': '1990-01-01',
#         'phone': '1234567890',
#         'ssn': '1234567890',
#     },
#     'subscriber': {
#         'identificationCode': '1234567890',
#         'name': 'John Doe',
#         'refId': '1234567890',
#     },
#     'ssn': '1234567890',
#     'enrollee': {
#         'refId': '1234567890',
#         'referenceId': '1234567890',
#     },
#     'history': [{
#         'updated': {
#             'by': 'system',
#             'at': '2026-01-01',
#         },
#         'histories': [
#             {
#                 'demographic': {
#                     'address': {
#                         'addressLine1': '123 Main St',
#                         'addressLine2': 'Apt 1',
#                         'city': 'Anytown',
#                         'state': 'CA',
#                         'zipCode': '12345',
#                         'zipCode4': '1234',
#                     },
#                     'email': 'test@example.com',
#                     'firstName': 'John',
#                     'lastName': 'Doe',
#                     'middleName': 'Smith',
#                     'gender': 'Male',
#                     'dob': '1990-01-01',
#                     'phone': '1234567890',
#                     'ssn': '1234567890',
#                 },
#                 'subscriber': {
#                     'identificationCode': '1234567890',
#                     'name': 'John Doe',
#                     'refId': '1234567890',
#                 },
#                 'ssn': '1234567890',
#                 'enrollee': {
#                     'refId': '1234567890',
#                     'referenceId': '1234567890',
#                 },
#             }
#         ]
#     }]
# }
