from datetime import datetime
from enum import Enum
from typing import Any, List, Literal, NotRequired, Optional, TypedDict

from src.shared.interface.common import (
    AssignerEntity,
    IBaseEntity,
    IHistory,
    IIdentifierReference,
    ITherapyHistory,
)
from src.shared.interface.project_module import ProjectModule
from src.shared.interface.service_date import IServiceDate


class IArdbTherapyNote(TypedDict):
    RECEIPT_DETAIL_ID: str
    RECEIPT_ID: str
    INVOICE_BILLING_ID: str
    INVOICE_ITEM_NUMBER: str
    PAYMENT_NOTES: str
    DATE_ENTERED: str


type TherapyTaskActionType = Literal[
    "MOVE", "REASSIGN", "ASSIGN", "UNASSIGN", "ON-HOLD", "RESUME"
]


class TherapyNoteProjectModule(str, Enum):
    INVOICE_BILLING = ProjectModule.INVOICE_BILLING
    INVOICE_BILLING_DETAIL = ProjectModule.INVOICE_BILLING_DETAIL
    RECEIPT = ProjectModule.RECEIPT
    RECEIPT_DETAIL = ProjectModule.RECEIPT_DETAIL
    TASK = ProjectModule.TASK


class ITherapyNoteInfo(TypedDict):
    note: str
    tags: Optional[List[str]]

    isEdited: Optional[bool]
    locked: Optional[AssignerEntity]


class ITherapyNoteReference(TypedDict):
    module: TherapyNoteProjectModule
    subModule: Optional[ProjectModule]
    patientRef: Optional[IIdentifierReference]
    invoiceBillingRef: Optional[IIdentifierReference]
    invoiceBillingDetailRef: Optional[IIdentifierReference]
    receiptDetailRef: Optional[IIdentifierReference]
    enrolleeRef: Optional[IIdentifierReference]
    receiptRef: Optional[IIdentifierReference]
    taskRef: Optional[IIdentifierReference]
    procedureCode: Optional[str]
    serviceDate: Optional[IServiceDate]
    invoicePaymentReceiptRef: Optional[IIdentifierReference]
    ardbSourceDocument: str


class ITherapyNoteAction(TypedDict):
    type: TherapyTaskActionType
    data: NotRequired[Any]


class ITherapyNoteMetadata(TypedDict):
    action: Optional[ITherapyNoteAction]


class ITherapyLinkedDocument(TypedDict):
    refId: str
    originalName: str
    size: int


class ITherapyHistoryItem(ITherapyNoteInfo):
    updated: AssignerEntity
    ardbSourceDocument: str


class ITherapyNote(IBaseEntity, ITherapyNoteInfo, ITherapyHistory[ITherapyHistoryItem]):
    references: ITherapyNoteReference
    metadata: ITherapyNoteMetadata
    linkedDocuments: List[ITherapyLinkedDocument]


a: ITherapyNote = {
    "_id": "1",
    "created": {"by": "system", "at": datetime.now()},
    "updated": {"by": "system", "at": datetime.now()},
    "note": "This is a test note",
    "tags": ["test", "note"],
    "isEdited": False,
    "locked": {"by": "system", "at": datetime.now()},
    "references": {
        "module": TherapyNoteProjectModule.INVOICE_BILLING,
        "subModule": ProjectModule.INVOICE_BILLING_DETAIL,
        "enrolleeRef": {"refId": 1, "identificationCode": "123", "name": "John Doe"},
        "invoiceBillingRef": {
            "refId": 1,
            "identificationCode": "123",
            "name": "John Doe",
        },
        "invoiceBillingDetailRef": {
            "refId": 1,
            "identificationCode": "123",
            "name": "John Doe",
        },
        "receiptDetailRef": {
            "refId": 1,
            "identificationCode": "123",
            "name": "John Doe",
        },
        "receiptRef": {"refId": 1, "identificationCode": "123", "name": "John Doe"},
        "taskRef": {"refId": 1, "identificationCode": "123", "name": "John Doe"},
        "procedureCode": "123",
        "serviceDate": {
            "endDate": datetime.now(),
            "startDate": datetime.now(),
            "formattedEndDate": "2026-01-01",
            "formattedStartDate": "2026-01-01",
            "month": "01",
            "year": "2026",
        },
        "patientRef": {"refId": 1, "identificationCode": "123", "name": "John Doe"},
    },
    "metadata": {
        "action": {"type": "MOVE", "data": {"to": "123"}},
    },
    "linkedDocuments": [
        {
            "refId": "1",
            "originalName": "test.pdf",
            "size": 100,
        }
    ],
    "histories": [
        {
            "note": "This is a test note",
            "tags": ["test", "note"],
            "isEdited": False,
            "locked": {"by": "system", "at": datetime.now()},
            "updated": {
                "by": "system",
                "at": datetime.now(),
            },
            "ardbSourceDocument": "test.pdf",
        }
    ],
}
