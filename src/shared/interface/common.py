from datetime import date
from typing import Any, Generic, List, Optional, TypedDict, TypeVar


T = TypeVar("T", bound=Any)
TStr = TypeVar("TStr", bound=str)


class CommonAssignerEntity(TypedDict):
    by: Optional[str]
    at: Optional[date]


class AdminAssignerEntity(CommonAssignerEntity):
    pass


class AssignerEntity(TypedDict, CommonAssignerEntity):
    id: Optional[str]


class IReference(TypedDict):
    refId: Optional[str]


class INameReference(IReference):
    name: Optional[str]


class ICodeReference(IReference):
    identificationCode: Optional[str]


class IQualifier(TypedDict, Generic[TStr]):
    codeQualifier: str
    label: TStr


class IIdentifierReference(INameReference, ICodeReference):
    pass


class IName(TypedDict):
    firstName: str
    middleName: Optional[str]
    lastName: str


class IAddress(TypedDict):
    addressLine1: str
    addressLine2: Optional[str]
    city: str
    state: str
    zipCode: str
    zipCode4: Optional[str]


class IDemographic(IName):
    gender: Optional[str]
    dob: Optional[str]
    phone: Optional[str]
    address: Optional[IAddress]


class IDemographicWithEmail(IDemographic):
    email: Optional[str]


class IDocumentReference(IReference):
    fileName: Optional[str]
    filePath: Optional[str]
    isReconciled: Optional[bool]


class IARDBDocumentReference(TypedDict):
    ardbDocuments: Optional[List[IDocumentReference]]
    ardbSourceDocument: Optional[str]
    ardbLastModifiedDate: Optional[str]


class IOldSystemReference(TypedDict):
    referenceId: Optional[str]


class IHistoryItem(TypedDict, Generic[T], total=False):
    """Represents T & { updated: AssignerEntity }"""

    updated: AssignerEntity
    history: T


class IHistory(TypedDict, Generic[T], total=False):
    histories: Optional[
        List[IHistoryItem[T]]
    ]  # Represents Partial<T & { updated: AssignerEntity }>


class IBaseEntity(TypedDict):
    _id: str
    created: AdminAssignerEntity
    updated: AdminAssignerEntity


class IOldAndNewSystemReference(IOldSystemReference, IReference):
    pass


class IEnrolleeReference(TypedDict):
    enrollee: IOldAndNewSystemReference


class IInsuredEnrolleeReference(TypedDict):
    insuredEnrollee: IOldAndNewSystemReference


class IDateRange(TypedDict):
    startDate: Optional[date]
    formattedStartDate: Optional[str]
    endDate: Optional[date]
    formattedEndDate: Optional[str]
