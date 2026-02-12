from typing import TypedDict

from src.shared.interface.common import IARDBDocumentReference, IBaseEntity


class IArdbProviderClaim(TypedDict):
    CLAIM_ID: str
    INSURED_ENROLLEE_ID: str


class ITherapyProviderClaimSubscriberInfo(TypedDict):
    INSURED_ENROLLEE_ID: str


class ITherapyProviderClaim(IBaseEntity, IARDBDocumentReference):
    CLAIM_ID: str
    SUBSCRIBER_INFO: ITherapyProviderClaimSubscriberInfo
