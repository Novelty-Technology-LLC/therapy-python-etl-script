from typing import TypedDict
from src.shared.interface.common import IBaseEntity


class ITherapyProductReference(TypedDict):
    referenceId: str


class ITherapyProduct(IBaseEntity):
    name: str
    product: ITherapyProductReference
