from typing import TypedDict
from src.shared.interface.common import IBaseEntity


class ITherapyProvider(IBaseEntity):
    referenceId: str
