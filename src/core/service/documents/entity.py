from typing import TypedDict

from src.shared.interface.common import IBaseEntity
from src.shared.interface.document import DocumentStatusEnum


class IDocument(IBaseEntity):
    status: DocumentStatusEnum
