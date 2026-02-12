from typing import Literal, Optional, List, Union, Any, Generic, TypeVar, TypedDict
from enum import Enum
from bson.binary import Binary

# Generic type for entity
EntityType = TypeVar('EntityType', bound=Any)

# BSON type aliases (common MongoDB BSON types)
BSONTypeAlias = Literal[
    'string',
    'int',
    'long',
    'double',
    'decimal128',
    'bool',
    'date',
    'objectId',
    'binary',
    'object',
    'array'
]

# Database algorithm enum
class DbAlgorithm(str, Enum):
    """Database encryption algorithm types"""
    RANDOM = 'AEAD_AES_256_CBC_HMAC_SHA_512-Random'
    DETERMINISTIC = 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic'

# In-use encryption type
InUseEncryptionType = Literal['CSFLE', 'QE']


class EncryptionOptionType(TypedDict, Generic[EntityType], total=False):
    path: str  # keyof EntityType | GetDTOKeys<EntityType> -> simplified to str
    bsonType: BSONTypeAlias
    keyId: Optional[Binary]  # Optional explicit encryption key


class CSFLEOptionType(EncryptionOptionType[EntityType], total=False):
    algorithm: Optional[DbAlgorithm]


class QEQueryOptions(TypedDict, total=False):
    queryType: Literal['equality', 'none']
    contention: Optional[str]


class QEOptionType(EncryptionOptionType[EntityType], total=False):
    queries: Optional[QEQueryOptions]


class EncryptionFieldType(TypedDict, Generic[EntityType]):
    inUseEncryptionType: InUseEncryptionType
    fields: Union[
        List[CSFLEOptionType[EntityType]],
        List[QEOptionType[EntityType]]
    ]

