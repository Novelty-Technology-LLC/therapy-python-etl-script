from src.mapper.relationships import RELATIONSHIP_CODES
from src.shared.interface.common import IQualifier


def resolve_qualifier(
    code_mapper: list[list[str]] | None = None,
    code: str | None = None,
) -> IQualifier[str]:
    if not code:
        return {"codeQualifier": "", "label": ""}

    qualifier: IQualifier[str] = {"codeQualifier": code, "label": ""}
    for key, value in code_mapper or []:
        if key == code:
            qualifier["label"] = value
            break
    return qualifier


def resolve_relationship(relationship_code: str | None = None) -> IQualifier[str]:
    return resolve_qualifier(RELATIONSHIP_CODES, relationship_code)
