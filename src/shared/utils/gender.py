from src.shared.interface.migration import Gender


def to_therapy_gender(value: str | None) -> Gender | None:
    if value == "F" or value == "f":
        return Gender.FEMALE
    if value == "M" or value == "m":
        return Gender.MALE
    if value == "O" or value == "o":
        return Gender.OTHER

    return None


def to_ardb_gender(value: Gender | None) -> str | None:
    if value == Gender.FEMALE:
        return "F"
    if value == Gender.MALE:
        return "M"
    if value == Gender.OTHER:
        return "O"

    return None
