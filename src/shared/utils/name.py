from src.shared.interface.common import IName
from src.shared.utils.obj import get_obj_value


def get_name(name: IName) -> str:
    first_name = get_obj_value(name, "firstName")
    middle_name = get_obj_value(name, "middleName")
    last_name = get_obj_value(name, "lastName")

    parts = []
    if first_name and len(first_name) > 0:
        parts.append(first_name)
    if middle_name and len(middle_name) > 0:
        parts.append(middle_name)
    if last_name and len(last_name) > 0:
        parts.append(last_name)

    if len(parts) == 0:
        return ""

    return " ".join(p for p in parts if p)
