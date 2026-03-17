from src.shared.constant.priority_sheet_name import priority_sheet
from src.shared.interface.etl.sheet_name import SheetName


def sort_and_filter_sheets(sheet_names: list[SheetName]) -> list[SheetName]:
    """Filter to only priority sheets and sort by priority."""
    priority_map = {item["sheet_name"]: item["priority"] for item in priority_sheet}
    return sorted(
        [s for s in sheet_names if s in priority_map],
        key=lambda s: priority_map[s],
    )
