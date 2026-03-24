from pathlib import Path

from src.shared.interface.migration import InputFileType


def get_input_files_path(
    input_file_path: Path, file_type: InputFileType = InputFileType.EXCEL
) -> list[Path]:
    project_root = Path(__file__).parent.parent.parent.parent

    input_dir = project_root / input_file_path

    all_files = list(input_dir.rglob(f"*.{file_type.value}"))
    return [f for f in all_files if not f.name.startswith("~$")]
