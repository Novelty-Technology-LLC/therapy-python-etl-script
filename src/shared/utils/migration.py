import uuid_utils as uuid

from src.shared.interface.etl.migration import FileMetadata
from src.shared.utils.obj import get_obj_value


def generate_uuid() -> str:
    return str(uuid.uuid7())


def generate_file_metadata(file_metadata: FileMetadata):
    return {
        "documentId": get_obj_value(file_metadata, "document_id"),
        "originalName": get_obj_value(file_metadata, "original_file_name"),
        "fileExtension": get_obj_value(file_metadata, "file_extension"),
        "fileType": get_obj_value(file_metadata, "file_type"),
        "fileName": get_obj_value(file_metadata, "ardb_file_name"),
        "filePath": get_obj_value(file_metadata, "ardb_file_path"),
        "isReconciled": False,
        "updatedAt": get_obj_value(file_metadata, "ardb_file_processed_at"),
    }
