from datetime import datetime
from pathlib import Path
from typing import Optional, TypedDict
import uuid_utils as uuid

from src.core.service.documents.model import documentsModel
from src.shared.helper.s3_bucket_helper import aws_s3_helper
from src.shared.interface.document import DocumentStatusEnum
from src.shared.interface.etl.migration import FileMetadata
from src.shared.interface.migration import InputFileType
from src.shared.utils.date import timeStamp
from src.shared.utils.obj import get_obj_value


class DocumentResponse(TypedDict):
    file_metadata: FileMetadata
    documentId: str


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
        "metadata": {
            "size": get_obj_value(file_metadata, "file_size"),
            "source": "aws_s3",
        },
    }


def get_unique_documents(documents):
    seen = set()
    result = []

    for document in documents:
        if document["documentId"] not in seen:
            seen.add(document["documentId"])
            result.append(document)

    return result


def verify_and_generate_document(
    file: Path,
    support_duplicate_documents: bool,
    s3_module: str,
    file_type: InputFileType,
    enable_backup: bool = True,
    etl_type: str | None = None,
) -> DocumentResponse | None:
    if not support_duplicate_documents:
        documentFromDb = documentsModel.get_model().find_one(
            {
                "originalName": file.name,
                "status": {"$ne": DocumentStatusEnum.FAILED},
                **(etl_type and {"metadata.etlType": etl_type} or {}),
            }
        )

        if documentFromDb:
            print(f"Document {file.name} already exists in the database")
            return None

    receivedAt = datetime.now()
    s3_file_name = f"{timeStamp(receivedAt)}-{file.name}"
    s3_key = f"{s3_module}/{s3_file_name}" if enable_backup else None

    if enable_backup:
        aws_s3_helper.upload_file(file, s3_key)
    s3_prefix_key = aws_s3_helper._prefix_key(s3_key) if enable_backup else None

    documentId = generate_uuid()
    documentsModel.get_model().insert_one(
        {
            "_id": documentId,
            "originalName": file.name,
            "status": DocumentStatusEnum.NEW,
            "receivedAt": receivedAt,
            "fileName": s3_file_name,
            "metadata": {
                "destination": s3_prefix_key,
                "source": "aws_s3" if enable_backup else None,
                "size": file.stat().st_size,
                **(etl_type and {"etlType": etl_type} or {}),
            },
        }
    )

    file_metadata = FileMetadata(
        ardb_file_name=s3_file_name,
        ardb_file_path=s3_prefix_key,
        ardb_file_processed_at=receivedAt,
        document_id=documentId,
        file_extension=file.suffix,
        file_type=file_type,
        original_file_name=file.name,
        file_size=file.stat().st_size,
    )

    return {
        "file_metadata": file_metadata,
        "documentId": documentId,
    }
