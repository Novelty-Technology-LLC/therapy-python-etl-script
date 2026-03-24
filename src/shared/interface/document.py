from enum import Enum


class DocumentStatusEnum(str, Enum):
    NEW = "New"
    READY_TO_PROCESS = "Ready to process"
    INVALID = "Invalid"
    PROCESSING = "Processing"
    COMPLETED = "Completed"
    FAILED = "Failed"
