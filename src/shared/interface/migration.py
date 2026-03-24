from enum import Enum


class Gender(str, Enum):
    FEMALE = "FEMALE"
    MALE = "MALE"
    OTHER = "OTHER"


class InputFileType(str, Enum):
    EXCEL = "xlsx"
    RPT = "rpt"
