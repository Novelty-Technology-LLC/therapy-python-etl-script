from enum import Enum


class Gender(str, Enum):
    FEMALE = "FEMALE"
    MALE = "MALE"
    OTHER = "OTHER"


class InputFileType(Enum):
    EXCEL = "xlsx"
    RPT = "rpt"
