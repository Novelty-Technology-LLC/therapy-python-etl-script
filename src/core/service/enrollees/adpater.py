from typing import List

from src.core.service.enrollees.entity import IArdbEnrollee, ITherapyEnrollee
from src.core.service.enrollees.mapper import enrollee_mapper
from src.shared.interface.etl.migration import FileMetadata


class EnrolleeAdapter:
    """Enrollee Adapter"""

    def to_ardb_format(self, enrollees: List[ITherapyEnrollee]) -> List[IArdbEnrollee]:
        """Convert the enrollee dataframe to ardb format"""
        return [enrollee_mapper.to_ardb(enrollee) for enrollee in enrollees]

    def to_therapy_format(
        self, enrollees: List[IArdbEnrollee], file_metadata: FileMetadata
    ) -> List[ITherapyEnrollee]:
        """Convert the enrollee dataframe to therapy format"""
        a = [
            enrollee_mapper.to_therapy(enrollee, file_metadata)
            for enrollee in enrollees
        ]

        return a


enrollee_adapter = EnrolleeAdapter()
