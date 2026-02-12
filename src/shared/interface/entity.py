from typing import Optional
from src.shared.interface.common import IDemographic


class IMemberDemographic(IDemographic):
    """Include Demographic and Email"""

    email: Optional[str]
    ssn: Optional[str]
