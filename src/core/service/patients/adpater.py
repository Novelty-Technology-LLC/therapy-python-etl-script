from typing import List

from src.core.service.patients.entity import IArdbPatient, ITherapyPatient
from src.core.service.patients.mapper import patient_mapper


class PatientAdapter:
    """Patient Adapter"""

    def to_ardb_format(self, patients: List[ITherapyPatient]) -> List[IArdbPatient]:
        """Convert the patient dataframe to ardb format"""

        result: List[IArdbPatient] = []

        for patient in patients:

            ardb_patient = patient_mapper.to_ardb(patient)
            if not ardb_patient:
                continue

            result.append(ardb_patient)

        return result

    def to_therapy_format(self):
        """Convert the patient dataframe to therapy format"""
        pass


patient_adapter = PatientAdapter()
