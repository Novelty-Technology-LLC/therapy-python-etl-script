from src.core.service.enrollees.entity import ITherapyEnrollee
from src.core.service.patients.entity import IArdbPatient, ITherapyPatient
from src.core.service.subscribers.entity import ITherapySubscriber
from src.shared.interface.etl.migration import FileMetadata
from src.shared.utils.date import to_datetime
from src.shared.utils.name import get_name
from src.shared.utils.obj import get_obj_value
from src.shared.utils.qualifiers import resolve_relationship


class PatientMapper:
    """Patient Mapper"""

    def to_ardb(self, patient: ITherapyPatient) -> IArdbPatient:
        """Convert the patient therapy to ardb format"""

        result: IArdbPatient = {
            "ENROLLEE_ID": patient["enrollee"]["referenceId"],
            # 'LAST_NAME': patient['demographic']['lastName'],
            # 'FIRST_NAME': patient['demographic']['firstName'],
            # 'MIDDLE_NAME': patient['demographic']['middleName'],
            # 'DOB': patient['demographic']['dob'],
            # 'GENDER': patient['demographic']['gender'],
            # 'EMAIL': patient['demographic']['email'],
            # 'SS_NUMBER': patient['ssn'],
            # 'ADDRESS1': patient['demographic']['address']['addressLine1'],
            # 'ADDRESS2': patient['demographic']['address']['addressLine2'],
            # 'CITY': patient['demographic']['address']['city'],
            # 'STATE': patient['demographic']['address']['state'],
            # 'ZIP': patient['demographic']['address']['zipCode'],
            # 'ZIP_4': patient['demographic']['address']['zipCode4'],
            "RELATIONSHIP_CODE": patient["relationship"]["codeQualifier"],
            "MEMBER_ID": patient["memberId"],
            "SUBSCRIBER_ID": patient["subscriber"]["identificationCode"],
            "CREATION_DATE": patient["created"]["at"],
            "LAST_MODIFIED_DATE_TIME": patient["updated"]["at"],
        }

        return result

    def to_therapy(
        self,
        patient: IArdbPatient,
        enrollee: ITherapyEnrollee,
        subscriber: ITherapySubscriber,
        file_metadata: FileMetadata,
    ) -> ITherapyPatient:
        name = get_name(
            {
                "firstName": get_obj_value(subscriber, "firstName"),
                "middleName": get_obj_value(subscriber, "middleName"),
                "lastName": get_obj_value(subscriber, "lastName"),
            }
        )

        return {
            "_id": get_obj_value(patient, "_id"),
            "hasCompleteInfo": get_obj_value(patient, "hasCompleteInfo", default=True),
            "created": {
                "by": "system",
                "at": to_datetime(get_obj_value(patient, "CREATION_DATE")),
            },
            "updated": {
                "by": "system",
                "at": to_datetime(get_obj_value(patient, "LAST_MODIFIED_DATE_TIME")),
            },
            "enrollee": {
                "referenceId": get_obj_value(enrollee, "referenceId"),
                "refId": get_obj_value(enrollee, "_id"),
            },
            "demographic": {
                "lastName": get_obj_value(enrollee, "lastName"),
                "firstName": get_obj_value(enrollee, "firstName"),
                "middleName": get_obj_value(enrollee, "middleName"),
                "dob": to_datetime(get_obj_value(enrollee, "dob")),
                "formattedDob": get_obj_value(enrollee, "dob"),
                "gender": get_obj_value(enrollee, "gender"),
                "email": get_obj_value(enrollee, "email"),
                "phone": get_obj_value(enrollee, "phone"),
                "address": {
                    "addressLine1": get_obj_value(enrollee, "address", "addressLine1"),
                    "addressLine2": get_obj_value(enrollee, "address", "addressLine2"),
                    "city": get_obj_value(enrollee, "address", "city"),
                    "state": get_obj_value(enrollee, "address", "state"),
                    "zipCode": get_obj_value(enrollee, "address", "zipCode"),
                    "zipCode4": get_obj_value(enrollee, "address", "zipCode4"),
                },
            },
            "ssn": get_obj_value(enrollee, "ssn"),
            "subscriber": {
                "refId": get_obj_value(subscriber, "_id"),
                "identificationCode": get_obj_value(subscriber, "subscriberNumber"),
                "name": name,
            },
            "memberId": get_obj_value(patient, "MEMBER_ID"),
            "relationship": resolve_relationship(
                get_obj_value(patient, "RELATIONSHIP_CODE")
            ),
            "ardbDocuments": [
                {
                    "refId": None,
                    "fileName": get_obj_value(file_metadata, "ardb_file_name"),
                    "filePath": get_obj_value(file_metadata, "ardb_file_path"),
                    "isReconciled": False,
                    "updatedAt": get_obj_value(file_metadata, "ardb_file_processed_at"),
                }
            ],
            "ardbSourceDocument": get_obj_value(file_metadata, "ardb_file_name"),
            "ardbLastModifiedDate": get_obj_value(
                file_metadata, "ardb_file_processed_at"
            ),
            "histories": [],
        }


patient_mapper = PatientMapper()
