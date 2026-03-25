from typing import List, Optional
from src.core.service.provider_claims.entity import (
    IArdbProviderClaim,
    IProviderClaimDiagnosisCode,
    IProviderClaimServiceLine,
    ITherapyProviderClaim,
)
from src.shared.interface.etl.migration import FileMetadata
from src.shared.utils.migration import generate_file_metadata, get_unique_documents
from src.shared.utils.obj import get_obj_value


class ProviderClaimMapper:
    def to_therapy_claim_format(
        self,
        provider_claim: IArdbProviderClaim,
        _id: str,
        file_metadata: FileMetadata,
        old_provider_claim: Optional[ITherapyProviderClaim],
    ) -> ITherapyProviderClaim:

        # For diagnosis codes
        provider_claim_diagnosis_codes: IProviderClaimDiagnosisCode = {
            "DIAGNOSIS_CODE": get_obj_value(provider_claim, "DIAGNOSIS_CODE"),
            "DIAGNOSIS_DESCRIPTION": get_obj_value(
                provider_claim, "DIAGNOSIS_DESCRIPTION"
            ),
            "POA_INDICATOR": get_obj_value(provider_claim, "POA_INDICATOR"),
            "CODE_TYPE": get_obj_value(provider_claim, "CODE_TYPE"),
            "HIPAA_ICD_QUALIFIER_ID": get_obj_value(
                provider_claim, "HIPAA_ICD_QUALIFIER_ID"
            ),
        }

        old_provider_claim_diagnosis_codes: list[IProviderClaimDiagnosisCode] = []
        if old_provider_claim is not None:
            old_provider_claim_diagnosis_codes = old_provider_claim.get(
                "DIAGNOSIS_CODES", []
            )

        is_new_diagnosis_code = True
        for old_provider_claim_diagnosis_code in old_provider_claim_diagnosis_codes:
            if (
                old_provider_claim_diagnosis_code["DIAGNOSIS_CODE"]
                == provider_claim_diagnosis_codes["DIAGNOSIS_CODE"]
            ):
                is_new_diagnosis_code = False
                break

        if is_new_diagnosis_code:
            old_provider_claim_diagnosis_codes.append(provider_claim_diagnosis_codes)

        ardb_documents = [generate_file_metadata(file_metadata)]
        if old_provider_claim is not None:
            ardb_documents.extend(old_provider_claim.get("ardbDocuments", []))

        ardb_documents = get_unique_documents(ardb_documents)

        return {
            "_id": _id,
            "ACCEPT_ASSIGNMENT": get_obj_value(provider_claim, "ACCEPT_ASSIGNMENT"),
            "ADDITIONAL_DOCS_ATTACHED": get_obj_value(
                provider_claim, "ADDITIONAL_DOCS_ATTACHED"
            ),
            "ADJUDICATE_CYCLE_ID": get_obj_value(provider_claim, "ADJUDICATE_CYCLE_ID"),
            "AGE_GROUP_ID": get_obj_value(provider_claim, "AGE_GROUP_ID"),
            "AGGREGATE_DEDUCTIBLE_TO_OOP_FLAG": get_obj_value(
                provider_claim, "AGGREGATE_DEDUCTIBLE_TO_OOP_FLAG"
            ),
            "ALTERNATE_CLAIM_ID": get_obj_value(provider_claim, "ALTERNATE_CLAIM_ID"),
            "ALTERNATE_CLAIM_INDICATOR": get_obj_value(
                provider_claim, "ALTERNATE_CLAIM_INDICATOR"
            ),
            "ANESTHESIA_CODE": get_obj_value(provider_claim, "ANESTHESIA_CODE"),
            "APMR_LOG_ID": get_obj_value(provider_claim, "APMR_LOG_ID"),
            "APMR_LOG_ID": get_obj_value(provider_claim, "APMR_LOG_ID"),
            "AUTO_ACCIDENT_STATE": get_obj_value(provider_claim, "AUTO_ACCIDENT_STATE"),
            "AUTO_ACCIDENT_STATE": get_obj_value(provider_claim, "AUTO_ACCIDENT_STATE"),
            "BALANCE_DUE_FLAG": get_obj_value(provider_claim, "BALANCE_DUE_FLAG"),
            "BALANCE_DUE_FLAG": get_obj_value(provider_claim, "BALANCE_DUE_FLAG"),
            "BANK_ID": get_obj_value(provider_claim, "BANK_ID"),
            "BANK_ID": get_obj_value(provider_claim, "BANK_ID"),
            "BENEFIT_LEVEL": get_obj_value(provider_claim, "BENEFIT_LEVEL"),
            "BENEFIT_LEVEL": get_obj_value(provider_claim, "BENEFIT_LEVEL"),
            "BENEFIT_LEVEL_ESCALATION": get_obj_value(
                provider_claim, "BENEFIT_LEVEL_ESCALATION"
            ),
            "BENEFIT_LEVEL_ESCALATION": get_obj_value(
                provider_claim, "BENEFIT_LEVEL_ESCALATION"
            ),
            "CASE_NUMBER": get_obj_value(provider_claim, "CASE_NUMBER"),
            "CASE_NUMBER": get_obj_value(provider_claim, "CASE_NUMBER"),
            "CHECKING_ACCOUNT": get_obj_value(provider_claim, "CHECKING_ACCOUNT"),
            "CHECKING_ACCOUNT": get_obj_value(provider_claim, "CHECKING_ACCOUNT"),
            "CHECK_NUMBER": get_obj_value(provider_claim, "CHECK_NUMBER"),
            "CHECK_NUMBER": get_obj_value(provider_claim, "CHECK_NUMBER"),
            "CLAIM_CATEGORY": get_obj_value(provider_claim, "CLAIM_CATEGORY"),
            "CLAIM_CATEGORY": get_obj_value(provider_claim, "CLAIM_CATEGORY"),
            "CLAIM_CODES": get_obj_value(provider_claim, "CLAIM_CODES"),
            "CLAIM_CODES": get_obj_value(provider_claim, "CLAIM_CODES"),
            "CLAIM_ID": get_obj_value(provider_claim, "CLAIM_ID"),
            "CLAIM_ID": get_obj_value(provider_claim, "CLAIM_ID"),
            "CLAIM_RESUBMISSION_QUALIFIER": get_obj_value(
                provider_claim, "CLAIM_RESUBMISSION_QUALIFIER"
            ),
            "CLAIM_RESUBMISSION_QUALIFIER": get_obj_value(
                provider_claim, "CLAIM_RESUBMISSION_QUALIFIER"
            ),
            "CLAIM_RESUBMITTED": get_obj_value(provider_claim, "CLAIM_RESUBMITTED"),
            "CLAIM_RESUBMITTED": get_obj_value(provider_claim, "CLAIM_RESUBMITTED"),
            "CLAIM_RESUBMIT_CATEGORY": get_obj_value(
                provider_claim, "CLAIM_RESUBMIT_CATEGORY"
            ),
            "CLAIM_RESUBMIT_CATEGORY": get_obj_value(
                provider_claim, "CLAIM_RESUBMIT_CATEGORY"
            ),
            "CLAIM_STATUS": get_obj_value(provider_claim, "CLAIM_STATUS"),
            "CLAIM_STATUS": get_obj_value(provider_claim, "CLAIM_STATUS"),
            "CLAIM_TYPE": get_obj_value(provider_claim, "CLAIM_TYPE"),
            "CLAIM_TYPE": get_obj_value(provider_claim, "CLAIM_TYPE"),
            "CLIENT_CLAIM_ID": get_obj_value(provider_claim, "CLIENT_CLAIM_ID"),
            "CLIENT_CLAIM_ID": get_obj_value(provider_claim, "CLIENT_CLAIM_ID"),
            "CODE_UNIVERSE_ID": get_obj_value(provider_claim, "CODE_UNIVERSE_ID"),
            "CODE_UNIVERSE_ID": get_obj_value(provider_claim, "CODE_UNIVERSE_ID"),
            "COPAY_AMOUNT": get_obj_value(provider_claim, "COPAY_AMOUNT"),
            "COPAY_AMOUNT": get_obj_value(provider_claim, "COPAY_AMOUNT"),
            "COUNTY_ID": get_obj_value(provider_claim, "COUNTY_ID"),
            "COUNTY_ID": get_obj_value(provider_claim, "COUNTY_ID"),
            "CREATION_DATE": get_obj_value(provider_claim, "CREATION_DATE"),
            "CREATION_DATE": get_obj_value(provider_claim, "CREATION_DATE"),
            "CTM_JOB_ID": get_obj_value(provider_claim, "CTM_JOB_ID"),
            "CTM_JOB_ID": get_obj_value(provider_claim, "CTM_JOB_ID"),
            "CURRENT_ILLNESS_DATE": get_obj_value(
                provider_claim, "CURRENT_ILLNESS_DATE"
            ),
            "CURRENT_ILLNESS_DATE": get_obj_value(
                provider_claim, "CURRENT_ILLNESS_DATE"
            ),
            "CURRENT_ILLNESS_DATE_QUALIFIER": get_obj_value(
                provider_claim, "CURRENT_ILLNESS_DATE_QUALIFIER"
            ),
            "CURRENT_ILLNESS_DATE_QUALIFIER": get_obj_value(
                provider_claim, "CURRENT_ILLNESS_DATE_QUALIFIER"
            ),
            "CVM_CLAIM_ID": get_obj_value(provider_claim, "CVM_CLAIM_ID"),
            "CVM_CLAIM_ID": get_obj_value(provider_claim, "CVM_CLAIM_ID"),
            "CVM_JOB_ID": get_obj_value(provider_claim, "CVM_JOB_ID"),
            "CVM_JOB_ID": get_obj_value(provider_claim, "CVM_JOB_ID"),
            "DATE_CLEAN_CLAIM": get_obj_value(provider_claim, "DATE_CLEAN_CLAIM"),
            "DATE_CLEAN_CLAIM": get_obj_value(provider_claim, "DATE_CLEAN_CLAIM"),
            "DATE_COMPLETED": get_obj_value(provider_claim, "DATE_COMPLETED"),
            "DATE_COMPLETED": get_obj_value(provider_claim, "DATE_COMPLETED"),
            "DATE_ENTERED": get_obj_value(provider_claim, "DATE_ENTERED"),
            "DATE_ENTERED": get_obj_value(provider_claim, "DATE_ENTERED"),
            "DATE_FIRST_STAMPED": get_obj_value(provider_claim, "DATE_FIRST_STAMPED"),
            "DATE_FIRST_STAMPED": get_obj_value(provider_claim, "DATE_FIRST_STAMPED"),
            "DATE_PAID": get_obj_value(provider_claim, "DATE_PAID"),
            "DATE_PAID": get_obj_value(provider_claim, "DATE_PAID"),
            "DATE_RECEIVED": get_obj_value(provider_claim, "DATE_RECEIVED"),
            "DATE_RECEIVED": get_obj_value(provider_claim, "DATE_RECEIVED"),
            "DIRTY_FLAG": get_obj_value(provider_claim, "DIRTY_FLAG"),
            "DOCUMENT_REFERENCE_FLAG": get_obj_value(
                provider_claim, "DOCUMENT_REFERENCE_FLAG"
            ),
            "DOMAIN_SOURCE_ID": get_obj_value(provider_claim, "DOMAIN_SOURCE_ID"),
            "EDIT_CYCLE_ID": get_obj_value(provider_claim, "EDIT_CYCLE_ID"),
            "EFFECTIVE_DATE": get_obj_value(provider_claim, "EFFECTIVE_DATE"),
            "EIM_JOB_ID": get_obj_value(provider_claim, "EIM_JOB_ID"),
            "ELIGIBILITY_STATUS": get_obj_value(provider_claim, "ELIGIBILITY_STATUS"),
            "ELIG_OTHER_INFO1": get_obj_value(provider_claim, "ELIG_OTHER_INFO1"),
            "ELIG_OTHER_INFO2": get_obj_value(provider_claim, "ELIG_OTHER_INFO2"),
            "ELIG_OTHER_INFO3": get_obj_value(provider_claim, "ELIG_OTHER_INFO3"),
            "ENCOUNTER_ID": get_obj_value(provider_claim, "ENCOUNTER_ID"),
            "ENTITY_TYPE": get_obj_value(provider_claim, "ENTITY_TYPE"),
            "EOB_PRESENT_FLAG": get_obj_value(provider_claim, "EOB_PRESENT_FLAG"),
            "ERM_JOB_ID": get_obj_value(provider_claim, "ERM_JOB_ID"),
            "FIRST_STAMPED_BY": get_obj_value(provider_claim, "FIRST_STAMPED_BY"),
            "FULFILLMENT_DATE": get_obj_value(provider_claim, "FULFILLMENT_DATE"),
            "HOSPITALIZED_DATE_FROM": get_obj_value(
                provider_claim, "HOSPITALIZED_DATE_FROM"
            ),
            "HOSPITALIZED_DATE_TO": get_obj_value(
                provider_claim, "HOSPITALIZED_DATE_TO"
            ),
            "ICD_CODE_TYPE": get_obj_value(provider_claim, "ICD_CODE_TYPE"),
            "IMPORT_DATE": get_obj_value(provider_claim, "IMPORT_DATE"),
            "INCOMPLETE_CLAIM_FLAG": get_obj_value(
                provider_claim, "INCOMPLETE_CLAIM_FLAG"
            ),
            "IPA": get_obj_value(provider_claim, "IPA"),
            "IPA_NAME": get_obj_value(provider_claim, "IPA_NAME"),
            "NETWORK_ID": get_obj_value(provider_claim, "NETWORK_ID"),
            "NOTES": get_obj_value(provider_claim, "NOTES"),
            "OFFICE_REF_NUMBER": get_obj_value(provider_claim, "OFFICE_REF_NUMBER"),
            "ORIGINAL_CLAIM_ID": get_obj_value(provider_claim, "ORIGINAL_CLAIM_ID"),
            "OTHER_INSURANCE_COMPANY": get_obj_value(
                provider_claim, "OTHER_INSURANCE_COMPANY"
            ),
            "OTHER_INSURANCE_FLAG": get_obj_value(
                provider_claim, "OTHER_INSURANCE_FLAG"
            ),
            "OTHER_INSURED_DOB": get_obj_value(provider_claim, "OTHER_INSURED_DOB"),
            "OTHER_INSURED_EMPLOYER_OR_SCHOOL": get_obj_value(
                provider_claim, "OTHER_INSURED_EMPLOYER_OR_SCHOOL"
            ),
            "OTHER_INSURED_GENDER": get_obj_value(
                provider_claim, "OTHER_INSURED_GENDER"
            ),
            "OTHER_INSURED_NAME": get_obj_value(provider_claim, "OTHER_INSURED_NAME"),
            "OTHER_INSURED_PATIENT_RELATIONSHIP": get_obj_value(
                provider_claim, "OTHER_INSURED_PATIENT_RELATIONSHIP"
            ),
            "OTHER_INSURED_PLAN_OR_PROGRAM": get_obj_value(
                provider_claim, "OTHER_INSURED_PLAN_OR_PROGRAM"
            ),
            "OTHER_INSURED_POLICY": get_obj_value(
                provider_claim, "OTHER_INSURED_POLICY"
            ),
            "OTHER_INSURED_SUBSCRIBER_ID": get_obj_value(
                provider_claim, "OTHER_INSURED_SUBSCRIBER_ID"
            ),
            "OTHER_INSURED_TYPE": get_obj_value(provider_claim, "OTHER_INSURED_TYPE"),
            "OUTSIDE_LAB": get_obj_value(provider_claim, "OUTSIDE_LAB"),
            "OUT_OF_AREA_FLAG": get_obj_value(provider_claim, "OUT_OF_AREA_FLAG"),
            "PATIENT_UNABLE_TO_WORK_DATE_FROM": get_obj_value(
                provider_claim, "PATIENT_UNABLE_TO_WORK_DATE_FROM"
            ),
            "PATIENT_UNABLE_TO_WORK_DATE_TO": get_obj_value(
                provider_claim, "PATIENT_UNABLE_TO_WORK_DATE_TO"
            ),
            "PAYMENT_CYCLE_ID": get_obj_value(provider_claim, "PAYMENT_CYCLE_ID"),
            "PAYMENT_GROUP_ID": get_obj_value(provider_claim, "PAYMENT_GROUP_ID"),
            "PAYMENT_NOTES": get_obj_value(provider_claim, "PAYMENT_NOTES"),
            "PAY_MEMBER_FLAG": get_obj_value(provider_claim, "PAY_MEMBER_FLAG"),
            "PCP_MATCH_FLAG": get_obj_value(provider_claim, "PCP_MATCH_FLAG"),
            "PREDETERMINATION_CLAIM_ID": get_obj_value(
                provider_claim, "PREDETERMINATION_CLAIM_ID"
            ),
            "PREMIUM_GROUP_DEPARTMENT_ID": get_obj_value(
                provider_claim, "PREMIUM_GROUP_DEPARTMENT_ID"
            ),
            "PREMIUM_GROUP_ID": get_obj_value(provider_claim, "PREMIUM_GROUP_ID"),
            "PRICING_REQUEST_ID": get_obj_value(provider_claim, "PRICING_REQUEST_ID"),
            "PROCESSING_GROUP_ID": get_obj_value(provider_claim, "PROCESSING_GROUP_ID"),
            "PROCESSING_STATE": get_obj_value(provider_claim, "PROCESSING_STATE"),
            "RECORD_SOURCE_ID": get_obj_value(provider_claim, "RECORD_SOURCE_ID"),
            "RECORD_SOURCE_SUBTYPE": get_obj_value(
                provider_claim, "RECORD_SOURCE_SUBTYPE"
            ),
            "REFERRAL_DATE": get_obj_value(provider_claim, "REFERRAL_DATE"),
            "REFERRAL_REQUIRED_FLAG": get_obj_value(
                provider_claim, "REFERRAL_REQUIRED_FLAG"
            ),
            "REFUND_ID": get_obj_value(provider_claim, "REFUND_ID"),
            "REGION_ID": get_obj_value(provider_claim, "REGION_ID"),
            "RELATED_TO_AUTO_ACCIDENT": get_obj_value(
                provider_claim, "RELATED_TO_AUTO_ACCIDENT"
            ),
            "RELATED_TO_EMPLOYMENT": get_obj_value(
                provider_claim, "RELATED_TO_EMPLOYMENT"
            ),
            "RELATED_TO_OTHER_ACCIDENT": get_obj_value(
                provider_claim, "RELATED_TO_OTHER_ACCIDENT"
            ),
            "RELATIONSHIP_CODE": get_obj_value(provider_claim, "RELATIONSHIP_CODE"),
            "RESUBMISSION_ORIGINAL_CLAIM": get_obj_value(
                provider_claim, "RESUBMISSION_ORIGINAL_CLAIM"
            ),
            "RESUBMIT_REASON_CODE": get_obj_value(
                provider_claim, "RESUBMIT_REASON_CODE"
            ),
            "ROWVERSION": get_obj_value(provider_claim, "ROWVERSION"),
            "SECONDARY_CLIENT_CLAIM_ID": get_obj_value(
                provider_claim, "SECONDARY_CLIENT_CLAIM_ID"
            ),
            "SECONDARY_CLIENT_CLAIM_QUALIFIER": get_obj_value(
                provider_claim, "SECONDARY_CLIENT_CLAIM_QUALIFIER"
            ),
            "SIMILAR_ILLNESS_DATE": get_obj_value(
                provider_claim, "SIMILAR_ILLNESS_DATE"
            ),
            "SIMILAR_ILLNESS_DATE_QUALIFIER": get_obj_value(
                provider_claim, "SIMILAR_ILLNESS_DATE_QUALIFIER"
            ),
            "SINGLE_PROCESSING_FLAG": get_obj_value(
                provider_claim, "SINGLE_PROCESSING_FLAG"
            ),
            "SUBROGATION_TYPE": get_obj_value(provider_claim, "SUBROGATION_TYPE"),
            "SUBSCRIBER_CONTRACT_TYPE": get_obj_value(
                provider_claim, "SUBSCRIBER_CONTRACT_TYPE"
            ),
            "SUPPLEMENTAL_CLAIM_INFORMATION": get_obj_value(
                provider_claim, "SUPPLEMENTAL_CLAIM_INFORMATION"
            ),
            "TERMINATION_DATE": get_obj_value(provider_claim, "TERMINATION_DATE"),
            "VISIT_TYPE": get_obj_value(provider_claim, "VISIT_TYPE"),
            "ZIP": get_obj_value(provider_claim, "ZIP"),
            "ZIP_4": get_obj_value(provider_claim, "ZIP_4"),
            "ZONE_ID": get_obj_value(provider_claim, "ZONE_ID"),
            "INSURER_INFO": {
                "INSURER_ID": get_obj_value(provider_claim, "INSURER_ID"),
                "INSURER_DESCRIPTION": get_obj_value(
                    provider_claim, "INSURER_DESCRIPTION"
                ),
            },
            "LOCATION_INFO": {
                "LOCATION_ID": get_obj_value(provider_claim, "LOCATION_ID"),
                "LOCATION_NAME": get_obj_value(provider_claim, "LOCATION_NAME"),
                "LOCATION_NPI": get_obj_value(provider_claim, "LOCATION_NPI"),
                "LOCATION_SUPPLEMENTAL_QUALIFIER": get_obj_value(
                    provider_claim, "LOCATION_SUPPLEMENTAL_QUALIFIER"
                ),
                "LOCATION_SUPPLEMENTAL_IDENTIFIER": get_obj_value(
                    provider_claim, "LOCATION_SUPPLEMENTAL_IDENTIFIER"
                ),
            },
            "MARKET_INFO": {
                "MARKET_ID": get_obj_value(provider_claim, "MARKET_ID"),
                "MARKET_DESCRIPTION": get_obj_value(
                    provider_claim, "MARKET_DESCRIPTION"
                ),
            },
            "PATIENT": {
                "ENROLLEE_ID": get_obj_value(provider_claim, "ENROLLEE_ID"),
                "SUBSCRIBER_ID": get_obj_value(provider_claim, "SUBSCRIBER_ID"),
                "MEMBER_ID": get_obj_value(provider_claim, "MEMBER_ID"),
                "PATIENT_LAST_NAME": get_obj_value(provider_claim, "PATIENT_LAST_NAME"),
                "PATIENT_FIRST_NAME": get_obj_value(
                    provider_claim, "PATIENT_FIRST_NAME"
                ),
                "PATIENT_MIDDLE_NAME": get_obj_value(
                    provider_claim, "PATIENT_MIDDLE_NAME"
                ),
                "AGE": get_obj_value(provider_claim, "AGE"),
                "GENDER": get_obj_value(provider_claim, "GENDER"),
                "DOB": get_obj_value(provider_claim, "DOB"),
            },
            "PAYEE_INFO": {
                "PAYEE_ID": get_obj_value(provider_claim, "PAYEE_ID"),
                "PAYEE_NAME": get_obj_value(provider_claim, "PAYEE_NAME"),
                "PAYEE_ADDRESS1": get_obj_value(provider_claim, "PAYEE_ADDRESS1"),
                "PAYEE_ADDRESS2": get_obj_value(provider_claim, "PAYEE_ADDRESS2"),
                "PAYEE_CITY": get_obj_value(provider_claim, "PAYEE_CITY"),
                "PAYEE_STATE": get_obj_value(provider_claim, "PAYEE_STATE"),
                "PAYEE_ZIP": get_obj_value(provider_claim, "PAYEE_ZIP"),
                "PAYEE_ZIP_4": get_obj_value(provider_claim, "PAYEE_ZIP_4"),
                "PAYEE_TAXONOMY": get_obj_value(provider_claim, "PAYEE_TAXONOMY"),
                "PAYEE_NPI": get_obj_value(provider_claim, "PAYEE_NPI"),
                "TAX_ID": get_obj_value(provider_claim, "TAX_ID"),
                "TAX_ID_QUALIFIER": get_obj_value(provider_claim, "TAX_ID_QUALIFIER"),
                "PAYEE_LICENSE_NUMBER": get_obj_value(
                    provider_claim, "PAYEE_LICENSE_NUMBER"
                ),
                "PAYEE_SUPPLEMENTAL_IDENTIFIER": get_obj_value(
                    provider_claim, "PAYEE_SUPPLEMENTAL_IDENTIFIER"
                ),
            },
            "PRODUCT_INFO": {
                "PRODUCT_ID": get_obj_value(provider_claim, "PRODUCT_ID"),
                "PRODUCT_NAME": get_obj_value(provider_claim, "PRODUCT_NAME"),
            },
            "REFERRING_PROVIDER_INFO": {
                "REFERRING_PROVIDER": get_obj_value(
                    provider_claim, "REFERRING_PROVIDER"
                ),
                "REFERRING_PROVIDER_QUALIFIER": get_obj_value(
                    provider_claim, "REFERRING_PROVIDER_QUALIFIER"
                ),
                "REFERRING_PROVIDER_NPI": get_obj_value(
                    provider_claim, "REFERRING_PROVIDER_NPI"
                ),
                "REFERRING_PROVIDER_SUPPLEMENTAL_QUALIFIER": get_obj_value(
                    provider_claim, "REFERRING_PROVIDER_SUPPLEMENTAL_QUALIFIER"
                ),
                "REFERRING_PROVIDER_SUPPLEMENTAL_IDENTIFIER": get_obj_value(
                    provider_claim, "REFERRING_PROVIDER_SUPPLEMENTAL_IDENTIFIER"
                ),
            },
            "RENDERING_PROVIDER_INFO": {
                "PROVIDER_ID": get_obj_value(provider_claim, "PROVIDER_ID"),
                "PROVIDER_LAST_NAME": get_obj_value(
                    provider_claim, "PROVIDER_LAST_NAME"
                ),
                "PROVIDER_FIRST_NAME": get_obj_value(
                    provider_claim, "PROVIDER_FIRST_NAME"
                ),
                "PROVIDER_TYPE": get_obj_value(provider_claim, "PROVIDER_TYPE"),
                "PROVIDER_TAXONOMY": get_obj_value(provider_claim, "PROVIDER_TAXONOMY"),
                "PROVIDER_NPI": get_obj_value(provider_claim, "PROVIDER_NPI"),
                "PROVIDER_LICENSE_NUMBER": get_obj_value(
                    provider_claim, "PROVIDER_LICENSE_NUMBER"
                ),
                "PROVIDER_SUPPLEMENTAL_IDENTIFIER": get_obj_value(
                    provider_claim, "PROVIDER_SUPPLEMENTAL_IDENTIFIER"
                ),
                "PROVIDER_SIGNATURE_ON_FILE": get_obj_value(
                    provider_claim, "PROVIDER_SIGNATURE_ON_FILE"
                ),
            },
            "SUBSCRIBER_INFO": {
                "INSURED_ENROLLEE_ID": get_obj_value(
                    provider_claim, "INSURED_ENROLLEE_ID"
                ),
                "INSURED_FIRST_NAME": get_obj_value(
                    provider_claim, "INSURED_FIRST_NAME"
                ),
                "INSURED_LAST_NAME": get_obj_value(provider_claim, "INSURED_LAST_NAME"),
                "INSURED_POLICY_OR_FECA_NUMBER": get_obj_value(
                    provider_claim, "INSURED_POLICY_OR_FECA_NUMBER"
                ),
                "INSURED_DOB": get_obj_value(provider_claim, "INSURED_DOB"),
                "INSURED_GENDER": get_obj_value(provider_claim, "INSURED_GENDER"),
                "INSURED_EMPLOYER_OR_SCHOOL": get_obj_value(
                    provider_claim, "INSURED_EMPLOYER_OR_SCHOOL"
                ),
                "INSURED_INSURANCE_PLAN": get_obj_value(
                    provider_claim, "INSURED_INSURANCE_PLAN"
                ),
                "INSURED_OTHER_INSURANCE": get_obj_value(
                    provider_claim, "INSURED_OTHER_INSURANCE"
                ),
                "MARITAL_STATUS": get_obj_value(provider_claim, "MARITAL_STATUS"),
                "EMPLOYMENT_STATUS": get_obj_value(provider_claim, "EMPLOYMENT_STATUS"),
            },
            "DIAGNOSIS_CODES": old_provider_claim_diagnosis_codes,
            "ardbDocuments": ardb_documents,
            "ardbSourceDocument": get_obj_value(file_metadata, "ardb_file_name"),
            "ardbLastModifiedDate": get_obj_value(
                file_metadata, "ardb_file_processed_at"
            ),
        }

    def to_therapy_service_line_format(
        self,
        provider_service_lines: list(IArdbProviderClaim),
        old_provider_service_lines: list(IProviderClaimServiceLine),
    ) -> list[IProviderClaimServiceLine]:
        response: list[IProviderClaimServiceLine] = []

        for provider_service_line in provider_service_lines:

            service_line_from_response = None
            for service_line in response:
                if (
                    service_line["CODE"] == provider_service_line["CODE"]
                    and service_line["DATE_OF_SERVICE"]
                    == provider_service_line["DATE_OF_SERVICE"]
                ):
                    service_line_from_response = service_line
                    break

            if service_line_from_response is not None:
                continue

            old_provider_service_line = None

            for old_provider_service_line in old_provider_service_lines:
                if (
                    old_provider_service_line["CODE"] == provider_service_line["CODE"]
                    and old_provider_service_line["DATE_OF_SERVICE"]
                    == provider_service_line["DATE_OF_SERVICE"]
                ):
                    old_provider_service_line = old_provider_service_line
                    break

            mapped_service_line = self.map_to_therapy_service_line_format(
                provider_service_line, old_provider_service_line
            )

            response.append(mapped_service_line)

        return response

    def map_to_therapy_service_line_format(
        self,
        provider_service_line: IArdbProviderClaim,
        old_provider_service_line: Optional[IProviderClaimServiceLine],
    ) -> IProviderClaimServiceLine:

        old_modifier1 = get_obj_value(old_provider_service_line, "MODIFIER1")
        old_modifier2 = get_obj_value(old_provider_service_line, "MODIFIER2")
        old_modifier3 = get_obj_value(old_provider_service_line, "MODIFIER3")
        old_modifier4 = get_obj_value(old_provider_service_line, "MODIFIER4")

        modifier1 = get_obj_value(provider_service_line, "MODIFIER1")
        modifier2 = get_obj_value(provider_service_line, "MODIFIER2")
        modifier3 = get_obj_value(provider_service_line, "MODIFIER3")
        modifier4 = get_obj_value(provider_service_line, "MODIFIER4")

        modifier1 = modifier1 if modifier1 is not None else old_modifier1
        modifier2 = modifier2 if modifier2 is not None else old_modifier2
        modifier3 = modifier3 if modifier3 is not None else old_modifier3
        modifier4 = modifier4 if modifier4 is not None else old_modifier4

        diagnosis_code_index1 = get_obj_value(
            provider_service_line, "DIAGNOSIS_CODE_INDEX1"
        )
        diagnosis_code_index2 = get_obj_value(
            provider_service_line, "DIAGNOSIS_CODE_INDEX2"
        )
        diagnosis_code_index3 = get_obj_value(
            provider_service_line, "DIAGNOSIS_CODE_INDEX3"
        )
        diagnosis_code_index4 = get_obj_value(
            provider_service_line, "DIAGNOSIS_CODE_INDEX4"
        )
        diagnosis_code_index5 = get_obj_value(
            provider_service_line, "DIAGNOSIS_CODE_INDEX5"
        )
        diagnosis_code_index6 = get_obj_value(
            provider_service_line, "DIAGNOSIS_CODE_INDEX6"
        )
        diagnosis_code_index7 = get_obj_value(
            provider_service_line, "DIAGNOSIS_CODE_INDEX7"
        )
        diagnosis_code_index8 = get_obj_value(
            provider_service_line, "DIAGNOSIS_CODE_INDEX8"
        )

        old_diagnosis_code_index1 = get_obj_value(
            old_provider_service_line, "DIAGNOSIS_CODE_INDEX1"
        )
        old_diagnosis_code_index2 = get_obj_value(
            old_provider_service_line, "DIAGNOSIS_CODE_INDEX2"
        )
        old_diagnosis_code_index3 = get_obj_value(
            old_provider_service_line, "DIAGNOSIS_CODE_INDEX3"
        )
        old_diagnosis_code_index4 = get_obj_value(
            old_provider_service_line, "DIAGNOSIS_CODE_INDEX4"
        )
        old_diagnosis_code_index5 = get_obj_value(
            old_provider_service_line, "DIAGNOSIS_CODE_INDEX5"
        )
        old_diagnosis_code_index6 = get_obj_value(
            old_provider_service_line, "DIAGNOSIS_CODE_INDEX6"
        )
        old_diagnosis_code_index7 = get_obj_value(
            old_provider_service_line, "DIAGNOSIS_CODE_INDEX7"
        )
        old_diagnosis_code_index8 = get_obj_value(
            old_provider_service_line, "DIAGNOSIS_CODE_INDEX8"
        )

        diagnosis_code_index1 = (
            diagnosis_code_index1
            if diagnosis_code_index1 is not None
            else old_diagnosis_code_index1
        )
        diagnosis_code_index2 = (
            diagnosis_code_index2
            if diagnosis_code_index2 is not None
            else old_diagnosis_code_index2
        )
        diagnosis_code_index3 = (
            diagnosis_code_index3
            if diagnosis_code_index3 is not None
            else old_diagnosis_code_index3
        )
        diagnosis_code_index4 = (
            diagnosis_code_index4
            if diagnosis_code_index4 is not None
            else old_diagnosis_code_index4
        )
        diagnosis_code_index5 = (
            diagnosis_code_index5
            if diagnosis_code_index5 is not None
            else old_diagnosis_code_index5
        )
        diagnosis_code_index6 = (
            diagnosis_code_index6
            if diagnosis_code_index6 is not None
            else old_diagnosis_code_index6
        )
        diagnosis_code_index7 = (
            diagnosis_code_index7
            if diagnosis_code_index7 is not None
            else old_diagnosis_code_index7
        )
        diagnosis_code_index8 = (
            diagnosis_code_index8
            if diagnosis_code_index8 is not None
            else old_diagnosis_code_index8
        )

        return {
            "SERVICE_PAYMENT_NOTES": get_obj_value(
                provider_service_line, "SERVICE_PAYMENT_NOTES"
            ),
            "ITEM_NUMBER": get_obj_value(provider_service_line, "ITEM_NUMBER"),
            "CLAIM_TYPE": get_obj_value(provider_service_line, "CLAIM_TYPE"),
            "ENCOUNTER_ID": get_obj_value(provider_service_line, "ENCOUNTER_ID"),
            "CODE": get_obj_value(provider_service_line, "CODE"),
            "CODE_ID": get_obj_value(provider_service_line, "CODE_ID"),
            "QUANTITY": get_obj_value(provider_service_line, "QUANTITY"),
            "DATE_OF_SERVICE": get_obj_value(provider_service_line, "DATE_OF_SERVICE"),
            "BILLED_AMOUNT": get_obj_value(provider_service_line, "BILLED_AMOUNT"),
            "COB_COLLECTED_AMOUNT": get_obj_value(
                provider_service_line, "COB_COLLECTED_AMOUNT"
            ),
            "AUTH_DATE": get_obj_value(provider_service_line, "AUTH_DATE"),
            "PROCEDURE_DESCRIPTION": get_obj_value(
                provider_service_line, "PROCEDURE_DESCRIPTION"
            ),
            "AUTH_NUMBER": get_obj_value(provider_service_line, "AUTH_NUMBER"),
            "ALLOWABLE_PERCENT": get_obj_value(
                provider_service_line, "ALLOWABLE_PERCENT"
            ),
            "CONVERSION_FACTOR": get_obj_value(
                provider_service_line, "CONVERSION_FACTOR"
            ),
            "ALLOWED_AMOUNT": get_obj_value(provider_service_line, "ALLOWED_AMOUNT"),
            "MEDICARE_ALLOWED_AMOUNT": get_obj_value(
                provider_service_line, "MEDICARE_ALLOWED_AMOUNT"
            ),
            "UC_AMOUNT": get_obj_value(provider_service_line, "UC_AMOUNT"),
            "PAYABLE_AMOUNT": get_obj_value(provider_service_line, "PAYABLE_AMOUNT"),
            "QUANTITY_AUTHORIZED": get_obj_value(
                provider_service_line, "QUANTITY_AUTHORIZED"
            ),
            "COPAY_AMOUNT": get_obj_value(provider_service_line, "COPAY_AMOUNT"),
            "COINSURANCE_AMOUNT": get_obj_value(
                provider_service_line, "COINSURANCE_AMOUNT"
            ),
            "AMOUNT_OVER_MAXIMUM": get_obj_value(
                provider_service_line, "AMOUNT_OVER_MAXIMUM"
            ),
            "AMOUNT_OVER_OOP": get_obj_value(provider_service_line, "AMOUNT_OVER_OOP"),
            "PATIENT_PAY_APPLIED_AMOUNT": get_obj_value(
                provider_service_line, "PATIENT_PAY_APPLIED_AMOUNT"
            ),
            "PAID_AMOUNT": get_obj_value(provider_service_line, "PAID_AMOUNT"),
            "ADJUSTED_PAID_AMOUNT": get_obj_value(
                provider_service_line, "ADJUSTED_PAID_AMOUNT"
            ),
            "RESUBMIT_COUNT": get_obj_value(provider_service_line, "RESUBMIT_COUNT"),
            "DEDUCTIBLE_APPLIED_AMOUNT": get_obj_value(
                provider_service_line, "DEDUCTIBLE_APPLIED_AMOUNT"
            ),
            "MAXIMUM_APPLIED_AMOUNT": get_obj_value(
                provider_service_line, "MAXIMUM_APPLIED_AMOUNT"
            ),
            "OOP_APPLIED_AMOUNT": get_obj_value(
                provider_service_line, "OOP_APPLIED_AMOUNT"
            ),
            "INTEREST_AMOUNT": get_obj_value(provider_service_line, "INTEREST_AMOUNT"),
            "PLACE_OF_SERVICE": get_obj_value(
                provider_service_line, "PLACE_OF_SERVICE"
            ),
            "REFERRAL_NUMBER": get_obj_value(provider_service_line, "REFERRAL_NUMBER"),
            "ONE_PER_VISIT_COPAY_FLAG": get_obj_value(
                provider_service_line, "ONE_PER_VISIT_COPAY_FLAG"
            ),
            "SERVICE_STATUS": get_obj_value(provider_service_line, "SERVICE_STATUS"),
            "COMPENSATION_ACCOUNT": get_obj_value(
                provider_service_line, "COMPENSATION_ACCOUNT"
            ),
            "NO_RECODING_FLAG": get_obj_value(
                provider_service_line, "NO_RECODING_FLAG"
            ),
            "SYSTEM_GENERATED_FLAG": get_obj_value(
                provider_service_line, "SYSTEM_GENERATED_FLAG"
            ),
            "DOMAIN_SOURCE_ID": get_obj_value(
                provider_service_line, "DOMAIN_SOURCE_ID"
            ),
            "CODE_TYPE": get_obj_value(provider_service_line, "CODE_TYPE"),
            "USER_ADDED_FLAG": get_obj_value(provider_service_line, "USER_ADDED_FLAG"),
            "REFUND_ID": get_obj_value(provider_service_line, "REFUND_ID"),
            "COMPARISON_GROUP_ID": get_obj_value(
                provider_service_line, "COMPARISON_GROUP_ID"
            ),
            "AUTH_ID": get_obj_value(provider_service_line, "AUTH_ID"),
            "CALCULATED_PAID_AMOUNT": get_obj_value(
                provider_service_line, "CALCULATED_PAID_AMOUNT"
            ),
            "DATE_OF_SERVICE_TO": get_obj_value(
                provider_service_line, "DATE_OF_SERVICE_TO"
            ),
            "PAYMENT_NOTES2": get_obj_value(provider_service_line, "PAYMENT_NOTES2"),
            "EPSDT": get_obj_value(provider_service_line, "EPSDT"),
            "COB": get_obj_value(provider_service_line, "COB"),
            "AUTH_ITEM_NUMBER": get_obj_value(
                provider_service_line, "AUTH_ITEM_NUMBER"
            ),
            "AUTH_PROVIDED_AT_ENTRY": get_obj_value(
                provider_service_line, "AUTH_PROVIDED_AT_ENTRY"
            ),
            "VISIT_COPAY_AMOUNT": get_obj_value(
                provider_service_line, "VISIT_COPAY_AMOUNT"
            ),
            "EMERGENCY_FLAG": get_obj_value(provider_service_line, "EMERGENCY_FLAG"),
            "REIMBURSEMENT_SCHEDULE_ID": get_obj_value(
                provider_service_line, "REIMBURSEMENT_SCHEDULE_ID"
            ),
            "PRODUCT_PERIOD": get_obj_value(provider_service_line, "PRODUCT_PERIOD"),
            "SERVICE_PLAN_ID": get_obj_value(provider_service_line, "SERVICE_PLAN_ID"),
            "SERVICE_PLAN_ITEM_NUMBER": get_obj_value(
                provider_service_line, "SERVICE_PLAN_ITEM_NUMBER"
            ),
            "SERVICE_PLAN_AUTH_DATE": get_obj_value(
                provider_service_line, "SERVICE_PLAN_AUTH_DATE"
            ),
            "ORIGINAL_ITEM_NUMBER": get_obj_value(
                provider_service_line, "ORIGINAL_ITEM_NUMBER"
            ),
            "NDC_NUMBER": get_obj_value(provider_service_line, "NDC_NUMBER"),
            "NDC_PRICE": get_obj_value(provider_service_line, "NDC_PRICE"),
            "NDC_UNIT_TYPE": get_obj_value(provider_service_line, "NDC_UNIT_TYPE"),
            "NDC_QUANTITY": get_obj_value(provider_service_line, "NDC_QUANTITY"),
            "NDC_PRESCRIPTION": get_obj_value(
                provider_service_line, "NDC_PRESCRIPTION"
            ),
            "LINE_ITEM_CONTROL_NUMBER": get_obj_value(
                provider_service_line, "LINE_ITEM_CONTROL_NUMBER"
            ),
            "PROVIDER_WRITEOFF_AMOUNT": get_obj_value(
                provider_service_line, "PROVIDER_WRITEOFF_AMOUNT"
            ),
            "BALANCE_DUE_AMOUNT": get_obj_value(
                provider_service_line, "BALANCE_DUE_AMOUNT"
            ),
            "ADJUDICATION_ORDER": get_obj_value(
                provider_service_line, "ADJUDICATION_ORDER"
            ),
            "COB_ADJUSTMENT_AMOUNT": get_obj_value(
                provider_service_line, "COB_ADJUSTMENT_AMOUNT"
            ),
            "QUANTITY_RECEIVED": get_obj_value(
                provider_service_line, "QUANTITY_RECEIVED"
            ),
            "QUANTITY_UNIT": get_obj_value(provider_service_line, "QUANTITY_UNIT"),
            "RENDERING_PROVIDER_NPI": get_obj_value(
                provider_service_line, "RENDERING_PROVIDER_NPI"
            ),
            "RENDERING_PROVIDER_SUPPLEMENTAL_QUALIFIER": get_obj_value(
                provider_service_line, "RENDERING_PROVIDER_SUPPLEMENTAL_QUALIFIER"
            ),
            "RENDERING_PROVIDER_SUPPLEMENTAL_IDENTIFIER": get_obj_value(
                provider_service_line, "RENDERING_PROVIDER_SUPPLEMENTAL_IDENTIFIER"
            ),
            "SUPPLEMENTAL_SERVICE_INFORMATION": get_obj_value(
                provider_service_line, "SUPPLEMENTAL_SERVICE_INFORMATION"
            ),
            "PRODUCT_ID": get_obj_value(provider_service_line, "PRODUCT_ID"),
            "BENEFIT_LEVEL": get_obj_value(provider_service_line, "BENEFIT_LEVEL"),
            "FEE_SCHEDULE_VALUE": get_obj_value(
                provider_service_line, "FEE_SCHEDULE_VALUE"
            ),
            "ALTERNATIVE_FEE_SCHEDULE_ID": get_obj_value(
                provider_service_line, "ALTERNATIVE_FEE_SCHEDULE_ID"
            ),
            "ALTERNATIVE_FEE_SCHEDULE_VALUE": get_obj_value(
                provider_service_line, "ALTERNATIVE_FEE_SCHEDULE_VALUE"
            ),
            "MODIFIER1": modifier1,
            "MODIFIER2": modifier2,
            "MODIFIER3": modifier3,
            "MODIFIER4": modifier4,
            "TYPE_OF_SERVICE": get_obj_value(provider_service_line, "TYPE_OF_SERVICE"),
            "DIAGNOSIS_CODE_INDEX1": diagnosis_code_index1,
            "DIAGNOSIS_CODE_INDEX2": diagnosis_code_index2,
            "DIAGNOSIS_CODE_INDEX3": diagnosis_code_index3,
            "DIAGNOSIS_CODE_INDEX4": diagnosis_code_index4,
            "DIAGNOSIS_CODE_INDEX5": diagnosis_code_index5,
            "DIAGNOSIS_CODE_INDEX6": diagnosis_code_index6,
            "DIAGNOSIS_CODE_INDEX7": diagnosis_code_index7,
            "DIAGNOSIS_CODE_INDEX8": diagnosis_code_index8,
            "REIMBURSEMENT_METHOD": get_obj_value(
                provider_service_line, "REIMBURSEMENT_METHOD"
            ),
        }


provider_claim_mapper = ProviderClaimMapper()
