from datetime import datetime
from pymongo import ASCENDING
from pymongo.operations import UpdateOne
from src.core.migrate.base_etl import BaseEtl
from src.core.service.eligibility.entity import ITherapyEligibility
from src.core.service.eligibility.model import eligibilityModel
from src.core.service.products.entity import ITherapyProduct
from src.core.service.products.model import productsModel
from src.shared.utils.batch import get_total_batch_count
from src.shared.utils.obj import get_obj_value


class EligibilityFixProductAndPatientDobPatch(BaseEtl):
    def __init__(self):
        super().__init__()
        self.batch_size = 100

    def execute(self):
        total_count = eligibilityModel.get_model().count_documents(filter={})
        total_batches = get_total_batch_count(total_count, self.batch_size)
        print(f"Total batches: {total_batches}")

        last_visited_batch_id = None

        for batch_num in range(total_batches):
            print(f"Processing batch {batch_num + 1} of {total_batches}")

            query = (
                {"_id": {"$gt": last_visited_batch_id}} if last_visited_batch_id else {}
            )

            eligibilitiesFromDb = list[ITherapyEligibility](
                eligibilityModel.get_model().find(
                    filter=query,
                    limit=self.batch_size,
                    sort=[("_id", ASCENDING)],
                    projection={
                        "_id": 1,
                        "product": {"referenceId": 1},
                        "patient": {"formattedDob": 1},
                    },
                )
            )

            if not eligibilitiesFromDb:
                break

            last_visited_batch_id = eligibilitiesFromDb[-1]["_id"]
            update_ops = []

            product_reference_ids = set[str]()

            for eligibility in eligibilitiesFromDb:
                product_reference_id = get_obj_value(
                    eligibility, "product", "referenceId"
                )

                if product_reference_id:
                    product_reference_ids.add(product_reference_id)

            productsFromDb = (
                list[ITherapyProduct](
                    productsModel.get_model().find(
                        filter={
                            "product.referenceId": {"$in": list(product_reference_ids)}
                        },
                        projection={"_id": 1, "name": 1, "product": {"referenceId": 1}},
                    )
                )
                if len(product_reference_ids) > 0
                else []
            )

            product_map: dict[str, ITherapyProduct] = {}

            for product in productsFromDb:
                productReferenceId = get_obj_value(product, "product", "referenceId")

                if productReferenceId is not None:
                    product_map[productReferenceId] = product

            for eligibility in eligibilitiesFromDb:
                product_ops = None
                patient_ops = None

                product_reference_id = get_obj_value(
                    eligibility, "product", "referenceId"
                )

                if product_reference_id is not None:
                    product = product_map.get(product_reference_id)

                    if product is not None:
                        product_ops = {"product.name": product["name"]}

                patient_formatted_dob = get_obj_value(
                    eligibility, "patient", "formattedDob"
                )

                if patient_formatted_dob is not None:
                    patient_ops = {
                        "patient.formattedDob": patient_formatted_dob.strftime(
                            "%Y-%m-%d"
                        )
                    }

                if product_ops is not None or patient_ops is not None:
                    update_ops.append(
                        UpdateOne(
                            {"_id": eligibility["_id"]},
                            {
                                "$set": {
                                    **(product_ops or {}),
                                    **(patient_ops or {}),
                                }
                            },
                        )
                    )

            if len(update_ops) > 0:
                eligibilityModel.get_model().bulk_write(update_ops)

            print(
                f"Updated {len(update_ops)} eligibility in batch {batch_num + 1} of {total_batches}"
            )
