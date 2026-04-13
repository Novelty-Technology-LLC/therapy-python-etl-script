"""
Backfill / normalize formatted date fields to MM/DD/YYYY (e.g. 05/25/1976) on
enrollee, patient, subscriber, and eligibility documents.

Field mapping (see service mappers under src/core/service/*/mapper.py):
  enrollees:   demographic.formattedDob <- demographic.dob
               additionalInformation.formattedDeathDate <- additionalInformation.deathDate
  patients:    demographic.formattedDob <- demographic.dob
  subscribers: demographic.formattedDob, employment.formattedStartDate,
               employment.formattedTerminationDate
  eligibility: patient.formattedDob, serviceDate.formattedStartDate/EndDate,
               additionalInformation.paidThrough.formattedDate,
               additionalInformation.terminationReason.formattedEventDate

Collections:
  therapy: enrollees, patients, subscribers, eligibilities (CollectionName.THERAPY_*)
  test:    PYTHON_TEST_ENROLLEE, PYTHON_TEST_PATIENT, PYTHON_TEST_SUBSCRIBER,
           PYTHON_TEST_ELIGIBILITY (CollectionName.*)

Run:
  python main.py --execute UPDATE_FORMATTED_DATES
  python -m src.core.migrate.script.update_formatted_dates_patch --mode therapy
  python -m src.core.migrate.script.update_formatted_dates_patch --mode test --dry-run
"""

from __future__ import annotations

import argparse
from typing import Any, Optional

from pymongo import ASCENDING
from pymongo.operations import UpdateOne

from src.core.migrate.base_etl import BaseEtl
from src.shared.constant.collection_name import CollectionName
from src.shared.constant.constant import BATCH_SIZE
from src.shared.helper.mongodb_helper import mongodb_helper
from src.shared.utils.batch import get_total_batch_count
from src.shared.utils.date import from_string_to_formatted_date
from src.shared.utils.obj import get_obj_value


def _fmt(source: Any) -> Optional[str]:
    """Format a date-like value to MM/DD/YYYY; None if source is missing."""
    if source is None:
        return None
    return from_string_to_formatted_date(source)


def _build_enrollee_patient_updates(doc: dict) -> dict[str, Any]:
    """Align with enrollees/patients mappers: demographic + optional enrollee deathDate."""
    out: dict[str, Any] = {}

    dob = get_obj_value(doc, "demographic", "dob")
    if dob is not None:
        new_fmt = _fmt(dob)
        if new_fmt is not None:
            out["demographic.formattedDob"] = new_fmt

    death = get_obj_value(doc, "additionalInformation", "deathDate")
    if death is not None:
        fd = _fmt(death)
        if fd is not None:
            out["additionalInformation.formattedDeathDate"] = fd

    return out


def _build_subscriber_updates(doc: dict) -> dict[str, Any]:
    out: dict[str, Any] = {}
    dob = get_obj_value(doc, "demographic", "dob")
    if dob is not None:
        fd = _fmt(dob)
        if fd is not None:
            out["demographic.formattedDob"] = fd

    start = get_obj_value(doc, "employment", "startDate")
    if start is not None:
        fs = _fmt(start)
        if fs is not None:
            out["employment.formattedStartDate"] = fs

    end = get_obj_value(doc, "employment", "endDate")
    if end is not None:
        ft = _fmt(end)
        if ft is not None:
            out["employment.formattedTerminationDate"] = ft

    return out


def _build_eligibility_updates(doc: dict) -> dict[str, Any]:
    """Align with eligibility mapper: patient, serviceDate, paidThrough, terminationReason."""
    out: dict[str, Any] = {}

    pdob = get_obj_value(doc, "patient", "dob")
    if pdob is not None:
        fd = _fmt(pdob)
        if fd is not None:
            out["patient.formattedDob"] = fd

    s_start = get_obj_value(doc, "serviceDate", "startDate")
    if s_start is not None:
        fs = _fmt(s_start)
        if fs is not None:
            out["serviceDate.formattedStartDate"] = fs

    s_end = get_obj_value(doc, "serviceDate", "endDate")
    if s_end is not None:
        fe = _fmt(s_end)
        if fe is not None:
            out["serviceDate.formattedEndDate"] = fe

    paid = get_obj_value(doc, "additionalInformation", "paidThrough", "date")
    if paid is not None:
        fp = _fmt(paid)
        if fp is not None:
            out["additionalInformation.paidThrough.formattedDate"] = fp

    term_ev = get_obj_value(doc, "additionalInformation", "terminationReason", "eventDate")
    if term_ev is not None:
        ft = _fmt(term_ev)
        if ft is not None:
            out["additionalInformation.terminationReason.formattedEventDate"] = ft

    return out


_BUILDERS = {
    "enrollee": _build_enrollee_patient_updates,
    "patient": _build_enrollee_patient_updates,
    "subscriber": _build_subscriber_updates,
    "eligibility": _build_eligibility_updates,
}

# Only scan documents that have at least one source date field to format.
_FILTERS: dict[str, dict[str, Any]] = {
    "enrollee": {
        "$or": [
            {"demographic.dob": {"$exists": True, "$ne": None}},
            {"additionalInformation.deathDate": {"$exists": True, "$ne": None}},
        ]
    },
    "patient": {"demographic.dob": {"$exists": True, "$ne": None}},
    "subscriber": {
        "$or": [
            {"demographic.dob": {"$exists": True, "$ne": None}},
            {"employment.startDate": {"$exists": True, "$ne": None}},
            {"employment.endDate": {"$exists": True, "$ne": None}},
        ]
    },
    "eligibility": {
        "$or": [
            {"patient.dob": {"$exists": True, "$ne": None}},
            {"serviceDate.startDate": {"$exists": True, "$ne": None}},
            {"serviceDate.endDate": {"$exists": True, "$ne": None}},
            {"additionalInformation.paidThrough.date": {"$exists": True, "$ne": None}},
            {
                "additionalInformation.terminationReason.eventDate": {
                    "$exists": True,
                    "$ne": None,
                }
            },
        ]
    },
}


def _query_with_cursor(base_filter: dict[str, Any], last_id: Optional[Any]) -> dict[str, Any]:
    """Append _id cursor for stable pagination."""
    if last_id is None:
        return base_filter
    if "$or" in base_filter or "$and" in base_filter:
        return {"$and": [base_filter, {"_id": {"$gt": last_id}}]}
    return {**base_filter, "_id": {"$gt": last_id}}


def _resolve_collections(mode: str) -> list[tuple[str, str]]:
    if mode == "therapy":
        return [
            (CollectionName.THERAPY_ENROLLEE, "enrollee"),
            (CollectionName.THERAPY_PATIENT, "patient"),
            (CollectionName.THERAPY_SUBSCRIBER, "subscriber"),
            (CollectionName.THERAPY_ELIGIBILITY, "eligibility"),
        ]
    if mode == "test":
        return [
            (CollectionName.ENROLLEE, "enrollee"),
            (CollectionName.PATIENTS, "patient"),
            (CollectionName.SUBSCRIBER, "subscriber"),
            (CollectionName.ELIGIBILITY, "eligibility"),
        ]
    raise ValueError(f"mode must be 'therapy' or 'test', got {mode!r}")


class UpdateFormattedDatesPatch(BaseEtl):
    """Normalize formatted* date strings to MM/DD/YYYY from sibling *Date fields."""

    def __init__(self) -> None:
        super().__init__()
        self.batch_size = BATCH_SIZE

    def execute(self, mode: str = "therapy", dry_run: bool = False) -> None:
        pairs = _resolve_collections(mode)
        print(f"Update formatted dates — mode={mode!r}, dry_run={dry_run}, batch_size={self.batch_size}")

        for collection_name, kind in pairs:
            self._process_collection(collection_name, kind, dry_run)

    def _process_collection(self, collection_name: str, kind: str, dry_run: bool) -> None:
        coll = mongodb_helper._get_database()[collection_name]
        builder = _BUILDERS[kind]
        base_filter = _FILTERS[kind]

        total = coll.count_documents(base_filter)
        total_batches = max(1, get_total_batch_count(total, self.batch_size)) if total else 0
        print(f"  [{collection_name}] kind={kind} matching ~{total} doc(s), ~{total_batches} batch(es)")

        last_id = None
        updated_total = 0
        scanned = 0

        while True:
            q = _query_with_cursor(base_filter, last_id)
            cursor = coll.find(
                q,
                projection={
                    "_id": 1,
                    "demographic": 1,
                    "employment": 1,
                    "patient": 1,
                    "serviceDate": 1,
                    "additionalInformation": 1,
                },
                sort=[("_id", ASCENDING)],
                limit=self.batch_size,
            )
            batch = list(cursor)
            if not batch:
                break

            last_id = batch[-1]["_id"]
            ops: list[UpdateOne] = []

            for doc in batch:
                scanned += 1
                fields = builder(doc)
                if not fields:
                    continue
                ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": fields}))

            if ops and not dry_run:
                coll.bulk_write(ops, ordered=False)
                updated_total += len(ops)

            if ops:
                print(f"    batch: matched {len(batch)} scanned, {len(ops)} updates" + (" (dry-run, not applied)" if dry_run else ""))

        print(f"  [{collection_name}] done — scanned ~{scanned}, updates {'would be ' if dry_run else ''}{updated_total}")


def main_cli() -> None:
    parser = argparse.ArgumentParser(description="Normalize formatted dates to MM/DD/YYYY.")
    parser.add_argument(
        "--mode",
        choices=("therapy", "test"),
        default="therapy",
        help="therapy = production collection names; test = PYTHON_TEST_* collections",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute updates but do not write to MongoDB",
    )
    args = parser.parse_args()

    with UpdateFormattedDatesPatch() as etl:
        etl.execute(mode=args.mode, dry_run=args.dry_run)


if __name__ == "__main__":
    main_cli()
