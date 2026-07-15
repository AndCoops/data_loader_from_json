#!/usr/bin/env python3

import json
import re
import sys

import psycopg2
from psycopg2.extras import execute_values

from pymongo import MongoClient, UpdateOne


# ============================================================
# GENERAL CONFIGURATION
# ============================================================

INVENTORY_JSON_PATH = "/path/to/jenkins_xml_inventory.json"

BATCH_SIZE = 250

# Keep this True until the printed mappings are verified.
DRY_RUN = True


# ============================================================
# TWO-STAGE LOOKUP CONFIGURATION
# ============================================================

# Stage 1:
#
# A recognizable substring in the Jenkins job name maps to an
# intermediate integer ID.
#
# Example:
#
# Jenkins job name:
#     "Production ABC-Customer XML Export"
#
# Partial-match lookup:
#     "ABC-Customer" -> 101
#
JOB_NAME_INTERMEDIATE_ID_LOOKUP = {
    "ABC-Customer": 101,
    "DEF-Customer": 102,
    "Some Other Recognizable Key": 103,
}


# Stage 2:
#
# The intermediate integer ID maps exactly to the final TargetID.
# This final TargetID is written to PostgreSQL and MongoDB.
#
INTERMEDIATE_ID_TARGET_ID_LOOKUP = {
    101: 12345,
    102: 67890,
    103: 24680,
}


# When True, the partial job-name comparison ignores:
#
# - capitalization
# - spaces
# - hyphens
# - underscores
# - punctuation
#
# For example:
#
#   "ABC-Customer"
#   "abc_customer"
#   "ABC Customer"
#
# all normalize to the same comparison value.
NORMALIZE_LOOKUP_MATCHING = True


# ============================================================
# POSTGRESQL CONFIGURATION
# ============================================================

POSTGRES_HOST = "postgres-host"
POSTGRES_PORT = 5432
POSTGRES_DATABASE = "database-name"
POSTGRES_USER = "database-user"
POSTGRES_PASSWORD = "database-password"

POSTGRES_SCHEMA = "public"
POSTGRES_TABLE = "target_table"

# Existing column containing the full XML filename.
POSTGRES_FILENAME_COLUMN = "filename"

# Existing column that should receive the final TargetID.
POSTGRES_TARGET_ID_COLUMN = "target_id"


# ============================================================
# MONGODB CONFIGURATION
# ============================================================

MONGO_URI = "mongodb://mongo-user:mongo-password@mongo-host:27017/"
MONGO_DATABASE = "database-name"
MONGO_COLLECTION = "target_collection"

# First Mongo match field. Its value is always the same.
MONGO_CONSTANT_MATCH_FIELD = "recordType"
MONGO_CONSTANT_MATCH_VALUE = "some-hardcoded-value"

# Second and third Mongo match fields.
# Their values are extracted from the XML filename.
MONGO_SUBSTRING_A_FIELD = "fieldA"
MONGO_SUBSTRING_B_FIELD = "fieldB"

# New field to add or overwrite with the final TargetID.
MONGO_TARGET_ID_FIELD = "targetId"


# ============================================================
# INVENTORY READING
# ============================================================

def load_inventory():
    with open(INVENTORY_JSON_PATH, "r", encoding="utf-8") as input_file:
        return json.load(input_file)


def iterate_inventory_records(inventory):
    """
    Expected Jenkins inventory shape:

    {
        "views": [
            {
                "viewName": "View One",
                "jobs": [
                    {
                        "jobName": "Some Jenkins Job",
                        "xmlFiles": [
                            {
                                "fileName": "Prefix-ValueA_ValueB-End.XML",
                                "url": "http://..."
                            }
                        ]
                    }
                ]
            }
        ]
    }

    The XML files themselves are never opened or downloaded.
    Only jobName and fileName are used.
    """
    for view_record in inventory.get("views", []):
        view_name = view_record.get("viewName")

        for job_record in view_record.get("jobs", []):
            job_name = job_record.get("jobName")

            for xml_record in job_record.get("xmlFiles", []):
                file_name = xml_record.get("fileName")

                if not job_name:
                    print(
                        "Skipping inventory record with no jobName",
                        file=sys.stderr,
                    )
                    continue

                if not file_name:
                    print(
                        "Skipping inventory record with no fileName",
                        file=sys.stderr,
                    )
                    continue

                yield {
                    "viewName": view_name,
                    "jobName": job_name,
                    "fileName": file_name,
                    "xmlUrl": xml_record.get("url"),
                }


# ============================================================
# STAGE 1 AND STAGE 2 LOOKUP LOGIC
# ============================================================

def normalize_lookup_value(value):
    if value is None:
        return ""

    value = value.lower()

    if NORMALIZE_LOOKUP_MATCHING:
        value = re.sub(r"[^a-z0-9]", "", value)

    return value


def resolve_target_id(job_name):
    """
    Resolve the final TargetID through two lookup stages.

    Stage 1:
        Partially match a lookup key against the Jenkins job name.

    Stage 2:
        Use the resulting intermediate integer ID as an exact key
        in INTERMEDIATE_ID_TARGET_ID_LOOKUP.

    The longest matching stage-1 key wins.

    Returns:

        {
            "lookupKey": "ABC-Customer",
            "intermediateId": 101,
            "targetId": 12345
        }

    Returns None if no stage-1 lookup key matches.
    """
    normalized_job_name = normalize_lookup_value(job_name)

    matches = []

    for lookup_key, intermediate_id in (
        JOB_NAME_INTERMEDIATE_ID_LOOKUP.items()
    ):
        normalized_key = normalize_lookup_value(lookup_key)

        if normalized_key and normalized_key in normalized_job_name:
            matches.append({
                "lookupKey": lookup_key,
                "normalizedLength": len(normalized_key),
                "intermediateId": intermediate_id,
            })

    if not matches:
        return None

    # Prefer the longest matching key.
    #
    # This protects against cases such as:
    #
    #     "Customer"
    #     "Customer-East"
    #
    # where both could match the same job name.
    matches.sort(
        key=lambda item: item["normalizedLength"],
        reverse=True,
    )

    best_match = matches[0]

    equally_specific_matches = [
        match
        for match in matches
        if match["normalizedLength"] == best_match["normalizedLength"]
    ]

    equally_specific_ids = set(
        match["intermediateId"]
        for match in equally_specific_matches
    )

    if len(equally_specific_ids) > 1:
        raise ValueError(
            "Ambiguous stage-1 lookup for job {!r}: {}".format(
                job_name,
                [
                    {
                        "lookupKey": match["lookupKey"],
                        "intermediateId": match["intermediateId"],
                    }
                    for match in equally_specific_matches
                ],
            )
        )

    intermediate_id = best_match["intermediateId"]

    if intermediate_id not in INTERMEDIATE_ID_TARGET_ID_LOOKUP:
        raise ValueError(
            "Intermediate ID {!r} from lookup key {!r} has no "
            "stage-2 TargetID mapping".format(
                intermediate_id,
                best_match["lookupKey"],
            )
        )

    target_id = INTERMEDIATE_ID_TARGET_ID_LOOKUP[intermediate_id]

    return {
        "lookupKey": best_match["lookupKey"],
        "intermediateId": intermediate_id,
        "targetId": target_id,
    }


# ============================================================
# FILENAME PARSING FOR MONGODB
# ============================================================

def remove_xml_extension(file_name):
    if file_name.lower().endswith(".xml"):
        return file_name[:-4]

    return file_name


def extract_mongo_match_values(file_name):
    """
    Mongo substring A:

        After the first hyphen
        Before the first underscore

    Mongo substring B:

        After the first underscore
        Before the first hyphen occurring after that underscore

    Example filename:

        Prefix-ValueA_ValueB-Remainder.XML

    Produces:

        substring_a = "ValueA"
        substring_b = "ValueB"
    """
    base_name = remove_xml_extension(file_name)

    first_hyphen_index = base_name.find("-")

    if first_hyphen_index == -1:
        raise ValueError(
            "Filename has no hyphen: {!r}".format(file_name)
        )

    first_underscore_index = base_name.find(
        "_",
        first_hyphen_index + 1,
    )

    if first_underscore_index == -1:
        raise ValueError(
            "Filename has no underscore after its first hyphen: {!r}".format(
                file_name
            )
        )

    next_hyphen_index = base_name.find(
        "-",
        first_underscore_index + 1,
    )

    if next_hyphen_index == -1:
        raise ValueError(
            "Filename has no hyphen after its first underscore: {!r}".format(
                file_name
            )
        )

    substring_a = base_name[
        first_hyphen_index + 1:first_underscore_index
    ]

    substring_b = base_name[
        first_underscore_index + 1:next_hyphen_index
    ]

    if not substring_a:
        raise ValueError(
            "Filename produced an empty Mongo substring A: {!r}".format(
                file_name
            )
        )

    if not substring_b:
        raise ValueError(
            "Filename produced an empty Mongo substring B: {!r}".format(
                file_name
            )
        )

    return substring_a, substring_b


# ============================================================
# PREPARE UPDATE RECORD
# ============================================================

def build_update_record(inventory_record):
    job_name = inventory_record["jobName"]
    file_name = inventory_record["fileName"]

    lookup_result = resolve_target_id(job_name)

    if lookup_result is None:
        raise ValueError(
            "No stage-1 job-name lookup matched {!r}".format(job_name)
        )

    substring_a, substring_b = extract_mongo_match_values(
        file_name
    )

    return {
        "viewName": inventory_record.get("viewName"),
        "jobName": job_name,
        "fileName": file_name,
        "xmlUrl": inventory_record.get("xmlUrl"),

        # Stage 1 result.
        "lookupKey": lookup_result["lookupKey"],
        "intermediateId": lookup_result["intermediateId"],

        # Stage 2 result.
        # This is the value written to both databases.
        "targetId": lookup_result["targetId"],

        # Mongo filter values parsed from the filename.
        "mongoSubstringA": substring_a,
        "mongoSubstringB": substring_b,
    }


# ============================================================
# BATCH GENERATION
# ============================================================

def generate_batches(records, batch_size):
    batch = []

    for record in records:
        batch.append(record)

        if len(batch) >= batch_size:
            yield batch
            batch = []

    if batch:
        yield batch


# ============================================================
# DATABASE CONNECTIONS
# ============================================================

def open_postgres_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DATABASE,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def open_mongo_connection():
    client = MongoClient(MONGO_URI)

    collection = client[
        MONGO_DATABASE
    ][
        MONGO_COLLECTION
    ]

    return client, collection


# ============================================================
# POSTGRESQL BATCH UPDATE
# ============================================================

def run_postgres_batch(cursor, batch):
    """
    For every batch record, perform the conceptual update:

        UPDATE target_table
        SET target_id = final_target_id
        WHERE filename = xml_filename

    execute_values builds one VALUES-based batch statement rather
    than issuing one SQL command per filename.
    """
    values = [
        (
            record["fileName"],
            record["targetId"],
        )
        for record in batch
    ]

    query = """
        UPDATE {schema}.{table} AS target
        SET {target_id_column} = incoming.target_id
        FROM (
            VALUES %s
        ) AS incoming(file_name, target_id)
        WHERE target.{filename_column} = incoming.file_name
    """.format(
        schema=POSTGRES_SCHEMA,
        table=POSTGRES_TABLE,
        target_id_column=POSTGRES_TARGET_ID_COLUMN,
        filename_column=POSTGRES_FILENAME_COLUMN,
    )

    execute_values(
        cursor,
        query,
        values,
        template="(%s, %s)",
    )

    return cursor.rowcount


# ============================================================
# MONGODB BATCH UPDATE
# ============================================================

def build_mongo_operations(batch):
    """
    Each operation matches three fields:

        1. A constant field/value.
        2. Filename-derived substring A.
        3. Filename-derived substring B.

    It then adds or replaces the configured TargetID field.
    """
    operations = []

    for record in batch:
        mongo_filter = {
            MONGO_CONSTANT_MATCH_FIELD: MONGO_CONSTANT_MATCH_VALUE,
            MONGO_SUBSTRING_A_FIELD: record["mongoSubstringA"],
            MONGO_SUBSTRING_B_FIELD: record["mongoSubstringB"],
        }

        mongo_update = {
            "$set": {
                MONGO_TARGET_ID_FIELD: record["targetId"],
            }
        }

        operations.append(
            UpdateOne(
                mongo_filter,
                mongo_update,
                upsert=False,
            )
        )

    return operations


def run_mongo_batch(collection, batch):
    operations = build_mongo_operations(batch)

    if not operations:
        return {
            "matched": 0,
            "modified": 0,
            "upserted": 0,
        }

    result = collection.bulk_write(
        operations,
        ordered=False,
    )

    return {
        "matched": result.matched_count,
        "modified": result.modified_count,
        "upserted": len(result.upserted_ids),
    }


# ============================================================
# DRY-RUN DISPLAY
# ============================================================

def print_batch_preview(batch, batch_number):
    print("")
    print(
        "DRY RUN - batch {} containing {} record(s)".format(
            batch_number,
            len(batch),
        )
    )

    for record in batch:
        print(
            "  file={!r} | "
            "job={!r} | "
            "lookupKey={!r} | "
            "intermediateId={!r} | "
            "targetId={!r} | "
            "mongoA={!r} | "
            "mongoB={!r}".format(
                record["fileName"],
                record["jobName"],
                record["lookupKey"],
                record["intermediateId"],
                record["targetId"],
                record["mongoSubstringA"],
                record["mongoSubstringB"],
            )
        )


# ============================================================
# OPTIONAL DUPLICATE VALIDATION
# ============================================================

def validate_prepared_records(records):
    """
    Detect conflicting duplicate update instructions.

    A duplicate filename is acceptable only if every occurrence maps
    to the same final TargetID.

    A duplicate Mongo filter is acceptable only if every occurrence
    maps to the same final TargetID.
    """
    postgres_targets = {}
    mongo_targets = {}

    for record in records:
        file_name = record["fileName"]
        target_id = record["targetId"]

        if file_name in postgres_targets:
            previous_target_id = postgres_targets[file_name]

            if previous_target_id != target_id:
                raise ValueError(
                    "Conflicting PostgreSQL mappings for filename {!r}: "
                    "{!r} and {!r}".format(
                        file_name,
                        previous_target_id,
                        target_id,
                    )
                )
        else:
            postgres_targets[file_name] = target_id

        mongo_key = (
            MONGO_CONSTANT_MATCH_VALUE,
            record["mongoSubstringA"],
            record["mongoSubstringB"],
        )

        if mongo_key in mongo_targets:
            previous_target_id = mongo_targets[mongo_key]

            if previous_target_id != target_id:
                raise ValueError(
                    "Conflicting Mongo mappings for filter {!r}: "
                    "{!r} and {!r}".format(
                        mongo_key,
                        previous_target_id,
                        target_id,
                    )
                )
        else:
            mongo_targets[mongo_key] = target_id


# ============================================================
# MAIN
# ============================================================

def main():
    inventory = load_inventory()

    prepared_records = []

    totals = {
        "inventoryRecords": 0,
        "preparedRecords": 0,
        "skippedRecords": 0,
        "batches": 0,
        "postgresMatched": 0,
        "mongoMatched": 0,
        "mongoModified": 0,
        "mongoUpserted": 0,
    }

    print("Reading Jenkins inventory...")

    for inventory_record in iterate_inventory_records(inventory):
        totals["inventoryRecords"] += 1

        try:
            update_record = build_update_record(
                inventory_record
            )

            prepared_records.append(update_record)
            totals["preparedRecords"] += 1

        except Exception as error:
            totals["skippedRecords"] += 1

            print(
                "SKIPPED: file={!r}, job={!r}, reason={}".format(
                    inventory_record.get("fileName"),
                    inventory_record.get("jobName"),
                    error,
                ),
                file=sys.stderr,
            )

    print(
        "Prepared {} update record(s).".format(
            len(prepared_records)
        )
    )

    # Stop before database work if contradictory duplicate mappings
    # were generated.
    validate_prepared_records(prepared_records)

    postgres_connection = None
    postgres_cursor = None
    mongo_client = None
    mongo_collection = None

    try:
        if DRY_RUN:
            print(
                "DRY_RUN is enabled. No database changes will be made."
            )
        else:
            postgres_connection = open_postgres_connection()
            postgres_cursor = postgres_connection.cursor()

            mongo_client, mongo_collection = open_mongo_connection()

        batches = generate_batches(
            prepared_records,
            BATCH_SIZE,
        )

        for batch_number, batch in enumerate(
            batches,
            start=1,
        ):
            totals["batches"] += 1

            if DRY_RUN:
                print_batch_preview(
                    batch,
                    batch_number,
                )
                continue

            print("")
            print(
                "Running batch {} containing {} record(s)...".format(
                    batch_number,
                    len(batch),
                )
            )

            try:
                # PostgreSQL changes remain uncommitted until after
                # the Mongo batch succeeds.
                postgres_matched = run_postgres_batch(
                    postgres_cursor,
                    batch,
                )

                mongo_result = run_mongo_batch(
                    mongo_collection,
                    batch,
                )

                postgres_connection.commit()

                totals["postgresMatched"] += postgres_matched
                totals["mongoMatched"] += mongo_result["matched"]
                totals["mongoModified"] += mongo_result["modified"]
                totals["mongoUpserted"] += mongo_result["upserted"]

                print(
                    "  PostgreSQL rows matched: {}".format(
                        postgres_matched
                    )
                )

                print(
                    "  Mongo documents matched: {}".format(
                        mongo_result["matched"]
                    )
                )

                print(
                    "  Mongo documents modified: {}".format(
                        mongo_result["modified"]
                    )
                )

            except Exception:
                postgres_connection.rollback()

                print(
                    "Batch {} failed. PostgreSQL was rolled back.".format(
                        batch_number
                    ),
                    file=sys.stderr,
                )

                raise

    finally:
        if postgres_cursor is not None:
            postgres_cursor.close()

        if postgres_connection is not None:
            postgres_connection.close()

        if mongo_client is not None:
            mongo_client.close()

    print("")
    print("Finished")
    print(
        "Inventory records: {}".format(
            totals["inventoryRecords"]
        )
    )
    print(
        "Prepared records: {}".format(
            totals["preparedRecords"]
        )
    )
    print(
        "Skipped records: {}".format(
            totals["skippedRecords"]
        )
    )
    print(
        "Batches: {}".format(
            totals["batches"]
        )
    )
    print(
        "PostgreSQL rows matched: {}".format(
            totals["postgresMatched"]
        )
    )
    print(
        "Mongo documents matched: {}".format(
            totals["mongoMatched"]
        )
    )
    print(
        "Mongo documents modified: {}".format(
            totals["mongoModified"]
        )
    )


if __name__ == "__main__":
    main()
