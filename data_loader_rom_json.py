import json
import os
import sys
import xml.etree.ElementTree as ET

import psycopg2
from pymongo import MongoClient


# ============================================================
# CONFIGURATION
# ============================================================

INVENTORY_JSON_PATH = "/path/to/jenkins_xml_inventory.json"

# Directory containing the XML files you exported/downloaded.
XML_ROOT_DIRECTORY = "/path/to/exported/xml/files"


# PostgreSQL
POSTGRES_HOST = "postgres-server"
POSTGRES_PORT = 5432
POSTGRES_DATABASE = "database_name"
POSTGRES_USER = "database_user"
POSTGRES_PASSWORD = "database_password"

POSTGRES_TABLE = "target_table"
POSTGRES_KEY_COLUMN = "xml_identifier"
POSTGRES_VALUE_COLUMN = "new_relationship_value"


# MongoDB
MONGO_URI = "mongodb://mongo-user:mongo-password@mongo-server:27017/"
MONGO_DATABASE = "database_name"
MONGO_COLLECTION = "target_collection"

MONGO_KEY_FIELD = "xmlIdentifier"
MONGO_VALUE_FIELD = "newRelationshipValue"


# Do not perform database writes while True.
DRY_RUN = True


# Commit PostgreSQL updates every N successfully processed XML files.
POSTGRES_COMMIT_BATCH_SIZE = 100


# ============================================================
# XML EXTRACTION
# ============================================================

def strip_namespace(tag):
    """
    Convert:

        {http://example.com/schema}Identifier

    into:

        Identifier
    """
    if "}" in tag:
        return tag.split("}", 1)[1]

    return tag


def find_element_text(root, element_name):
    """
    Find the first XML element with a matching local tag name.

    This works with both namespaced and non-namespaced XML.
    """
    for element in root.iter():
        if strip_namespace(element.tag) == element_name:
            if element.text is not None:
                return element.text.strip()

    return None


def extract_update_data(xml_path, inventory_context):
    """
    Extract the database lookup key and the value to write.

    Customize this function based on the structure of your XML.

    inventory_context contains values such as:
        viewName
        jobName
        fileName
        xmlFolderUrl
        xmlUrl
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # --------------------------------------------------------
    # CUSTOMIZE THIS
    # --------------------------------------------------------

    # Example:
    #
    # <Document>
    #     <Identifier>ABC-123</Identifier>
    # </Document>
    #
    lookup_key = find_element_text(root, "Identifier")

    # In your case, the new value may come from the Jenkins job/folder name.
    new_value = inventory_context["jobName"]

    # Alternatively, the new value could come from the XML:
    #
    # new_value = find_element_text(root, "RelationshipValue")

    if not lookup_key:
        raise ValueError(
            "Could not find Identifier in XML: {}".format(xml_path)
        )

    if not new_value:
        raise ValueError(
            "Could not determine relationship value for: {}".format(xml_path)
        )

    return {
        "lookupKey": lookup_key,
        "newValue": new_value,
    }


# ============================================================
# INVENTORY READING
# ============================================================

def load_inventory():
    with open(INVENTORY_JSON_PATH, "r", encoding="utf-8") as input_file:
        return json.load(input_file)


def iterate_inventory_xml_files(inventory):
    """
    Iterate over the JSON structure produced by the Jenkins inventory script.

    Expected shape:

    {
        "views": [
            {
                "viewName": "...",
                "jobs": [
                    {
                        "jobName": "...",
                        "xmlFiles": [
                            {
                                "fileName": "...",
                                "url": "..."
                            }
                        ]
                    }
                ]
            }
        ]
    }
    """
    for view_record in inventory.get("views", []):
        view_name = view_record.get("viewName")

        for job_record in view_record.get("jobs", []):
            job_name = job_record.get("jobName")
            xml_folder_url = job_record.get("xmlFolderUrl")

            for xml_record in job_record.get("xmlFiles", []):
                file_name = xml_record.get("fileName")

                if not file_name:
                    continue

                yield {
                    "viewName": view_name,
                    "jobName": job_name,
                    "fileName": file_name,
                    "xmlFolderUrl": xml_folder_url,
                    "xmlUrl": xml_record.get("url"),
                    "relativeHref": xml_record.get("relativeHref"),
                }


def find_local_xml_file(context):
    """
    Resolve an inventory XML record to a local file.

    This tries several likely layouts:

        XML_ROOT/file.xml
        XML_ROOT/ViewName/file.xml
        XML_ROOT/ViewName/JobName/file.xml
        XML_ROOT/JobName/file.xml

    Adjust this if your export uses a different directory structure.
    """
    file_name = context["fileName"]
    view_name = context.get("viewName")
    job_name = context.get("jobName")

    candidates = [
        os.path.join(XML_ROOT_DIRECTORY, file_name),
    ]

    if view_name:
        candidates.append(
            os.path.join(
                XML_ROOT_DIRECTORY,
                view_name,
                file_name,
            )
        )

    if job_name:
        candidates.append(
            os.path.join(
                XML_ROOT_DIRECTORY,
                job_name,
                file_name,
            )
        )

    if view_name and job_name:
        candidates.append(
            os.path.join(
                XML_ROOT_DIRECTORY,
                view_name,
                job_name,
                file_name,
            )
        )

    for candidate in candidates:
        if os.path.isfile(candidate):
            return candidate

    return None


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

    database = client[MONGO_DATABASE]
    collection = database[MONGO_COLLECTION]

    return client, collection


# ============================================================
# DATABASE UPDATES
# ============================================================

def update_postgres(cursor, lookup_key, new_value):
    """
    Uses parameterized values to avoid SQL injection.

    Table and column names cannot be passed as regular query parameters,
    so they are constants defined above.
    """
    query = """
        UPDATE {table}
        SET {value_column} = %s
        WHERE {key_column} = %s
    """.format(
        table=POSTGRES_TABLE,
        value_column=POSTGRES_VALUE_COLUMN,
        key_column=POSTGRES_KEY_COLUMN,
    )

    cursor.execute(
        query,
        (
            new_value,
            lookup_key,
        ),
    )

    return cursor.rowcount


def update_mongo(collection, lookup_key, new_value):
    result = collection.update_many(
        {
            MONGO_KEY_FIELD: lookup_key,
        },
        {
            "$set": {
                MONGO_VALUE_FIELD: new_value,
            }
        },
    )

    return result.matched_count, result.modified_count


# ============================================================
# PROCESSING
# ============================================================

def process_xml_file(
    xml_path,
    context,
    postgres_cursor,
    mongo_collection,
):
    update_data = extract_update_data(
        xml_path,
        context,
    )

    lookup_key = update_data["lookupKey"]
    new_value = update_data["newValue"]

    print(
        "XML: {} | key={} | value={}".format(
            xml_path,
            lookup_key,
            new_value,
        )
    )

    if DRY_RUN:
        return {
            "postgresMatched": 0,
            "mongoMatched": 0,
            "mongoModified": 0,
        }

    postgres_matched = update_postgres(
        postgres_cursor,
        lookup_key,
        new_value,
    )

    mongo_matched, mongo_modified = update_mongo(
        mongo_collection,
        lookup_key,
        new_value,
    )

    return {
        "postgresMatched": postgres_matched,
        "mongoMatched": mongo_matched,
        "mongoModified": mongo_modified,
    }


def main():
    inventory = load_inventory()

    postgres_connection = None
    postgres_cursor = None
    mongo_client = None

    totals = {
        "inventoryRecords": 0,
        "processed": 0,
        "missingFiles": 0,
        "errors": 0,
        "postgresMatched": 0,
        "mongoMatched": 0,
        "mongoModified": 0,
    }

    try:
        if not DRY_RUN:
            postgres_connection = open_postgres_connection()
            postgres_cursor = postgres_connection.cursor()

            mongo_client, mongo_collection = open_mongo_connection()
        else:
            mongo_collection = None
            print("DRY_RUN is enabled. No database changes will be made.")

        for context in iterate_inventory_xml_files(inventory):
            totals["inventoryRecords"] += 1

            xml_path = find_local_xml_file(context)

            if xml_path is None:
                totals["missingFiles"] += 1

                print(
                    "MISSING: view={} job={} file={}".format(
                        context.get("viewName"),
                        context.get("jobName"),
                        context.get("fileName"),
                    )
                )

                continue

            try:
                result = process_xml_file(
                    xml_path,
                    context,
                    postgres_cursor,
                    mongo_collection,
                )

                totals["processed"] += 1
                totals["postgresMatched"] += result["postgresMatched"]
                totals["mongoMatched"] += result["mongoMatched"]
                totals["mongoModified"] += result["mongoModified"]

                if (
                    not DRY_RUN
                    and totals["processed"] % POSTGRES_COMMIT_BATCH_SIZE == 0
                ):
                    postgres_connection.commit()

                    print(
                        "Committed PostgreSQL batch at {} processed files".format(
                            totals["processed"]
                        )
                    )

            except Exception as error:
                totals["errors"] += 1

                print(
                    "ERROR processing {}: {}".format(
                        xml_path,
                        error,
                    ),
                    file=sys.stderr,
                )

        if not DRY_RUN:
            postgres_connection.commit()

    except Exception:
        if postgres_connection is not None:
            postgres_connection.rollback()

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
    print("Inventory records: {}".format(totals["inventoryRecords"]))
    print("XML files processed: {}".format(totals["processed"]))
    print("Missing XML files: {}".format(totals["missingFiles"]))
    print("Errors: {}".format(totals["errors"]))
    print("PostgreSQL rows matched: {}".format(totals["postgresMatched"]))
    print("Mongo documents matched: {}".format(totals["mongoMatched"]))
    print("Mongo documents modified: {}".format(totals["mongoModified"]))


if __name__ == "__main__":
    main()
