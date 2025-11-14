from benchling_sdk.apps.framework import App
from pathlib import Path
from benchling_sdk.models import CustomEntity
from benchling_sdk.models import CustomEntityCreate
from benchling_sdk.helpers.serialization_helpers import fields
import os
import pandas as pd

from local_app.lib.logger import get_logger

logger = get_logger()


def download_csv(app: App, entit_id: str, destination_dict) -> None:
    """
    Generates a client from an existing App object, and uses this to connect to benchling.
    Finds the ID for the entity, then uses that to get the blob id of the csv, and downloads the CSV.
    """

    logger.info("Downloading file with API ID: %s", entit_id)

    benchling_csv_ent = app.benchling.custom_entities.get_by_id(entit_id)

    # get the blob ID
    blob_id = benchling_csv_ent.fields["CSV"].value
    blob_file_name = benchling_csv_ent.fields["CSV"].display_value  # display_value=
    blob_entity_name = benchling_csv_ent.name

    print("Helllooooooo!!!!!")

    print(blob_file_name)

    if not blob_file_name.endswith(".csv"):
        return f"Are you sure {blob_file_name} has the correct format?"

    path_value = next(
        (value for key, value in destination_dict.items() if key in blob_entity_name),
        None,
    )

    if not path_value:
        return f"This file’s entity name doesn’t match any of the predefined naming conventions. Entitiy name: {blob_entity_name}. File: {blob_file_name}"

    destination_path = Path(path_value)

    destination_path.parent.mkdir(parents=True, exist_ok=True)

    # Download the csv
    _ = app.benchling.blobs.download_file(blob_id, destination_path)

    # print("File dowloaded to " + str(destination_path))
    logger.info("File %s downloaded to: %s", blob_file_name, destination_path)
    return None


def check_all_csv_exist(file_dict):
    missing = []
    for label, path in file_dict.items():
        if not Path(path).is_file():
            missing.append(path)

    if missing:
        return f"Missing files: {missing}"
    else:
        return None


def upload_csv(
    app: App,
    df,
    destination_dict,
    path: Path,
    # new_filename: str,
    # new_entity_name: str,
    # folder_id: str,
    # schema_id= str, #"ts_WDtkRWgc",
):
    # first save the file
    df.to_csv(path, index=False)
    # reac crrna and extract the Order ID
    config = app.config_store.config_by_path

    crrna_df = pd.read_csv(destination_dict["crRNA_metadata"])
    order_number = str(crrna_df["Order ID"].iloc[0])
    new_filename = f"CLC_Plate{order_number}_API_IDs.csv"
    new_entity_name = f"CLC_Plate{order_number}_API_IDs"

    uploaded_blob = app.benchling.blobs.create_from_file(
        Path(path), name=new_filename, mime_type="text/csv"
    )

    entity_fields = fields(
        {
            "CSV": {
                "value": uploaded_blob.id
            }  # Assuming 'CSV' is the schema field for the blob link
        }
    )

    new_entity = CustomEntityCreate(
        name=new_entity_name,
        folder_id=config(["CSV files storage folder"]).required().value_str(),
        schema_id=config(["CSV Entity schema"]).required().value_str(),
        fields=entity_fields,
    )
    # app.benchling.dna_sequences.bulk_create

    created_entity = app.benchling.custom_entities.create(new_entity)
    print(f"Created entity: {created_entity.name} with ID: {created_entity.id}")

    destination_dict["api_ids"] = path

    return destination_dict, order_number, created_entity.id


# def process_csv(file_path):
#     new_row = ["Row3", "Hello again"]
#     with open(file_path, "a", newline="") as file:
#         writer = csv.writer(file)
#         writer.writerow(new_row)
#     print(f"Appended row: {new_row}")


def delete_csvs(file_path):
    os.remove(file_path)
