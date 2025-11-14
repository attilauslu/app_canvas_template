"""
plate_utils.py
Description: Find and fill the plates
Author: Laia Meseguer Monfort
Date: April 22, 2025
"""

# ==================================
# IMPORTS
# ==================================

from benchling_sdk.apps.framework import App
from benchling_sdk.models import (
    ContainerQuantity,
    ContainerQuantityUnits,
    MultipleContainersTransfer,
)
import pandas as pd
import re

# ==================================
# FUNCTIONS
# ==================================


# ================================== Fill plates ==================================
# def format_cell(cell):
#     letter = cell[0].upper()  # Capitalize the letter
#     number = cell[1:].zfill(2)  # Ensure the number has two digits
#     return f"{letter}{number}"


def fill_plate(plate_pre_recorded_wells_df, df, keep_columns, quantity_units):
    df = df[keep_columns]

    plate_pre_recorded_wells_df = pd.merge(
        plate_pre_recorded_wells_df,
        df,
        how="left",
        left_on="well_position_merge",
        right_on=keep_columns[0],
        # ,    suffixes=('', '_idt')  # Left DataFrame keeps original names, right gets '_idt'
    )

    plate_pre_recorded_wells_df = plate_pre_recorded_wells_df.dropna(
        subset=[keep_columns[1]]
    )

    all_container_transfers = []

    for index, row in plate_pre_recorded_wells_df.iterrows():
        container_quantity = ContainerQuantity()

        # Populate the _units and _value attributes
        container_quantity._units = quantity_units  # Use a valid unit from the enum
        container_quantity._value = row[keep_columns[2]]

        all_container_transfers.append(
            MultipleContainersTransfer(
                destination_container_id=row["well_api_id"],
                source_entity_id=row[keep_columns[1]],
                transfer_quantity=container_quantity,
            )
        )

    return all_container_transfers


def find_and_fill_plates(
    app: App,
    crrna_df_merge,
    receivers_df_merge,
    screening_df_merge,
    plate_list,
    order_number,
):
    all_plates_suffix = []
    all_plates_names = []
    for plate_id in plate_list:
        """
        Get plate
        Confirm the type and the data that needs inside if ends in _REC, _SCR or _crRNA


        """
        try:
            plate_info = app.benchling.plates.get_by_id(plate_id=str(plate_id))
        except:
            return f"Are you sure this plate is from order number {plate_list}"
        plate_wells = plate_info.wells.additional_properties
        plate_pre_recorded_wells_data = []

        for key, well in plate_wells.items():

            plate_pre_recorded_wells_data.append(
                {
                    "well_position": key,
                    "barcode": well.barcode,
                    "well_api_name": well.name,
                    "well_api_id": well.id,
                }
            )

        plate_pre_recorded_wells_df = pd.DataFrame(plate_pre_recorded_wells_data)

        plate_pre_recorded_wells_df[
            "well_position_merge"
        ] = plate_pre_recorded_wells_df["well_position"].str[
            0
        ].str.upper() + plate_pre_recorded_wells_df[
            "well_position"
        ].str[
            1:
        ].str.zfill(
            2
        )

        # check also the number in the plate
        pattern = rf"Plate[A-Z]{int(order_number)}"

        if not re.search(pattern, plate_info.name):
            return f"Are you sure this plate is from order number {order_number}: {plate_info.name}"

        plate_suffix = plate_info.name.rsplit("_", 1)[-1]

        if plate_suffix == "crRNA":
            plate_df = crrna_df_merge
            keep_columns = ["well_crrna_idt", "crRNA_b_id", "ug"]
            quantity_units = ContainerQuantityUnits.UG

        elif plate_suffix == "REC":
            plate_df = receivers_df_merge
            keep_columns = ["well_primer_idt", "receiver_primer_b_id", "ul_primers"]
            quantity_units = ContainerQuantityUnits.UL

        elif plate_suffix == "SCR":
            plate_df = screening_df_merge
            keep_columns = ["well_primer_idt", "primer_b_id", "ul_primers"]
            quantity_units = ContainerQuantityUnits.UL

        else:
            return f"Are you sure you uploaded the correct plate? {plate_info.name} should end with _REC, _SCR or _crRNA"

        all_container_transfers = fill_plate(
            plate_pre_recorded_wells_df,
            df=plate_df,
            keep_columns=keep_columns,
            quantity_units=quantity_units,
        )
        app.benchling.containers.transfer_into_containers(
            transfer_requests=all_container_transfers
        )  # [contrainer_transfer_to_do,contrainer_transfer_to_do_2])

        all_plates_suffix.append(plate_suffix)
        all_plates_names.append(plate_info.name)

    # Check that all plates are filled
    if sorted(all_plates_suffix) != sorted(["crRNA", "REC", "SCR"]):
        return f"Have you uploaded all 3 plates (they need different names and different barcodes). You have uploaded {all_plates_names}"

    else:

        return None
