"""
test_plate_utils.py
Description: Test the functions related to filling up the plates
Author: Laia Meseguer Monfort
Date: April 28, 2025
"""

# ==================================
# IMPORTS
# ==================================
import pytest
import pandas as pd

from unittest.mock import patch, MagicMock

from local_app.benchling_app import plate_utils

# ==================================
# FUNCTIONS
# ==================================

# ================================== Test find_and_fill_plates ==================================


# ================================== Test fill_plate
@patch("local_app.benchling_app.plate_utils.ContainerQuantity")
@patch("local_app.benchling_app.plate_utils.MultipleContainersTransfer")
def test_fill_plate_success(mock_MultipleContainersTransfer, mock_ContainerQuantity):
    # Create dummy input DataFrames
    plate_pre_recorded_wells_df = pd.DataFrame(
        {
            "well_position_merge": ["A1", "A2"],
            "well_api_id": ["well1", "well2"],
        }
    )

    df = pd.DataFrame(
        {
            "Well Position": ["A1", "A2"],
            "Entity ID": ["entity1", "entity2"],
            "Quantity": [5, 10],
        }
    )

    keep_columns = ["Well Position", "Entity ID", "Quantity"]
    quantity_units = "µL"

    # Setup mocks
    mock_quantity_instance = MagicMock()
    mock_ContainerQuantity.return_value = mock_quantity_instance

    mock_transfer_instance = MagicMock()
    mock_MultipleContainersTransfer.return_value = mock_transfer_instance

    # Call the function
    result = plate_utils.fill_plate(
        plate_pre_recorded_wells_df=plate_pre_recorded_wells_df,
        df=df,
        keep_columns=keep_columns,
        quantity_units=quantity_units,
    )

    # Verify the results
    assert len(result) == 2

    # Check container quantity called correctly
    assert mock_ContainerQuantity.call_count == 2
    assert mock_MultipleContainersTransfer.call_count == 2

    # Check values populated correctly
    for call_args in mock_MultipleContainersTransfer.call_args_list:
        kwargs = call_args.kwargs
        assert "destination_container_id" in kwargs
        assert "source_entity_id" in kwargs
        assert "transfer_quantity" in kwargs

    mock_ContainerQuantity.assert_called()
    mock_MultipleContainersTransfer.assert_called()


# ================================== Test find_and_fill_plates


@patch("local_app.benchling_app.plate_utils.fill_plate")
def test_find_and_fill_plates_success(mock_fill_plate):
    order_number = 1
    app = MagicMock()

    # 1. Prepare three plate IDs + matching suffixes
    plate_ids = ["p1", "p2", "p3"]
    suffixes = ["crRNA", "REC", "SCR"]
    # all share the same well structure
    wells = {
        "a1": MagicMock(barcode="barcode1", name="WellA1", id="well_id_1"),
        "b1": MagicMock(barcode="barcode2", name="WellB1", id="well_id_2"),
    }

    # 2. Create a plate_info mock for each suffix
    plate_infos = []
    for suffix in suffixes:
        pi = MagicMock()
        # Must match regex Plate[A-Z]1_<suffix>
        pi.name = f"PlateA1_{suffix}"
        pi.wells.additional_properties = wells
        plate_infos.append(pi)

    # 3. Make get_by_id return them in sequence
    app.benchling.plates.get_by_id.side_effect = plate_infos

    # 4. Mock fill_plate to return a dummy transfer list
    mock_fill_plate.return_value = ["tx1", "tx2"]

    # 5. Minimal DataFrames for each branch
    crrna_df = pd.DataFrame(
        {
            "well_crrna_idt": ["A01", "B01"],
            "crRNA_b_id": ["crrna1", "crrna2"],
            "ug": [5.0, 10.0],
        }
    )
    receivers_df = pd.DataFrame(
        {
            "well_primer_idt": ["A01", "B01"],
            "receiver_primer_b_id": ["r1", "r2"],
            "ul_primers": [2.0, 4.0],
        }
    )
    screening_df = pd.DataFrame(
        {
            "well_primer_idt": ["A01", "B01"],
            "primer_b_id": ["p1", "p2"],
            "ul_primers": [3.0, 6.0],
        }
    )

    # 6. Call the function
    result = plate_utils.find_and_fill_plates(
        app=app,
        crrna_df_merge=crrna_df,
        receivers_df_merge=receivers_df,
        screening_df_merge=screening_df,
        plate_list=plate_ids,
        order_number=order_number,
    )

    # 7. Should succeed
    assert result is None

    # 8. plates.get_by_id called once per plate
    assert app.benchling.plates.get_by_id.call_count == 3
    for pid in plate_ids:
        app.benchling.plates.get_by_id.assert_any_call(plate_id=pid)

    # 9. fill_plate called once per plate
    assert mock_fill_plate.call_count == 3

    # 10. transfer_into_containers called once per plate
    assert app.benchling.containers.transfer_into_containers.call_count == 3

    # 11. Spot‑check the first DataFrame passed to fill_plate
    first_df = mock_fill_plate.call_args_list[0][0][0]
    assert "well_position_merge" in first_df.columns
    assert first_df["well_position_merge"].tolist() == ["A01", "B01"]


@pytest.fixture
def dummy_dfs():
    return (
        pd.DataFrame({"well_crrna_idt": [], "crRNA_b_id": [], "ug": []}),
        pd.DataFrame(),
        pd.DataFrame(),
    )


@patch("local_app.benchling_app.plate_utils.fill_plate")
def test_find_and_fill_plates_invalid_order(mock_fill_plate, dummy_dfs):
    app = MagicMock()
    # Plate name doesn’t match order_number=1
    bad_plate = MagicMock()
    bad_plate.name = "PlateA2_crRNA"  # 2 ≠ 1

    # ← give it at least one well so DataFrame builds
    dummy_wells = {"a1": MagicMock(barcode="b", name="WellA1", id="well_id")}
    bad_plate.wells.additional_properties = dummy_wells

    app.benchling.plates.get_by_id.return_value = bad_plate

    msg = plate_utils.find_and_fill_plates(
        app=app,
        crrna_df_merge=dummy_dfs[0],
        receivers_df_merge=dummy_dfs[1],
        screening_df_merge=dummy_dfs[2],
        plate_list=["p1"],
        order_number=1,
    )

    assert "Are you sure this plate is from order number 1" in msg
    mock_fill_plate.assert_not_called()


@patch("local_app.benchling_app.plate_utils.fill_plate")
def test_find_and_fill_plates_bad_suffix(mock_fill_plate, dummy_dfs):
    app = MagicMock()
    bad_plate = MagicMock()
    bad_plate.name = "PlateB1_UNKNOWN"  # valid order, bad suffix

    # ← again, at least one well
    dummy_wells = {"a1": MagicMock(barcode="b", name="WellA1", id="well_id")}
    bad_plate.wells.additional_properties = dummy_wells

    app.benchling.plates.get_by_id.return_value = bad_plate

    msg = plate_utils.find_and_fill_plates(
        app=app,
        crrna_df_merge=dummy_dfs[0],
        receivers_df_merge=dummy_dfs[1],
        screening_df_merge=dummy_dfs[2],
        plate_list=["p1"],
        order_number=1,
    )

    assert "should end with _REC, _SCR or _crRNA" in msg
    mock_fill_plate.assert_not_called()


@patch("local_app.benchling_app.plate_utils.fill_plate")
def test_find_and_fill_plates_missing_some(mock_fill_plate, dummy_dfs):
    """If not all three plate types are processed, it should return the 'uploaded all 3 plates' message."""
    app = MagicMock()

    # Create two valid plate_infos: crRNA and REC (missing SRC)
    wells = {
        "a1": MagicMock(barcode="b", name="n", id="i"),
    }
    pi1 = MagicMock()
    pi1.name = "PlateA1_crRNA"
    pi1.wells.additional_properties = wells
    pi2 = MagicMock()
    pi2.name = "PlateA1_REC"
    pi2.wells.additional_properties = wells
    app.benchling.plates.get_by_id.side_effect = [pi1, pi2]

    # Mock fill_plate so it doesn’t error
    mock_fill_plate.return_value = []

    crrna_df = dummy_dfs[0]
    receivers_df = pd.DataFrame(
        {
            "well_primer_idt": ["A01"],
            "receiver_primer_b_id": ["r1"],
            "ul_primers": [1.0],
        }
    )
    screening_df = dummy_dfs[2]

    msg = plate_utils.find_and_fill_plates(
        app=app,
        crrna_df_merge=crrna_df,
        receivers_df_merge=receivers_df,
        screening_df_merge=screening_df,
        plate_list=["p1", "p2"],
        order_number=1,
    )

    assert "Have you uploaded all 3 plates" in msg
    # fill_plate should have been called twice (once per plate)
    assert mock_fill_plate.call_count == 2
