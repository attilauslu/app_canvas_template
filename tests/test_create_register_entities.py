"""
test_csv_utils.py
Description: Test the functions related to csv files managment
Author: Laia Meseguer Monfort
Date: April 25, 2025
"""

# ==================================
# IMPORTS
# ==================================
import pandas as pd
import pytest

from unittest.mock import patch, MagicMock

from local_app.benchling_app import create_register_entites


# ==================================
# FUNCTIONS
# ==================================

# ================================== testing load_and_clean_data ==================================
# ================================== testing read_and_basic_qc


def test_successful_read_and_qc():
    df_mock = pd.DataFrame({"crRNA_id": ["abc"], "BGC_number": ["001"]})
    min_cols = ["crRNA_id", "BGC_number"]

    with patch(
        "local_app.benchling_app.create_register_entites.pd.read_csv",
        return_value=df_mock,
    ):
        df, error = create_register_entites.read_and_basic_qc("fake/path.csv", min_cols)

    assert error is None
    assert not df.empty
    assert list(df.columns) == min_cols


def test_cannot_read_csv():
    min_cols = ["crRNA_id", "BGC_number"]

    with patch(
        "local_app.benchling_app.create_register_entites.pd.read_csv",
        side_effect=Exception("File error"),
    ):
        df, error = create_register_entites.read_and_basic_qc("bad/path.csv", min_cols)

    assert df.empty
    assert "Can't read" in error


def test_missing_columns():
    df_mock = pd.DataFrame({"crRNA_id": ["abc"]})
    min_cols = ["crRNA_id", "BGC_number"]

    with patch(
        "local_app.benchling_app.create_register_entites.pd.read_csv",
        return_value=df_mock,
    ):
        df, error = create_register_entites.read_and_basic_qc(
            "missing_col.csv", min_cols
        )

    assert df.empty
    assert "Missing required columns" in error
    assert "BGC_number" in error


# ================================== testing normalize_plate_df


def test_normalize_plate_df_success():
    input_df = pd.DataFrame(
        {
            "Well Position": ["A1", "B1"],
            "Sequence Name": ["cr1", "cr2"],
            "Sequence": ["ATCG", "GCTA"],
            "Extra Column": ["x", "y"],
        }
    )

    columns_to_keep = ["Well Position", "Sequence Name", "Sequence"]
    rename_map = {
        "Well Position": "well_id",
        "Sequence Name": "crRNA_id",
        "Sequence": "sequence",
    }

    result = create_register_entites.normalize_plate_df(
        input_df, columns_to_keep, rename_map
    )

    assert list(result.columns) == ["well_id", "crRNA_id", "sequence"]
    assert result.shape == (2, 3)
    assert result.iloc[0]["crRNA_id"] == "cr1"


def test_normalize_plate_df_missing_column():
    input_df = pd.DataFrame({"Well Position": ["A1"], "Sequence": ["ATCG"]})
    columns_to_keep = ["Well Position", "Sequence Name", "Sequence"]
    rename_map = {}

    with pytest.raises(KeyError):
        create_register_entites.normalize_plate_df(
            input_df, columns_to_keep, rename_map
        )


# ================================== testing  validate_plate


def base_inputs():
    metadata_df = pd.DataFrame(
        {
            "crRNA_id": ["A", "B"],
            "Well Position": ["A1", "B1"],
            "crRNA": ["ATCG", "GCTA"],
        }
    )

    plate_specs_df = pd.DataFrame(
        {
            "crRNA_id": ["A", "B"],
            "well_crrna_idt": ["A1", "B1"],
            "crrna_seq_idt": ["ATCG", "GCTA"],
        }
    )

    merged_df = pd.merge(metadata_df, plate_specs_df, on="crRNA_id")

    return metadata_df, plate_specs_df, merged_df


def test_validate_plate_success():
    metadata_df, plate_specs_df, merged_df = base_inputs()

    result, message = create_register_entites.validate_plate(
        metadata_df,
        plate_specs_df,
        merged_df,
        original_id_col="crRNA_id",
        spec_id_col="crRNA_id",
        original_well_col="Well Position",
        spec_well_col="well_crrna_idt",
        original_seq_col="crRNA",
        spec_seq_col="crrna_seq_idt",
        reference_label="crRNA",
    )

    assert result is True
    assert message == ""


def test_validate_plate_missing_id():
    metadata_df, plate_specs_df, merged_df = base_inputs()
    metadata_df.loc[2] = ["C", "C1", "CGAT"]  # Add a crRNA_id "C" not in specs

    result, message = create_register_entites.validate_plate(
        metadata_df,
        plate_specs_df,
        merged_df,
        original_id_col="crRNA_id",
        spec_id_col="crRNA_id",
        original_well_col="Well Position",
        spec_well_col="well_crrna_idt",
        original_seq_col="crRNA",
        spec_seq_col="crrna_seq_idt",
        reference_label="crRNA",
    )

    assert result is False
    assert "Missing the following crRNA" in message


def test_validate_plate_mismatched_well():
    metadata_df, plate_specs_df, merged_df = base_inputs()
    merged_df.loc[0, "Well Position"] = "Z9"  # mismatch well position

    result, message = create_register_entites.validate_plate(
        metadata_df,
        plate_specs_df,
        merged_df,
        original_id_col="crRNA_id",
        spec_id_col="crRNA_id",
        original_well_col="Well Position",
        spec_well_col="well_crrna_idt",
        original_seq_col="crRNA",
        spec_seq_col="crrna_seq_idt",
        reference_label="crRNA",
    )

    assert result is False
    assert "has changed wells" in message


def test_validate_plate_mismatched_sequence():
    metadata_df, plate_specs_df, merged_df = base_inputs()
    merged_df.loc[0, "crRNA"] = "TTTT"  # mismatch sequence

    result, message = create_register_entites.validate_plate(
        metadata_df,
        plate_specs_df,
        merged_df,
        original_id_col="crRNA_id",
        spec_id_col="crRNA_id",
        original_well_col="Well Position",
        spec_well_col="well_crrna_idt",
        original_seq_col="crRNA",
        spec_seq_col="crrna_seq_idt",
        reference_label="crRNA",
    )

    assert result is False
    assert "has different sequence" in message


# ================================== testing  cleaning_crrna
@patch("local_app.benchling_app.create_register_entites.validate_plate")
@patch("local_app.benchling_app.create_register_entites.normalize_plate_df")
def test_cleaning_crrna_success(mock_normalize, mock_validate):
    # Mock normalized plate spec
    plate_mock = pd.DataFrame(
        {
            "crRNA_id": ["cr1"],
            "well_crrna_idt": ["A1"],
            "crrna_seq_idt_original": [
                "/AltR1/rUrArA rUrUrU rCrUrA rCrUrA rArGrU rGrUrA rGrArU ATCG /AltR2/"
            ],
            "ug": [1.0],
        }
    )

    mock_normalize.return_value = plate_mock

    # crRNA metadata input
    crrna_df = pd.DataFrame(
        {
            "crRNA_id": ["cr1"],
            "Well Position": ["A1"],
            "crRNA": ["ATCG"],
            "BGC_number": ["001"],
            "strain_name": ["strainA"],
            "crRNA_strand": ["+"],
            "crRNA_loc": [123],
        }
    )

    # Merge will succeed due to ID match
    mock_validate.return_value = (True, "")

    result_df, error_msg = create_register_entites.cleaning_crrna(crrna_df, plate_mock)

    assert error_msg == ""
    assert not result_df.empty
    assert "crRNA_id" in result_df.columns


@patch("local_app.benchling_app.create_register_entites.validate_plate")
@patch("local_app.benchling_app.create_register_entites.normalize_plate_df")
def test_cleaning_crrna_validation_fails(mock_normalize, mock_validate):
    mock_normalize.return_value = pd.DataFrame(
        {
            "crRNA_id": ["cr1"],
            "well_crrna_idt": ["A1"],
            "crrna_seq_idt_original": ["ATCG"],
            "ug": [1.0],
        }
    )

    crrna_df = pd.DataFrame(
        {
            "crRNA_id": ["cr1"],
            "Well Position": ["A1"],
            "crRNA": ["ATCG"],
            "BGC_number": ["001"],
            "strain_name": ["strainA"],
            "crRNA_strand": ["+"],
            "crRNA_loc": [123],
        }
    )

    mock_validate.return_value = (False, "Validation error: sequence mismatch")

    result_df, error_msg = create_register_entites.cleaning_crrna(
        crrna_df, mock_normalize.return_value
    )

    assert result_df.empty
    assert "Validation error" in error_msg


# ================================== testing cleaning_receivers


@patch("local_app.benchling_app.create_register_entites.validate_plate")
@patch("local_app.benchling_app.create_register_entites.normalize_plate_df")
def test_cleaning_receivers_success(mock_normalize, mock_validate):
    # Fake normalized plate spec
    normalized_plate_df = pd.DataFrame(
        {
            "primer_id": ["P1"],
            "well_primer_idt": ["A1"],
            "primer_seq_idt": ["ATCG"],
            "ul_primers": [10.0],
        }
    )
    mock_normalize.return_value = normalized_plate_df

    # Metadata input
    receivers_df = pd.DataFrame(
        {
            "receiver_primer_id": ["P1"],
            "receiver_primer_seq": ["ATCG"],
            "BGC_number": [1],
        }
    )

    mock_validate.return_value = (True, "")

    result_df, plate_df, error = create_register_entites.cleaning_receivers(
        receivers_df, normalized_plate_df
    )

    assert error == ""
    assert not result_df.empty
    assert "receiver_primer_id" in result_df.columns
    assert plate_df.equals(normalized_plate_df)


@patch("local_app.benchling_app.create_register_entites.validate_plate")
@patch("local_app.benchling_app.create_register_entites.normalize_plate_df")
def test_cleaning_receivers_validation_fails(mock_normalize, mock_validate):
    normalized_plate_df = pd.DataFrame(
        {
            "primer_id": ["P1"],
            "well_primer_idt": ["A1"],
            "primer_seq_idt": ["ATCG"],
            "ul_primers": [10.0],
        }
    )
    mock_normalize.return_value = normalized_plate_df

    receivers_df = pd.DataFrame(
        {
            "receiver_primer_id": ["P1"],
            "receiver_primer_seq": ["GCTA"],
            "BGC_number": [1],
        }
    )

    mock_validate.return_value = (False, "Sequence mismatch")

    result_df, plate_df, error = create_register_entites.cleaning_receivers(
        receivers_df, normalized_plate_df
    )

    assert result_df.empty
    assert plate_df.empty
    assert "Sequence mismatch" in error


# ================================== testing cleaning_screening


@patch("local_app.benchling_app.create_register_entites.validate_plate")
def test_cleaning_screening_success(mock_validate):
    screening_df = pd.DataFrame(
        {
            "locus tag": ["CLC001C"],
            "f_primer_name": ["P1"],
            "f_primer_sequences(5-3)": ["ATCG"],
            "f_well_position": ["A1"],
            "r_primer_name": ["P2"],
            "r_primer_sequences(5-3)": ["CGTA"],
            "r_well_position": ["B1"],
        }
    )

    primers_plate_specs_df = pd.DataFrame(
        {
            "primer_id": ["P1", "P2"],
            "primer_seq_idt": ["ATCG", "CGTA"],
            "well_primer_idt": ["A1", "B1"],
        }
    )

    mock_validate.return_value = (True, "")

    cleaned_df, error = create_register_entites.cleaning_screening(
        screening_df, primers_plate_specs_df, only_make_idt_excel_for_C_primers=True
    )

    assert error == ""
    assert not cleaned_df.empty
    assert "primer_name" in cleaned_df.columns
    assert set(cleaned_df["primer_name"]) == {"P1", "P2"}


@patch("local_app.benchling_app.create_register_entites.validate_plate")
def test_cleaning_screening_validation_fails(mock_validate):
    screening_df = pd.DataFrame(
        {
            "locus tag": ["CLC001C"],
            "f_primer_name": ["P1"],
            "f_primer_sequences(5-3)": ["ATCG"],
            "f_well_position": ["A1"],
            "r_primer_name": ["P2"],
            "r_primer_sequences(5-3)": ["CGTA"],
            "r_well_position": ["B1"],
        }
    )

    primers_plate_specs_df = pd.DataFrame(
        {
            "primer_id": ["P1", "P2"],
            "primer_seq_idt": ["ATCG", "CGTA"],
            "well_primer_idt": ["A1", "B1"],
        }
    )

    mock_validate.return_value = (False, "Sequence mismatch")

    cleaned_df, error = create_register_entites.cleaning_screening(
        screening_df, primers_plate_specs_df, only_make_idt_excel_for_C_primers=True
    )

    assert cleaned_df.empty
    assert "Sequence mismatch" in error


# ================================== testing load_and_clean_data


@patch("local_app.benchling_app.create_register_entites.cleaning_screening")
@patch("local_app.benchling_app.create_register_entites.cleaning_receivers")
@patch("local_app.benchling_app.create_register_entites.cleaning_crrna")
@patch("local_app.benchling_app.create_register_entites.read_and_basic_qc")
def test_load_and_clean_data_success(
    mock_read, mock_crrna, mock_receivers, mock_screening
):
    # Set up mock return values for each read
    mock_read.side_effect = [
        (pd.DataFrame({"crRNA_id": ["A"]}), None),
        (pd.DataFrame({"receiver_primer_id": ["P1"]}), None),
        (pd.DataFrame({"locus tag": ["CLC001C"]}), None),
        (pd.DataFrame({"Sequence Name": ["A"]}), None),
        (pd.DataFrame({"Sequence Name": ["P1", "P2"]}), None),
        (pd.DataFrame({"benchling_name": ["Genome1"], "selection_name": ["G"]}), None),
        (pd.DataFrame({"BGC_number": ["001"], "96_well_formatted": ["A1"]}), None),
    ]

    mock_crrna.return_value = (pd.DataFrame({"crRNA_id": ["A"]}), "")
    mock_receivers.return_value = (
        pd.DataFrame({"receiver_primer_id": ["P1"]}),
        pd.DataFrame(),
        "",
    )
    mock_screening.return_value = (pd.DataFrame({"primer_name": ["P1", "P2"]}), "")

    dummy_dest = {
        "crRNA_metadata": "dummy/path/crrna.csv",
        "receiver_primers_metadata": "dummy/path/rec.csv",
        "screening_primers_metadata": "dummy/path/scr.csv",
        "crRNA_plate_specs": "dummy/path/crrna_specs.csv",
        "primers_plate_specs": "dummy/path/primers_specs.csv",
        "strain_names_mapping": "dummy/path/genome.csv",
        "plate_location_mapping": "dummy/path/mapping.csv",
    }

    (
        crrna_df_merge,
        receivers_df_merge,
        screening_df_merge,
        genomes_df,
        mapping_df,
        error,
    ) = create_register_entites.load_and_clean_data(dummy_dest)

    assert error is None
    assert not crrna_df_merge.empty
    assert not receivers_df_merge.empty
    assert not screening_df_merge.empty
    assert not genomes_df.empty
    assert not mapping_df.empty


@patch("local_app.benchling_app.create_register_entites.cleaning_screening")
@patch("local_app.benchling_app.create_register_entites.cleaning_receivers")
@patch("local_app.benchling_app.create_register_entites.cleaning_crrna")
@patch("local_app.benchling_app.create_register_entites.read_and_basic_qc")
def test_load_and_clean_data_file_missing(mock_read, *_):
    # Only first read fails
    mock_read.side_effect = [
        (pd.DataFrame(), "File read failed"),
        *[(pd.DataFrame(), None)] * 6,
    ]

    dummy_dest = {
        "crRNA_metadata": "dummy/path/crrna.csv",
        "receiver_primers_metadata": "dummy/path/rec.csv",
        "screening_primers_metadata": "dummy/path/scr.csv",
        "crRNA_plate_specs": "dummy/path/crrna_specs.csv",
        "primers_plate_specs": "dummy/path/primers_specs.csv",
        "strain_names_mapping": "dummy/path/genome.csv",
        "plate_location_mapping": "dummy/path/mapping.csv",
    }

    result = create_register_entites.load_and_clean_data(dummy_dest)

    assert result[-1] == ["File read failed"]


@patch("local_app.benchling_app.create_register_entites.cleaning_screening")
@patch("local_app.benchling_app.create_register_entites.cleaning_receivers")
@patch("local_app.benchling_app.create_register_entites.cleaning_crrna")
@patch("local_app.benchling_app.create_register_entites.read_and_basic_qc")
def test_load_and_clean_data_cleaning_fails(
    mock_read, mock_crrna, mock_receivers, mock_screening
):
    mock_read.side_effect = [*((pd.DataFrame({"mock": [1]}), None) for _ in range(7))]

    mock_crrna.return_value = (pd.DataFrame(), "crRNA cleaning failed")
    mock_receivers.return_value = (pd.DataFrame(), pd.DataFrame(), "")
    mock_screening.return_value = (pd.DataFrame(), "")

    dummy_dest = {
        "crRNA_metadata": "dummy/path/crrna.csv",
        "receiver_primers_metadata": "dummy/path/rec.csv",
        "screening_primers_metadata": "dummy/path/scr.csv",
        "crRNA_plate_specs": "dummy/path/crrna_specs.csv",
        "primers_plate_specs": "dummy/path/primers_specs.csv",
        "strain_names_mapping": "dummy/path/genome.csv",
        "plate_location_mapping": "dummy/path/mapping.csv",
    }

    result = create_register_entites.load_and_clean_data(dummy_dest)

    assert "crRNA cleaning failed" in result[-1]


# ================================== testing create_and_register_entities ==================================


# ================================== testing register_crrna
def test_bulk_register_entities_preexisting_only():
    df = pd.DataFrame({"name": ["E1", "E2"]})

    # Mock app
    app = MagicMock()

    mock_e1 = MagicMock()
    mock_e1.name = "E1"
    mock_e1.id = "id_e1"

    mock_e2 = MagicMock()
    mock_e2.name = "E2"
    mock_e2.id = "id_e2"

    app.benchling.dna_sequences.list.return_value = [[mock_e1, mock_e2]]

    entity_builder_fn = MagicMock()

    updated_df = create_register_entites.bulk_register_entities(
        app=app,
        entity_type="dna",
        df=df.copy(),
        name_column="name",
        output_column="registered_id",
        folder_id="folder123",
        schema_id="schema123",
        entity_builder_fn=entity_builder_fn,
    )

    assert updated_df["registered_id"].tolist() == ["id_e1", "id_e2"]
    entity_builder_fn.assert_not_called()
    app.benchling.dna_sequences.bulk_create.assert_not_called()


def test_bulk_register_entities_mixed():
    df = pd.DataFrame({"name": ["E1", "E3"]})

    app = MagicMock()
    mock_e1 = MagicMock()
    mock_e1.name = "E1"
    mock_e1.id = "id_e1"
    app.benchling.custom_entities.list.return_value = [[mock_e1]]

    dummy_entity = MagicMock()
    entity_builder_fn = MagicMock(return_value=dummy_entity)

    mock_entity = MagicMock()
    mock_entity.name = "E3"
    mock_entity.id = "id_e3"

    mock_response = MagicMock()
    mock_response.custom_entities = [mock_entity]

    task = MagicMock()
    task.wait_for_response.return_value = mock_response
    app.benchling.custom_entities.bulk_create.return_value = task

    updated_df = create_register_entites.bulk_register_entities(
        app=app,
        entity_type="custom",
        df=df.copy(),
        name_column="name",
        output_column="registered_id",
        folder_id="folder123",
        schema_id="schema123",
        entity_builder_fn=entity_builder_fn,
    )

    assert updated_df["registered_id"].tolist() == ["id_e1", "id_e3"]
    entity_builder_fn.assert_called_once()
    app.benchling.custom_entities.bulk_create.assert_called_once()


def test_bulk_register_entities_empty_bulk_create_response():
    df = pd.DataFrame({"name": ["E1"]})

    app = MagicMock()
    app.benchling.dna_sequences.list.return_value = [[]]

    dummy_entity = MagicMock()
    entity_builder_fn = MagicMock(return_value=dummy_entity)

    mock_response = MagicMock()
    mock_response.dna_sequences = []

    task = MagicMock()
    task.wait_for_response.return_value = mock_response
    app.benchling.dna_sequences.bulk_create.return_value = task

    updated_df = create_register_entites.bulk_register_entities(
        app=app,
        entity_type="dna",
        df=df.copy(),
        name_column="name",
        output_column="registered_id",
        folder_id="folder123",
        schema_id="schema123",
        entity_builder_fn=entity_builder_fn,
    )

    # No match should have been found
    assert updated_df["registered_id"].isna().all()


# ================================== testing register_crrna


@patch("local_app.benchling_app.create_register_entites.bulk_register_entities")
def test_register_crrna_mixed(mock_bulk_register):
    # Prepare mock return value
    mock_result_df = pd.DataFrame(
        {"crRNA_id": ["CLCactX", "nonActY"], "crRNA_b_id": ["id1", "id2"]}
    )
    mock_bulk_register.return_value = mock_result_df

    # Input DataFrame
    input_df = pd.DataFrame(
        {
            "crRNA_id": ["CLCactX", "nonActY"],
            "crRNA": ["ATCG", "GCTA"],
            "crRNA_strand": ["+", "-"],
            "crRNA_loc": [123, 456],
        }
    )

    # Call the function
    folder_id = "folder123"
    schema_id = "schema456"
    registry = "registryABC"

    result_df = create_register_entites.register_crrna(
        app=MagicMock(),
        df=input_df,
        folder_id=folder_id,
        schema_id=schema_id,
        registry=registry,
    )

    # Validate return value is from mock
    assert result_df.equals(mock_result_df)

    # Validate mock was called with expected args
    mock_bulk_register.assert_called_once()
    args, kwargs = mock_bulk_register.call_args

    # Validate positional arguments (entity_type, df, etc.)
    assert args[1] == "dna"
    assert args[0]  # app should be passed
    assert args[2].equals(input_df)
    assert args[3] == "crRNA_id"
    assert args[4] == "crRNA_b_id"
    assert args[5] == folder_id
    assert args[6] == schema_id
    assert callable(args[7])  # entity_builder_fn

    # Bonus: test the actual build_entity logic through the passed function
    builder_fn = args[7]
    act_entity = builder_fn(input_df.iloc[0])
    assert act_entity.name == "CLCactX"

    nonact_entity = builder_fn(input_df.iloc[1])
    assert nonact_entity.name == "nonActY"


# ================================== testing register_receivers


@patch("local_app.benchling_app.create_register_entites.bulk_register_entities")
def test_register_receivers_success(mock_bulk_register):
    # Fake input and expected result
    input_df = pd.DataFrame(
        {"receiver_primer_id": ["P1", "P2"], "primer_seq_idt": ["ATCG", "CGTA"]}
    )

    mock_result_df = pd.DataFrame(
        {"receiver_primer_id": ["P1", "P2"], "receiver_primer_b_id": ["id1", "id2"]}
    )
    mock_bulk_register.return_value = mock_result_df

    # Call the function
    # app = MagicMock()
    folder_id = "folder123"
    schema_id = "schema456"
    registry = "registryABC"

    result = create_register_entites.register_receivers(
        app=MagicMock(),
        df=input_df,
        folder_id=folder_id,
        schema_id=schema_id,
        registry=registry,
    )

    # Verify return
    assert result.equals(mock_result_df)

    # Verify mock call
    mock_bulk_register.assert_called_once()
    args, kwargs = mock_bulk_register.call_args

    # Check argument correctness
    assert kwargs["app"]
    assert kwargs["entity_type"] == "dna"
    assert kwargs["df"].equals(input_df)
    assert kwargs["name_column"] == "receiver_primer_id"
    assert kwargs["output_column"] == "receiver_primer_b_id"
    assert kwargs["folder_id"] == folder_id
    assert kwargs["schema_id"] == schema_id
    assert callable(kwargs["entity_builder_fn"])

    # Check the builder builds the expected DnaSequenceBulkCreate
    builder_fn = kwargs["entity_builder_fn"]
    built_entity = builder_fn(input_df.iloc[0])
    assert built_entity.name == "P1"
    assert built_entity.bases == "ATCG"
    assert built_entity.folder_id == folder_id
    assert built_entity.schema_id == schema_id
    assert built_entity.is_circular is False


# ================================== testing register_clc_receivers
@patch("local_app.benchling_app.create_register_entites.bulk_register_entities")
@patch("local_app.benchling_app.create_register_entites.b_api_ids")
def test_register_clc_receivers_success(mock_b_api_ids, mock_bulk_register):
    # Set up constants
    mock_b_api_ids.backbone_primer48 = "primer_48_id"
    mock_b_api_ids.backbone_primer45 = "primer_45_id"
    mock_b_api_ids.backbone_plasmid48 = "plasmid_48_id"
    mock_b_api_ids.backbone_plasmid45 = "plasmid_45_id"

    # Input DataFrame
    input_df = pd.DataFrame(
        {
            "receiver_primer_id": ["R1", "R2"],
            "receiver_primer_b_id": ["b_id1", "b_id2"],
            "crRNA_prefix": ["U", "D"],
        }
    )

    # Expected result after mock registration
    mock_result_df = pd.DataFrame(
        {"receiver_primer_id": ["R1", "R2"], "clc_receiver_b_id": ["id_r1", "id_r2"]}
    )
    mock_bulk_register.return_value = mock_result_df

    app = MagicMock()
    folder_id = "folder123"
    schema_id = "schema456"
    registry = "registryABC"

    result = create_register_entites.register_clc_receivers(
        app, input_df.copy(), folder_id, schema_id, registry
    )

    # Output is from the mock
    assert result.equals(mock_result_df)

    # Assert mock was called correctly
    mock_bulk_register.assert_called_once()
    _, kwargs = mock_bulk_register.call_args

    # Check internal calc_receiver_name logic ran
    assert "clc_receiver_name" in kwargs["df"].columns
    assert list(kwargs["df"]["clc_receiver_name"]) == ["pBE48-R1", "pBE45-R2"]

    # Check build_entity behavior
    builder_fn = kwargs["entity_builder_fn"]
    row_u = kwargs["df"].iloc[0]
    entity_u = builder_fn(row_u)
    assert entity_u.name == "pBE48-R1"

    row_d = kwargs["df"].iloc[1]
    entity_d = builder_fn(row_d)
    assert entity_d.name == "pBE45-R2"


# ================================== testing register_screening
@patch("local_app.benchling_app.create_register_entites.bulk_register_entities")
def test_register_screening_success(mock_bulk_register):
    # Mock input dataframe
    input_df = pd.DataFrame(
        {"primer_id": ["S1", "S2"], "primer_seq_idt": ["ATCG", "CGTA"]}
    )

    # Mock return from bulk_register_entities
    mock_return_df = pd.DataFrame(
        {"primer_id": ["S1", "S2"], "primer_b_id": ["id_s1", "id_s2"]}
    )
    mock_bulk_register.return_value = mock_return_df

    # Inputs for the function
    app = MagicMock()
    folder_id = "folder123"
    schema_id = "schema456"
    registry = "registry789"

    # Call the function
    result_df = create_register_entites.register_screening(
        app, input_df.copy(), folder_id, schema_id, registry
    )

    # Verify output
    assert result_df.equals(mock_return_df)

    # Verify call to mock
    mock_bulk_register.assert_called_once()
    _, kwargs = mock_bulk_register.call_args

    # Argument assertions
    assert kwargs["app"] is app
    assert kwargs["entity_type"] == "dna"
    assert kwargs["df"].equals(input_df)
    assert kwargs["name_column"] == "primer_id"
    assert kwargs["output_column"] == "primer_b_id"
    assert kwargs["folder_id"] == folder_id
    assert kwargs["schema_id"] == schema_id
    assert callable(kwargs["entity_builder_fn"])

    # Test builder function directly
    builder_fn = kwargs["entity_builder_fn"]
    built_entity = builder_fn(input_df.iloc[0])
    assert built_entity.name == "S1"
    assert built_entity.bases == "ATCG"
    assert built_entity.folder_id == folder_id
    assert built_entity.schema_id == schema_id
    assert built_entity.is_circular is False


# ================================== testing find_genomes


def test_find_genomes_success():
    df = pd.DataFrame({"benchling_name": ["GenomeA", "GenomeB"]})

    # mock returned Benchling entities
    ent1 = MagicMock(name="GenomeA", id="id_a")
    ent1.name = "GenomeA"
    ent1.id = "id_a"

    ent2 = MagicMock(name="GenomeB", id="id_b")
    ent2.name = "GenomeB"
    ent2.id = "id_b"

    app = MagicMock()
    app.benchling.custom_entities.list.side_effect = [
        [[ent1]],  # from CLC
        [[ent2]],  # from NBC
    ]

    result_df, error = create_register_entites.find_genomes(
        app, df.copy(), "clc_folder", "nbc_folder", "schema_id"
    )

    assert error is None
    assert result_df.loc[0, "genome_b_id"] == "id_a"
    assert result_df.loc[1, "genome_b_id"] == "id_b"


# @patch("local_app.benchling_app.create_register_entites.logger")
def test_find_genomes_missing_name_triggers_error():  # mock_logger):
    df = pd.DataFrame({"benchling_name": ["MissingGenome"]})

    ent = MagicMock()
    ent.name = "OtherGenome"
    ent.id = "id_other"

    app = MagicMock()
    app.benchling.custom_entities.list.side_effect = [
        [[ent]],  # CLC
        [[]],  # NBC
    ]

    result_df, error = create_register_entites.find_genomes(
        app, df.copy(), "clc_folder", "nbc_folder", "schema_id"
    )

    assert result_df.empty
    assert "MissingGenome" in error

    # mock_logger.warning.assert_called_once()
    # assert "MissingGenome" in mock_logger.warning.call_args[0][0]


# ================================== testing register_dna_fragments


@patch("local_app.benchling_app.create_register_entites.bulk_register_entities")
def test_register_dna_fragments_success(mock_bulk_register):
    # Input DataFrame
    input_df = pd.DataFrame(
        {"benchling_name": ["GenomeA"], "genome_b_id": ["gen_id_A"]}
    )

    # Expected DataFrame result from bulk_register_entities
    mock_result_df = pd.DataFrame(
        {
            "benchling_name": ["GenomeA"],
            "genome_b_id": ["gen_id_A"],
            "dna_fragment_b_id": ["frag_id_A"],
        }
    )
    mock_bulk_register.return_value = mock_result_df

    # App and config inputs
    app = MagicMock()
    folder_id = "folder_xyz"
    schema_id = "schema_abc"
    registry = "registry_test"

    # Run the function
    result_df = create_register_entites.register_dna_fragments(
        app, input_df.copy(), folder_id, schema_id, registry
    )

    # Returned result is correct
    assert result_df.equals(mock_result_df)

    # bulk_register_entities called once
    mock_bulk_register.assert_called_once()
    _, kwargs = mock_bulk_register.call_args

    # df contains the new dna_fragment column
    assert "dna_fragment" in kwargs["df"].columns
    assert kwargs["df"]["dna_fragment"].iloc[0] == "GenomeA gDNA"

    # Validate arguments
    assert kwargs["app"] is app
    assert kwargs["entity_type"] == "dna"
    assert kwargs["name_column"] == "dna_fragment"
    assert kwargs["output_column"] == "dna_fragment_b_id"
    assert kwargs["folder_id"] == folder_id
    assert kwargs["schema_id"] == schema_id
    assert callable(kwargs["entity_builder_fn"])

    # Test builder function
    builder_fn = kwargs["entity_builder_fn"]
    built_entity = builder_fn(kwargs["df"].iloc[0])
    assert built_entity.name == "GenomeA gDNA"
    assert built_entity.schema_id == schema_id
    assert built_entity.folder_id == folder_id
    # assert built_entity.fields["Template strains"]["value"] == ["gen_id_A"]
    assert built_entity.bases == ""


# ================================== testing register_clc_bac


@patch("local_app.benchling_app.create_register_entites.bulk_register_entities")
def test_register_clc_bac_success(mock_bulk_register):
    # Setup
    mock_bulk_register.return_value = "mock_result"

    # Fake BGC range and act
    min_clust = 1
    max_clust = 1

    # Minimal working input for each DataFrame
    crrna_df = pd.DataFrame(
        {
            "BGC_number": ["001", "001", "act", "act"],
            "crRNA_prefix": ["U", "D", "U", "D"],
            "crRNA_b_id": ["grnaU", "grnaD", "grnaU_act", "grnaD_act"],
            "strain_name": [
                "strainX",
                "strainX",
                "Streptomyces coelicolor M145",
                "Streptomyces coelicolor M145",
            ],
            "crRNA_id": ["CLCactU001", "CLCactD001", "CLCactUact", "CLCactDact"],
        }
    )

    receivers_df = pd.DataFrame(
        {
            "BGC_number": ["001", "001", "act", "act"],
            "crRNA_prefix": ["U", "D", "U", "D"],
            "receiver_primer_b_id": ["recU", "recD", "recU_act", "recD_act"],
            "clc_receiver_b_id": ["clcU", "clcD", "clcU_act", "clcD_act"],
        }
    )

    screening_df = pd.DataFrame(
        {
            "BGC_number": ["001", "001", "act", "act"],
            "sufix": ["F", "R", "F", "R"],
            "primer_b_id": ["scrF", "scrR", "scrF_act", "scrR_act"],
        }
    )

    genome_df = pd.DataFrame(
        {
            "selection_name": ["strainX", "Streptomyces coelicolor M145"],
            "genome_b_id": ["genome1", "genome_act"],
            "dna_fragment_b_id": ["frag1", "frag_act"],
        }
    )

    mapping_df = pd.DataFrame(
        {"BGC_number": [1], "96_well_formatted": ["A01"]}
    ).set_index("BGC_number")

    app = MagicMock()

    result = create_register_entites.register_clc_bac(
        app=app,
        crrna_df_merge=crrna_df,
        receivers_df_merge=receivers_df,
        screening_df_merge=screening_df,
        genome_df=genome_df,
        mapping_df=mapping_df,
        min_clust=min_clust,
        max_clust=max_clust,
        folder_id="folder123",
        schema_id="schema456",
        registry="reg789",
    )

    # Output
    assert result == "mock_result"

    # bulk_register_entities was called
    mock_bulk_register.assert_called_once()
    _, kwargs = mock_bulk_register.call_args

    assert kwargs["app"] is app
    assert kwargs["entity_type"] == "custom"
    assert kwargs["name_column"] == "bac_name"
    assert kwargs["output_column"] == "bac_b_id"
    assert kwargs["folder_id"] == "folder123"
    assert kwargs["schema_id"] == "schema456"
    assert callable(kwargs["entity_builder_fn"])

    df = kwargs["df"]
    assert "bac_name" in df.columns
    assert "grna_U" in df.columns
    assert df.shape[0] == 2  # one normal BGC and one "act"

    # Test entity builder logic
    row = df.iloc[0]
    built_entity = kwargs["entity_builder_fn"](row)
    assert built_entity.name == row["bac_name"]
    # assert built_entity.fields["Well96"]["value"] == row["well_96"]
    # assert built_entity.fields["gRNA - Up"]["value"] == row["grna_U"]


# ================================== testing create_and_register_entities


@patch("local_app.benchling_app.create_register_entites.register_clc_bac")
@patch("local_app.benchling_app.create_register_entites.register_dna_fragments")
@patch("local_app.benchling_app.create_register_entites.find_genomes")
@patch("local_app.benchling_app.create_register_entites.register_screening")
@patch("local_app.benchling_app.create_register_entites.register_clc_receivers")
@patch("local_app.benchling_app.create_register_entites.register_receivers")
@patch("local_app.benchling_app.create_register_entites.register_crrna")
def test_create_and_register_entities_success(
    mock_register_crrna,
    mock_register_receivers,
    mock_register_clc_receivers,
    mock_register_screening,
    mock_find_genomes,
    mock_register_dna_fragments,
    mock_register_clc_bac,
):
    # Dummy inputs
    app = MagicMock()
    config_store = {
        "crRNA storage folder": "f1",
        "gRNA schema": "s1",
        "Registry schema": "r1",
        "Primers storage folder": "f2",
        "Primer schema": "s2",
        "Receivers assemblies storage folder": "f3",
        "CLC Receiver schema": "s3",
        "CLC strains storage folder": "f4",
        "NBC strains storage folder": "f5",
        "Strain schema": "s4",
        "DNA fragments storage folder": "f6",
        "DNA fragment schema": "s5",
        "BACs storage folder": "f7",
        "CLC BAC schema": "s6",
    }

    # Mock config calls
    app.config_store.config_by_path.side_effect = lambda keys: MagicMock(
        required=lambda: MagicMock(value_str=lambda: config_store[keys[0]])
    )

    dummy_df = pd.DataFrame({"BGC_number": ["001", "002"], "some_col": [1, 2]})

    # Mocks for each register function
    mock_register_crrna.return_value = dummy_df.copy()
    mock_register_receivers.return_value = dummy_df.copy()
    mock_register_clc_receivers.return_value = dummy_df.copy()
    mock_register_screening.return_value = dummy_df.copy()
    mock_find_genomes.return_value = (dummy_df.copy(), None)
    mock_register_dna_fragments.return_value = dummy_df.copy()
    mock_register_clc_bac.return_value = "final_output"

    result = create_register_entites.create_and_register_entities(
        app,
        crrna_df_merge=dummy_df.copy(),
        receivers_df_merge=dummy_df.copy(),
        screening_df_merge=dummy_df.copy(),
        genomes_df=pd.DataFrame(
            {
                "selection_name": ["strainX"],
                "genome_b_id": ["g1"],
                "dna_fragment_b_id": ["d1"],
            }
        ),
        mapping_df=pd.DataFrame(
            {"BGC_number": ["001", "002"], "96_well_formatted": ["A01", "A02"]}
        ).set_index("BGC_number"),
    )

    clc_bac_df, crrna_out, receivers_out, screening_out, error = result

    assert clc_bac_df == "final_output"
    assert isinstance(crrna_out, pd.DataFrame)
    assert isinstance(receivers_out, pd.DataFrame)
    assert isinstance(screening_out, pd.DataFrame)
    assert error is None

    mock_register_crrna.assert_called_once()
    mock_register_clc_bac.assert_called_once()


@patch("local_app.benchling_app.create_register_entites.find_genomes")
@patch("local_app.benchling_app.create_register_entites.register_crrna")
@patch("local_app.benchling_app.create_register_entites.register_receivers")
@patch("local_app.benchling_app.create_register_entites.register_clc_receivers")
@patch("local_app.benchling_app.create_register_entites.register_screening")
def test_create_and_register_entities_genome_error(
    mock_register_screening,
    mock_register_clc_receivers,
    mock_register_receivers,
    mock_register_crrna,
    mock_find_genomes,
):
    app = MagicMock()
    dummy_df = pd.DataFrame({"BGC_number": ["001"], "some_col": [1]})

    app.config_store.config_by_path.side_effect = lambda keys: MagicMock(
        required=lambda: MagicMock(value_str=lambda: "any_val")
    )

    mock_register_crrna.return_value = dummy_df.copy()
    mock_register_receivers.return_value = dummy_df.copy()
    mock_register_clc_receivers.return_value = dummy_df.copy()
    mock_register_screening.return_value = dummy_df.copy()
    mock_find_genomes.return_value = (pd.DataFrame(), "some genome error")

    result = create_register_entites.create_and_register_entities(
        app,
        crrna_df_merge=dummy_df.copy(),
        receivers_df_merge=dummy_df.copy(),
        screening_df_merge=dummy_df.copy(),
        genomes_df=dummy_df.copy(),
        mapping_df=pd.DataFrame(
            {"BGC_number": ["001"], "96_well_formatted": ["A01"]}
        ).set_index("BGC_number"),
    )

    assert result[-1] == "some genome error"
