"""
test_csv_utils.py
Description: Test the functions related to csv files managment
Author: Laia Meseguer Monfort
Date: April 25, 2025
"""

# ==================================
# IMPORTS
# ==================================
import pytest

from pathlib import Path
import pandas as pd


from unittest.mock import patch, MagicMock

from local_app.benchling_app import csv_utils


# ==================================
# FUNCTIONS
# ==================================

# ================================== Test download_csv ==================================


@pytest.fixture
def mock_app():
    # Create a fake app structure with mocked services
    mock_app = MagicMock()

    # Mock custom entity
    mock_entity = MagicMock()
    mock_entity.fields = {"CSV": MagicMock(value="fake_blob_id")}
    mock_entity.name = "crRNA_metadata_dummy.csv"
    mock_app.benchling.custom_entities.get_by_id.return_value = mock_entity

    # Mock download_file
    mock_app.benchling.blobs.download_file.return_value = None

    return mock_app


@patch("local_app.benchling_app.csv_utils.Path.mkdir")  # Prevent actual folder creation
@patch("local_app.benchling_app.csv_utils.Path")  # Prevent actual Path resolution
def test_download_csv_success(mock_path, mock_mkdir, mock_app):
    # Setup
    mock_path.return_value.parent.mkdir = MagicMock()
    mock_path.return_value.__truediv__ = lambda self, x: self
    mock_path.return_value.exists.return_value = True

    destination_dict = {"crRNA_metadata": "/some/fake/path.csv"}

    # Act
    result = csv_utils.download_csv(mock_app, "fake_entity_id", destination_dict)

    # Assert
    assert result is None
    mock_app.benchling.custom_entities.get_by_id.assert_called_once_with(
        "fake_entity_id"
    )
    mock_app.benchling.blobs.download_file.assert_called_once()
    mock_path.assert_called_with("/some/fake/path.csv")


def test_download_csv_no_match(mock_app):
    # Act
    result = csv_utils.download_csv(mock_app, "fake_entity_id", destination_dict={})

    # Assert
    assert result.startswith("This file’s entity name")


@pytest.fixture
def mock_app_bad():
    # Create a fake app structure with mocked services
    mock_app = MagicMock()

    # Mock custom entity
    mock_entity = MagicMock()
    mock_entity.fields = {"CSV": MagicMock(display_value="not_a_csv.txt")}
    mock_entity.name = "crRNA_metadata_dummy.txt"
    mock_app.benchling.custom_entities.get_by_id.return_value = mock_entity

    # Mock download_file
    mock_app.benchling.blobs.download_file.return_value = None

    return mock_app


def test_download_csv_bad_name(mock_app_bad):
    # Act
    result = csv_utils.download_csv(mock_app_bad, "fake_entity_id", destination_dict={})

    # Assert
    assert result.startswith("Are you sure")


# # ================================== Test check_all_csv_exist ==================================


def test_all_files_exist():
    test_dict = {
        "file1": "/fake/dir/file1.csv",
        "file2": "/fake/dir/file2.csv",
    }

    with patch("local_app.benchling_app.csv_utils.Path.is_file", return_value=True):
        result = csv_utils.check_all_csv_exist(test_dict)
        assert result is None


def test_some_files_missing():
    test_dict = {
        "file1": "/fake/dir/file1.csv",
        "file2": "/fake/dir/file2.csv",
    }

    # Simulate first exists, second doesn't
    with patch(
        "local_app.benchling_app.csv_utils.Path.is_file", side_effect=[True, False]
    ):
        result = csv_utils.check_all_csv_exist(test_dict)
        assert "file2.csv" in result
        assert result.startswith("Missing files:")


# # ================================== Test upload_csv ==================================


@patch("local_app.benchling_app.csv_utils.pd.read_csv")
@patch("local_app.benchling_app.csv_utils.pd.DataFrame.to_csv")
@patch("local_app.benchling_app.csv_utils.CustomEntityCreate")
@patch("local_app.benchling_app.csv_utils.fields")
def test_upload_csv_success(
    mock_fields, mock_CustomEntityCreate, mock_to_csv, mock_read_csv
):
    # Dummy df to upload
    dummy_df = pd.DataFrame({"some_col": [1]})

    # Fake reading the crRNA metadata file
    dummy_crrna_df = pd.DataFrame({"Order ID": ["12345"]})
    mock_read_csv.return_value = dummy_crrna_df

    # Destination dict
    destination_dict = {"crRNA_metadata": "fake/metadata/path.csv"}

    # Mock app and Benchling
    app = MagicMock()

    fake_blob = MagicMock()
    fake_blob.id = "blob123"
    app.benchling.blobs.create_from_file.return_value = fake_blob

    fake_entity = MagicMock()
    fake_entity.name = "dummy_api_ids_Plate12345"
    fake_entity.id = "entity123"
    app.benchling.custom_entities.create.return_value = fake_entity

    app.config_store.config_by_path.side_effect = lambda keys: MagicMock(
        required=lambda: MagicMock(value_str=lambda: f"dummy_val_for_{keys[0]}")
    )

    # Mocks for fields and CustomEntityCreate
    mock_fields.return_value = {"CSV": {"value": "blob123"}}
    mock_CustomEntityCreate.return_value = fake_entity

    # Path for upload
    upload_path = Path("some/fake/path.csv")

    # Call the function
    updated_dict, order_number, api_file_id = csv_utils.upload_csv(
        app=app,
        df=dummy_df.copy(),
        destination_dict=destination_dict.copy(),
        path=upload_path,
    )

    # Assert file was saved
    mock_to_csv.assert_called_once_with(upload_path, index=False)

    # Assert crRNA metadata file was read
    mock_read_csv.assert_called_once_with(destination_dict["crRNA_metadata"])

    # Assert blob created
    app.benchling.blobs.create_from_file.assert_called_once()

    # Assert entity created
    app.benchling.custom_entities.create.assert_called_once()

    # Check that destination dict updated
    assert "api_ids" in updated_dict
    assert updated_dict["api_ids"] == upload_path
