"""
test_notebook_utils.py
Description: Test the functions related to writing new info in a notebook
Author: Laia Meseguer Monfort
Date: April 28, 2025
"""

# ==================================
# IMPORTS
# ==================================
import pandas as pd

from unittest.mock import patch, MagicMock

from local_app.benchling_app import notebook_utils


# ==================================
# FUNCTIONS
# ==================================

# ================================== testing process_notebook ==================================
# ================================== testing find_results_table


def test_find_results_table_success():
    app = MagicMock()

    # Setup mock notebook structure
    mock_note = MagicMock()
    mock_note.type = "results_table"
    mock_note.assay_result_schema_id = "schema123"
    mock_note.api_id = "table_api_456"

    mock_entry = MagicMock()
    mock_entry.notes = [mock_note]

    mock_notebook = MagicMock()
    mock_notebook.days = [mock_entry]

    app.benchling.entries.get_entry_by_id.return_value = mock_notebook

    # Call function
    table_api_id, error = notebook_utils.find_results_table(
        app=app,
        notebook_api_id="notebook123",
        CLC_results_table_schema_id="schema123",
    )

    # Assertions
    app.benchling.entries.get_entry_by_id.assert_called_once_with("notebook123")
    assert table_api_id == "table_api_456"
    assert error is False


def test_find_results_table_no_match():
    app = MagicMock()

    # Setup mock notebook structure with wrong schema
    mock_note = MagicMock()
    mock_note.type = "results_table"
    mock_note.assay_result_schema_id = "different_schema"
    mock_note.api_id = "wrong_table_api"

    mock_entry = MagicMock()
    mock_entry.notes = [mock_note]

    mock_notebook = MagicMock()
    mock_notebook.days = [mock_entry]

    app.benchling.entries.get_entry_by_id.return_value = mock_notebook

    # Call function
    table_api_id, error = notebook_utils.find_results_table(
        app=app,
        notebook_api_id="notebook123",
        CLC_results_table_schema_id="schema123",
    )

    # Assertions
    app.benchling.entries.get_entry_by_id.assert_called_once_with("notebook123")
    assert table_api_id is None
    assert error is True


# ================================== testing process_notebook
# @pytest.fixture(autouse=True)
# def set_app_env_to_test(monkeypatch):
#     monkeypatch.setenv("APP_ENV", "test")


@patch("local_app.benchling_app.notebook_utils.find_results_table")
@patch("local_app.benchling_app.notebook_utils.fields")
@patch("local_app.benchling_app.notebook_utils.AssayResultCreate")
def test_process_notebook_success(
    mock_AssayResultCreate, mock_fields, mock_find_results_table, monkeypatch
):
    # ensure TESTING=True
    monkeypatch.setenv("APP_ENV", "test")

    # Setup app mock
    app = MagicMock()

    # Mock config return values
    app.config_store.config_by_path.side_effect = lambda keys: MagicMock(
        required=lambda: MagicMock(value_str=lambda: f"dummy_{keys[0]}")
    )

    # Mock notebook entries
    mock_entry = MagicMock()
    mock_entry.id = "notebook_123"
    app.benchling.entries.list_entries.return_value = [
        [mock_entry]
    ]  # Note double brackets!

    # Mock result table finding
    mock_find_results_table.return_value = ("result_table_456", False)

    # Mock fields() return
    mock_fields.return_value = {"sample": {"value": "dummy_value"}}

    # Mock AssayResultCreate return
    mock_assay_result = MagicMock()
    mock_AssayResultCreate.return_value = mock_assay_result

    # Dummy CLC BAC dataframe
    clc_bac_df = pd.DataFrame({"bac_b_id": ["bac1", "bac2"]})

    # Run the function
    result = notebook_utils.process_notebook(
        app=app,
        notebook_name="Test Notebook",
        clc_bac_df=clc_bac_df,
    )

    # ✅ Assertions
    app.benchling.entries.list_entries.assert_called_once_with(name="Test Notebook")
    mock_find_results_table.assert_called_once_with(
        app, "notebook_123", "dummy_Result schema"
    )
    assert result is None

    app.benchling.assay_results.bulk_create.assert_called_once()
    bulk_args = app.benchling.assay_results.bulk_create.call_args[1]

    # Check assay_results were created
    assert isinstance(bulk_args["assay_results"], list)
    assert len(bulk_args["assay_results"]) == 2  # one per row
    assert bulk_args["table_id"] == "result_table_456"


def test_process_notebook_no_table_found():
    app = MagicMock()
    app.config_store.config_by_path.side_effect = lambda keys: MagicMock(
        required=lambda: MagicMock(value_str=lambda: f"dummy_{keys[0]}")
    )
    mock_entry = MagicMock()
    mock_entry.id = "notebook_123"
    app.benchling.entries.list_entries.return_value = [[mock_entry]]

    with patch(
        "local_app.benchling_app.notebook_utils.find_results_table",
        return_value=(None, True),
    ):
        result = notebook_utils.process_notebook(
            app=app,
            notebook_name="Fake Notebook",
            clc_bac_df=pd.DataFrame({"bac_b_id": ["bac1"]}),
        )

    assert "The results table cannot be retreived" in result
