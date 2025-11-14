# ==================================
# IMPORTS
# ==================================

import pytest
from unittest.mock import MagicMock, ANY, patch
from local_app.benchling_app import canvas_interaction
from benchling_sdk.apps.canvas.framework import CanvasBuilder
import pandas as pd
from benchling_sdk.apps.status.errors import AppUserFacingError


from local_app.benchling_app.views.constants import PROCESS_BUTTON_ID
from benchling_sdk.models.webhooks.v0 import CanvasInteractionWebhookV2

# ==================================
# FUNCTIONS
# ==================================


# ================================== testing _canvas_builder_from_canvas_id ==================================
def test_canvas_builder_from_canvas_id_calls_and_returns_builder():
    # 1. Make a fake `app` and a fake canvas object
    app = MagicMock()
    mock_canvas = MagicMock()
    app.benchling.apps.get_canvas_by_id.return_value = mock_canvas

    # 2. Make CanvasBuilder.from_canvas return a sentinel builder
    fake_builder = MagicMock()
    with patch.object(
        CanvasBuilder, "from_canvas", return_value=fake_builder
    ) as mock_from_canvas:
        # 3. Call your helper
        result = canvas_interaction._canvas_builder_from_canvas_id(app, "canvas-xyz")

        # 4. It should have fetched the canvas by ID
        app.benchling.apps.get_canvas_by_id.assert_called_once_with("canvas-xyz")

        # 5. It should have passed *that* canvas into CanvasBuilder.from_canvas
        mock_from_canvas.assert_called_once_with(mock_canvas)

        # 6. And finally, it should return whatever from_canvas gave back
        assert result is fake_builder


# ================================== testing route_interaction_webhook ==================================
@pytest.fixture
def dummy_canvas_interaction():
    ci = MagicMock(spec=CanvasInteractionWebhookV2)
    ci.canvas_id = "canvas-123"
    ci.button_id = PROCESS_BUTTON_ID
    return ci


@pytest.fixture
def dummy_session():
    # Create a fake session and context manager with its own fake app
    session = MagicMock()
    session.app = MagicMock()
    session.attach_canvas = MagicMock()

    cm = MagicMock()
    cm.__enter__.return_value = session
    cm.__exit__.return_value = False

    return cm, session


@patch("local_app.benchling_app.canvas_interaction.delete_csvs", autospec=True)
@patch("local_app.benchling_app.canvas_interaction.download_csv", autospec=True)
@patch("local_app.benchling_app.canvas_interaction.check_all_csv_exist", autospec=True)
@patch("local_app.benchling_app.canvas_interaction.load_and_clean_data", autospec=True)
@patch(
    "local_app.benchling_app.canvas_interaction.create_and_register_entities",
    autospec=True,
)
@patch("local_app.benchling_app.canvas_interaction.upload_csv", autospec=True)
@patch("local_app.benchling_app.canvas_interaction.find_and_fill_plates", autospec=True)
@patch("local_app.benchling_app.canvas_interaction.process_notebook", autospec=True)
@patch(
    "local_app.benchling_app.canvas_interaction._canvas_builder_from_canvas_id",
    autospec=True,
)
@patch("local_app.benchling_app.canvas_interaction.App", autospec=True)
def test_route_interaction_webhook_happy_path(
    MockAppClass,  # <-- from @patch(App) (bottom)
    mock_builder_fn,  # <-- from @patch(_canvas_builder...)
    mock_process_notebook,  # <-- from @patch(process_notebook)
    mock_find_and_fill,  # <-- from @patch(find_and_fill_plates)
    mock_upload_csv,  # <-- from @patch(upload_csv)
    mock_create_and_register,  # <-- from @patch(create_and_register_entities)
    mock_load_and_clean,  # <-- from @patch(load_and_clean_data)
    mock_check_all,  # <-- from @patch(check_all_csv_exist)
    mock_download,  # <-- from @patch(download_csv)
    mock_delete_csvs,  # <-- from @patch(delete_csvs) (top)
    dummy_canvas_interaction,
    dummy_session,
):
    # 1) Prepare our fake App instance and session‐context
    fake_app = MagicMock()
    MockAppClass.return_value = fake_app
    create_session_cm, fake_session = dummy_session
    fake_app.create_session_context.return_value = create_session_cm

    # 2) Fake CanvasBuilder
    fake_builder = MagicMock()
    fake_builder.inputs_to_dict.return_value = {
        "input_block_metadata": ["meta_id"],
        "input_block_genome_mapping": ["genome_id"],
        "input_block_plates": ["plate1", "plate2", "plate3"],
        "input_block_plate_specs": ["plate1", "plate2", "plate3"],
        "input_block_notebook_name": "notebook.ipynb",
    }
    fake_update = {"foo": "bar"}
    fake_builder.with_blocks.return_value.to_update.return_value = fake_update
    mock_builder_fn.return_value = fake_builder

    # 3) CSV download & existence check succeed
    mock_download.return_value = None
    mock_check_all.return_value = None

    # 4) load_and_clean_data returns dfs + no error
    dfs_out = (
        pd.DataFrame({"x": []}),
        pd.DataFrame({"y": []}),
        pd.DataFrame({"z": []}),
        pd.DataFrame({"g": []}),
        pd.DataFrame({"m": []}),
        None,
    )
    mock_load_and_clean.return_value = dfs_out

    # 5) create_and_register_entities returns new clc_bac_df + no error
    clc_bac_df = pd.DataFrame({"a": []})
    mock_create_and_register.return_value = (
        clc_bac_df,
        dfs_out[0],
        dfs_out[1],
        dfs_out[2],
        None,
    )

    # 6) upload_csv returns new destination_dict + order_number
    new_dest = {
        "crRNA_metadata": "/external/crrna_metadata.csv",
        "receiver_primers_metadata": "/external/rec_metadata.csv",
        "screening_primers_metadata": "/external/scr_metadata.csv",
        "crRNA_plate_specs": "/external/crrna_specs.csv",
        "primers_plate_specs": "/external/primers_specs.csv",
        "strain_names_mapping": "/external/genome.csv",
        "plate_location_mapping": "/external/mapping.csv",
    }
    mock_upload_csv.return_value = (new_dest, 1, "id1234")

    # 7) find_and_fill_plates & process_notebook succeed
    mock_find_and_fill.return_value = None
    mock_process_notebook.return_value = None

    # 8) Call the function
    canvas_interaction.route_interaction_webhook(fake_app, dummy_canvas_interaction)

    # Assertions ───────────────────────────────────────────────────────────

    # A) We opened a session
    fake_app.create_session_context.assert_called_once_with(
        "Process CSV", timeout_seconds=20
    )

    # B) We attached the canvas
    fake_session.attach_canvas.assert_called_once_with("canvas-123")

    # C) We fetched and built the CanvasBuilder
    mock_builder_fn.assert_called_once_with(fake_app, "canvas-123")
    fake_builder.inputs_to_dict.assert_called_once()

    # D) We downloaded each of the two file IDs
    assert mock_download.call_count == 5  # 2
    mock_download.assert_any_call(
        app=fake_app, entit_id="meta_id", destination_dict=ANY
    )
    mock_download.assert_any_call(
        app=fake_app, entit_id="genome_id", destination_dict=ANY
    )

    # E) We checked all CSVs, loaded/cleaned data, registered entities, uploaded CSVs
    mock_check_all.assert_called_once_with(ANY)
    mock_load_and_clean.assert_called_once_with(ANY)
    mock_create_and_register.assert_called_once_with(
        fake_app, dfs_out[0], dfs_out[1], dfs_out[2], dfs_out[3], dfs_out[4]
    )
    mock_upload_csv.assert_called_once_with(
        app=fake_app,
        df=clc_bac_df,
        destination_dict=new_dest,
        path="/processed/api_ids.csv",
    )

    # F) We deleted each CSV
    # delete_csvs should have been called once per entry in new_dest
    assert mock_delete_csvs.call_count == len(new_dest)
    for path in new_dest.values():
        mock_delete_csvs.assert_any_call(path)

    # G) We filled three plates
    mock_find_and_fill.assert_called_once_with(
        app=fake_app,
        crrna_df_merge=dfs_out[0],
        receivers_df_merge=dfs_out[1],
        screening_df_merge=dfs_out[2],
        plate_list=["plate1", "plate2", "plate3"],
        order_number=1,
    )

    # H) We processed the notebook
    mock_process_notebook.assert_called_once_with(
        app=fake_app, notebook_name="notebook.ipynb", clc_bac_df=clc_bac_df
    )

    # I) We updated the canvas via session.app.benchling.apps.update_canvas
    fake_session.app.benchling.apps.update_canvas.assert_called_once_with(
        "canvas-123", fake_update
    )


@patch("local_app.benchling_app.canvas_interaction.download_csv", autospec=True)
@patch(
    "local_app.benchling_app.canvas_interaction._canvas_builder_from_canvas_id",
    autospec=True,
)
@patch("local_app.benchling_app.canvas_interaction.App", autospec=True)
def test_route_interaction_webhook_download_error_raises(
    MockAppClass,
    mock_builder_fn,
    mock_download,
    dummy_canvas_interaction,
    dummy_session,
):
    # 1) Arrange: make download_csv return an error message
    error_msg = "oh no, download failed"
    mock_download.return_value = error_msg

    # 2) Minimal builder so inputs_to_dict is well‐formed
    fake_builder = MagicMock()
    fake_builder.inputs_to_dict.return_value = {
        "input_block_metadata": ["meta_id"],
        "input_block_genome_mapping": ["genome_id"],
        "input_block_plates": ["plate"],
        "input_block_plate_specs": ["plate1_specs1"],
        "input_block_notebook_name": "notebook.ipynb",
    }
    mock_builder_fn.return_value = fake_builder

    # 3) Prepare fake App + session
    fake_app = MagicMock()
    MockAppClass.return_value = fake_app
    cm, session = dummy_session
    fake_app.create_session_context.return_value = cm

    # 4) Act + Assert: calling should raise AppUserFacingError with that message
    with pytest.raises(AppUserFacingError) as ei:
        canvas_interaction.route_interaction_webhook(fake_app, dummy_canvas_interaction)
    assert str(ei.value) == error_msg

    # 5) Also verify we tried to download the first ID
    mock_download.assert_called_once_with(
        app=fake_app, entit_id="meta_id", destination_dict=ANY
    )
