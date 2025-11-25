# canvas_initialize.py
from benchling_sdk.apps.canvas.framework import CanvasBuilder
from benchling_sdk.apps.canvas.types import UiBlock
from benchling_sdk.apps.framework import App
from benchling_sdk.models import (
    ButtonUiBlock,
    ButtonUiBlockType,
    MarkdownUiBlock,
    MarkdownUiBlockType,
    SearchInputUiBlock,
    SearchInputUiBlockType,
    SearchInputUiBlockItemType,
    TextInputUiBlock,
    TextInputUiBlockType,
    SearchInputMultiValueUiBlock,
    SearchInputMultiValueUiBlockType,
)
from local_app.benchling_app.views.constants import PROCESS_BUTTON_ID, TEXT_INPUT_ID
from benchling_sdk.models.webhooks.v0 import (
    CanvasCreatedWebhookV2Beta,
    CanvasInitializeWebhookV2,
)


# Constants to use across files


def render_text_canvas(app: App, canvas_initialized: CanvasInitializeWebhookV2) -> None:
    with app.create_session_context("Text Processor App", timeout_seconds=20):
        canvas_builder = CanvasBuilder(
            app_id=app.id,
            feature_id=canvas_initialized.feature_id,
            resource_id=canvas_initialized.resource_id,
        )
        canvas_builder.blocks.append(input_blocks())
        app.benchling.apps.create_canvas(canvas_builder.to_create())


def render_text_canvas_for_created_canvas(
    app: App, canvas_created: CanvasCreatedWebhookV2Beta
) -> None:
    with app.create_session_context("Text Processor App", timeout_seconds=20):
        canvas_builder = CanvasBuilder(
            app_id=app.id, feature_id=canvas_created.feature_id
        )
        canvas_builder.blocks.append(input_blocks())
        app.benchling.apps.update_canvas(
            canvas_created.canvas_id, canvas_builder.to_update()
        )


def input_blocks() -> list[UiBlock]:
    return [
        MarkdownUiBlock(
            id="instructions",
            type=MarkdownUiBlockType.MARKDOWN,
            value="# Create and register entities\nTag the 3 metadata files registered previously as CSV Entity in the search box below. You should be able to find them by typing something like @crRNA_metadata",
        ),
        SearchInputMultiValueUiBlock(
            id="input_block_metadata",
            type=SearchInputMultiValueUiBlockType.SEARCH_INPUT_MULTIVALUE,
            item_type=SearchInputUiBlockItemType.CUSTOM_ENTITY,
            value=[],
            schema_id=None,
            enabled=True,
        ),
        MarkdownUiBlock(
            id="comment1",
            type=MarkdownUiBlockType.MARKDOWN,
            value="Tag the 2 plate specifications in the following box",
        ),
        SearchInputMultiValueUiBlock(
            id="input_block_plate_specs",
            type=SearchInputMultiValueUiBlockType.SEARCH_INPUT_MULTIVALUE,
            item_type=SearchInputUiBlockItemType.CUSTOM_ENTITY,
            value=[],
            schema_id=None,
            enabled=True,
        ),
        MarkdownUiBlock(
            id="comment2",
            type=MarkdownUiBlockType.MARKDOWN,
            value="Tag the genome file and the mapping file in the box below",
        ),
        SearchInputMultiValueUiBlock(
            id="input_block_genome_mapping",
            type=SearchInputMultiValueUiBlockType.SEARCH_INPUT_MULTIVALUE,
            item_type=SearchInputUiBlockItemType.CUSTOM_ENTITY,
            value=[],
            schema_id=None,
            enabled=True,
        ),
        MarkdownUiBlock(
            id="comment3",
            type=MarkdownUiBlockType.MARKDOWN,
            value="Tag the 3 plates that were created just in the last table",
        ),
        SearchInputMultiValueUiBlock(
            id="input_block_plates",
            type=SearchInputMultiValueUiBlockType.SEARCH_INPUT_MULTIVALUE,
            item_type=SearchInputUiBlockItemType.PLATE,
            value=[],
            schema_id=None,
            enabled=True,
        ),
        MarkdownUiBlock(
            id="comment4",
            type=MarkdownUiBlockType.MARKDOWN,
            value="""Enter the **exact** notebook name in the box below, you can just copy-paste it from the top of the notebook, do **not** tag it""",
        ),
        TextInputUiBlock(
            id="input_block_notebook_name",
            type=TextInputUiBlockType.TEXT_INPUT,
            # item_type=SearchInputUiBlockItemType.CUSTOM_ENTITY,
            placeholder="Full run ____ - Material Preparation",
            value=None,
            # schema_id=None,
            enabled=True,
        ),
        ButtonUiBlock(
            id=PROCESS_BUTTON_ID,
            text="Process CSV",
            type=ButtonUiBlockType.BUTTON,
        ),
    ]
