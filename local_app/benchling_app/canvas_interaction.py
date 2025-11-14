import os
from typing import cast
import requests
from benchling_sdk.apps.canvas.framework import CanvasBuilder
from benchling_sdk.apps.framework import App
from benchling_sdk.apps.status.errors import AppUserFacingError
from benchling_sdk.models import AppCanvasUpdate
from benchling_sdk.models.webhooks.v0 import CanvasInteractionWebhookV2
from benchling_sdk.models import (
    ButtonUiBlock,
    ButtonUiBlockType,
    MarkdownUiBlock,
    MarkdownUiBlockType,
    TextInputUiBlock,
    TextInputUiBlockType,
    SearchInputUiBlock,
    SearchInputUiBlockType,
    SearchInputUiBlockItemType,
    ChipUiBlock,
    ChipUiBlockType,
)

from local_app.benchling_app.csv_utils import (
    download_csv,
    check_all_csv_exist,
    upload_csv,
    delete_csvs,
    # process_csv,
)
from local_app.benchling_app.notebook_utils import process_notebook
from local_app.benchling_app.create_register_entites import (
    load_and_clean_data,
    create_and_register_entities,
)
from local_app.benchling_app.plate_utils import find_and_fill_plates

from local_app.benchling_app.views.constants import PROCESS_BUTTON_ID, TEXT_INPUT_ID
from local_app.benchling_app.views.canvas_initialize import input_blocks

from local_app.lib.logger import get_logger

logger = get_logger()


class UnsupportedButtonError(Exception):
    pass


def route_interaction_webhook(
    app: App, canvas_interaction: CanvasInteractionWebhookV2
) -> None:
    canvas_id = canvas_interaction.canvas_id

    # When the button is pressed, do this here:
    if canvas_interaction.button_id == PROCESS_BUTTON_ID:
        with app.create_session_context("Process CSV", timeout_seconds=20) as session:

            session.attach_canvas(canvas_id)
            canvas_builder = _canvas_builder_from_canvas_id(app, canvas_id)
            canvas_inputs = (
                canvas_builder.inputs_to_dict()
            )  # .inputs_to_dict_single_value()

            # Extract the API IDs of the files given to canvas
            files_apis = (
                canvas_inputs["input_block_metadata"]
                + canvas_inputs["input_block_genome_mapping"]
                + canvas_inputs["input_block_plate_specs"]
            )
            plate_list = canvas_inputs["input_block_plates"]
            notebook_name = canvas_inputs["input_block_notebook_name"]

            destination_dict = {
                #  "crRNA_metadata": {
                #     "path": "external/crrna_dummy.csv",
                # },
                # dummies
                # "crRNA_metadata": "/external/113_195_crRNA_metadata_2025_02_27_188_189_act_dummy.csv",
                # "receiver_primers_metadata": "/external/rec_dummy.csv",
                # "screening_primers_metadata": "/external/crrna_plate_specs_dummy.csv",
                # "crRNA_plate_specs": "/external/crrna_specs_dummy.csv",
                # "primers_plate_specs": "/external/primers_specs_dummy.csv",
                # "strain_names_mapping": "/external/genome_dummy.csv",
                # "plate_location_mapping": "/external/mapping_dummy.csv",
                "crRNA_metadata": "/external/crrna_metadata.csv",
                "receiver_primers_metadata": "/external/rec_metadata.csv",
                "screening_primers_metadata": "/external/scr_metadata.csv",
                "crRNA_plate_specs": "/external/crrna_specs.csv",
                "primers_plate_specs": "/external/primers_specs.csv",
                "strain_names_mapping": "/external/genome.csv",
                "plate_location_mapping": "/external/mapping.csv",
            }
            # erase preloaded files
            for key, value in destination_dict.items():
                if os.path.isfile(value):
                    delete_csvs(value)

            #  Download all files

            for ent_id in files_apis:

                error_download_csv = download_csv(
                    app=app, entit_id=ent_id, destination_dict=destination_dict
                )

                if error_download_csv:
                    logger.warning(error_download_csv)
                    raise AppUserFacingError(error_download_csv)

            # commen tthis for when testing and not uploading all the files:
            # Check that all necessary files have been uploaded
            error_missing_csv = check_all_csv_exist(destination_dict)
            if error_missing_csv:
                logger.warning(error_missing_csv)
                raise AppUserFacingError(error_missing_csv)

            # Read the files, check they contain all the necessary information
            (
                crrna_df_merge,
                receivers_df_merge,
                screening_df_merge,
                genomes_df,
                mapping_df,
                error_load_data,
            ) = load_and_clean_data(destination_dict)
            if error_load_data:
                logger.warning(error_load_data)
                raise AppUserFacingError(error_load_data)

            # Register the entities
            # TODO uncomment lines to actually register the entities in the registries, currently just creating entities
            (
                clc_bac_df,
                crrna_df_merge,
                receivers_df_merge,
                screening_df_merge,
                error_create_and_register_entities,
            ) = create_and_register_entities(
                app,
                crrna_df_merge,
                receivers_df_merge,
                screening_df_merge,
                genomes_df,
                mapping_df,
            )

            if error_create_and_register_entities:
                logger.warning(str(error_create_and_register_entities))
                raise AppUserFacingError(error_create_and_register_entities)

            destination_dict, order_number, api_file_id = upload_csv(
                app=app,
                df=clc_bac_df,
                destination_dict=destination_dict,
                path="/processed/api_ids.csv",
                # folder_id=  b_api_ids.my_folder_id,  # "my_folder",
                # schema_id= b_api_ids.schema_id,  #"ts_WDtkRWgc",
            )

            # erase unnecessary files
            for key, value in destination_dict.items():
                delete_csvs(value)

            # Find and fill the plates with the corresponding oligos

            error_find_and_fill_plates = find_and_fill_plates(
                app=app,
                crrna_df_merge=crrna_df_merge,
                receivers_df_merge=receivers_df_merge,
                screening_df_merge=screening_df_merge,
                plate_list=plate_list,
                order_number=order_number,
            )

            if error_find_and_fill_plates:
                logger.warning(str(error_find_and_fill_plates))
                raise AppUserFacingError(error_find_and_fill_plates)

            # Find the results table and return the created entities as tags

            error_process_notebook = process_notebook(
                app=app, notebook_name=notebook_name, clc_bac_df=clc_bac_df
            )
            if error_process_notebook:
                logger.warning(error_process_notebook)
                raise AppUserFacingError(error_process_notebook)

                # api_file_id

            results_blocks = [
                MarkdownUiBlock(
                    id="results_display",
                    type=MarkdownUiBlockType.MARKDOWN,
                    value="""
                    Success!!!\n Please click the plug icon in the toolbar of the **CLC BAC Entities Registration Output** results 
                    table (just underneath) to integrate pending results. It might take a couple of seconds for the icon to activate, if 
                    it doesn't, refresh the page to activate it. \n Also please check that the plates have been filled up (hovering 
                    on top of their tags should already show that they are not empty anymore).
                    
                
                    The following file will be necessary if many/all of the entities that you just created 
                    need to be modified or achived computationally.
                    """,
                ),
                ChipUiBlock(
                    id="file_with_apis", type=ChipUiBlockType.CHIP, value=api_file_id
                ),
            ]

            canvas_update = canvas_builder.with_blocks(results_blocks).to_update()
            session.app.benchling.apps.update_canvas(canvas_id, canvas_update)

    else:
        # Re-enable the Canvas, or it will stay disabled and the user will be stuck
        app.benchling.apps.update_canvas(canvas_id, AppCanvasUpdate(enabled=True))
        # Not shown to user by default, for our own logs cause we forgot to handle some button
        raise UnsupportedButtonError(
            f"Whoops, the developer forgot to handle the button {canvas_interaction.button_id}",
        )


"""
if canvas_interaction.button_id == "PROCESS_BUTTON_ID":
        # Optionally start the container (only if not running)
        subprocess.run(["docker", "run", "-d", "-p", "8501:8501", "your-image-name"])

        # Respond with link to app
        return CanvasResponse(actions=[ # I don't know if this even exists
            OpenUrlAction(
                url="http://<your-vm-ip>:8501",
                target="new"
            )
        ])

        # instead of the response maybe use this, maybe not suported to oopen links, check first:"""
# results_blocks = [
#         MarkdownUiBlock(
#             id="results_display",
#             type=MarkdownUiBlockType.MARKDOWN,
#             value="""If the app doesnt appear in a new tab click here: test link, that probably doesn't work... [Duck Duck Go](https://duckduckgo.com) """,
#         )
#     ]


def _canvas_builder_from_canvas_id(app: App, canvas_id: str) -> CanvasBuilder:
    current_canvas = app.benchling.apps.get_canvas_by_id(canvas_id)
    return CanvasBuilder.from_canvas(current_canvas)
