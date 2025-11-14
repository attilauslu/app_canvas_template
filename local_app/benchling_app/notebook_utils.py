"""
notebook_utils.py
Description: Find the specific notebook and results table and fill it up with the api ids previously extracted.
Author: Laia Meseguer Monfort
Date: April 11, 2025
"""

# ==================================
# IMPORTS
# ==================================
import os

from benchling_sdk.apps.framework import App
from benchling_sdk.models import CustomEntity, AssayResultCreate

from benchling_sdk.helpers.serialization_helpers import fields


# ==================================
# FUNCTIONS
# ==================================

# ================================== Find notebook and table ==================================


def find_results_table(app: App, notebook_api_id, CLC_results_table_schema_id):

    notebook_entries = app.benchling.entries.get_entry_by_id(notebook_api_id)
    for entry in notebook_entries.days:
        for e in entry.notes:
            try:
                if e.type == "results_table":
                    if e.assay_result_schema_id == CLC_results_table_schema_id:
                        return e.api_id, False
                        # CLC_results_table_api_id = e.api_id
                        # error = False
            except:
                pass
                # return None, True
                # CLC_results_table_api_id = None
                # error = True

    return None, True


def process_notebook(
    app: App, notebook_name: str, clc_bac_df
) -> None:  # , destination_path: Path) -> None:
    """
    Generates a client from an existing App object, and uses this to connect to benchling.
    With the name of a notebook finds its id and maybe goes though it?
    """

    config = app.config_store.config_by_path

    notebook_entries = app.benchling.entries.list_entries(name=notebook_name)
    TESTING = os.getenv("APP_ENV") == "test"

    # get the noebook ID
    for entry in notebook_entries:
        notebook_api_id = entry[0].id

    result_table_api_id, error_find_results_table = find_results_table(
        app,
        notebook_api_id,
        config(["Result schema"])
        .required()
        .value_str(),  # b_api_ids.CLC_results_table_schema_id
    )
    if error_find_results_table:
        return f"The results table cannot be retreived, Is this {notebook_name} the correct notebook name? Is there a CLC BAC Entities Registration Output results table in this notebook?"

    bulk_entities = []

    for _, row in clc_bac_df.iterrows():

        if TESTING:
            fields_dict = {
                "sample": {"value": row["bac_b_id"]},
            }
        else:
            fields_dict = {
                "bac": {"value": row["bac_b_id"]},
                "well96": {"value": row["well_96"]},
                "grna_up": {"value": row["grna_U"]},
                "grna_down": {"value": row["grna_D"]},
                "receiver_primer_pbe45": {"value": row["rec_primer_D_45"]},
                "receiver_primer_pbe48": {"value": row["rec_primer_U_48"]},
                "receiver_assembly_pbe45": {"value": row["clc_rec_primer_D_45"]},
                "receiver_assembly_pbe48": {"value": row["clc_rec_primer_U_48"]},
                "screening_primer_cf": {"value": row["scr_primer_f"]},
                "screening_primer_cr": {"value": row["scr_primer_r"]},
                "strain": {"value": row["strain"]},
                "dna_fragment": {"value": row["dna_fragment"]},
            }

        entityToCreate = AssayResultCreate(
            project_id=config(["Project schema"])
            .required()
            .value_str(),  # b_api_ids.project_id,
            schema_id=config(["Result schema"])
            .required()
            .value_str(),  # b_api_ids.CLC_results_table_schema_id,
            fields=fields(
                fields_dict
                # {
                #     "sample": {"value": row["bac_b_id"]},
                #     # "bac": {"value": row["bac_b_id"] },
                #     # "well96": {"value": row["well_96"] },
                #     # "grna_up": {"value": row["grna_U"] },
                #     # "grna_down": {"value": row["grna_D"]  },
                #     # "receiver_primer_pbe45": {"value": row["rec_primer_D_45"]  },
                #     # "receiver_primer_pbe48": {"value": row["rec_primer_U_48"]  },
                #     # "receiver_assembly_pbe45": {"value": row["clc_rec_primer_D_45"]  },
                #     # "receiver_assembly_pbe48": {"value": row["clc_rec_primer_U_48"] },
                #     # "screening_primer_cf": {"value": row["scr_primer_f"] },
                #     # "screening_primer_cr": {"value": row["scr_primer_r"] },
                #     # "strain": {"value": row["strain"] },
                #     # "dna_fragment": {"value": row["dna_fragment"] },
                # }
            ),
        )

        bulk_entities.append(entityToCreate)

    app.benchling.assay_results.bulk_create(
        assay_results=bulk_entities, table_id=result_table_api_id
    )  # dna_sequences.bulk_create(bulk_entities)

    return None
