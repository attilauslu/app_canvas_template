"""
create_register_entities.py
Description: Application of the benchling code to generate and register all the entities related to CLC OligoCraft
Author: Laia Meseguer Monfort
Date: April 11, 2025
"""

# ==================================
# IMPORTS
# ==================================


import pandas as pd
import os

from benchling_sdk.apps.framework import App
from benchling_sdk.helpers.serialization_helpers import fields
from benchling_sdk.helpers.task_helpers import TaskHelper
from benchling_sdk.models import (
    CustomEntityBulkCreate,
    DnaSequenceBulkCreate,
    NamingStrategy,
)

import local_app.benchling_app.benchling_api_ids as b_api_ids
from local_app.lib.logger import get_logger


# ==================================
# FUNCTIONS
# ==================================

logger = get_logger()

TESTING = os.getenv("APP_ENV") == "test"


def registry_kwargs(registry_id):
    """
    Returns a dict of registry args if we’re in prod,
    or an empty dict if TESTING.
    """
    if TESTING:
        return {}
    return {
        "registry_id": registry_id,
        "naming_strategy": NamingStrategy.NEW_IDS,
    }


# ================================== Load and clean data ==================================


def read_and_basic_qc(filepath, min_cols):
    # check that it can be read
    try:
        df = pd.read_csv(filepath, index_col=False)
    except:

        return (
            pd.DataFrame(),
            f"Can't read  {filepath}. Are you sure the file you uploaded has the correct CSV format?",
        )

    # that is not missing any important columns
    missing = [col for col in min_cols if col not in df.columns]
    if missing:
        return pd.DataFrame(), f"Missing required columns: {missing} in file {filepath}"

    # And clean fully empty rows
    df = df.dropna(how="all")
    return df, None


def normalize_plate_df(df, columns_to_keep, rename_map):
    """Selects and renames specific columns from a plate spec DataFrame."""
    df = df[columns_to_keep]
    return df.rename(columns=rename_map)


def validate_plate(
    metadata_df,
    plate_specs_df,
    merged_df,
    *,
    original_id_col,  # crRNA_id
    spec_id_col,
    original_well_col,
    spec_well_col,
    original_seq_col,
    spec_seq_col,
    reference_label="item",
):

    df_missing = metadata_df[
        ~metadata_df[original_id_col].isin(plate_specs_df[spec_id_col])
    ]

    if not df_missing.empty:
        return False, f"Missing the following {reference_label}: {df_missing} "

    for _, row in merged_df.iterrows():
        if not row[spec_id_col].startswith("CLCact"):
            # ref_seq = transform_seq(row[original_seq_col]) if transform_seq else row[original_seq_col]
            if row[original_well_col] != row[spec_well_col]:
                return (
                    False,
                    f"{reference_label} {row[spec_id_col]}: has changed wells in plate",
                )

            if row[original_seq_col] != row[spec_seq_col]:
                return (
                    False,
                    f"{reference_label} {row[spec_id_col]}: has different sequence",
                )

    return True, ""


def cleaning_crrna(crrna_df, crrna_plate_specs_df):
    columns = ["Well Position", "Sequence Name", "Sequence", "µg"]
    rename_map = {
        "Well Position": "well_crrna_idt",
        "Sequence Name": "crRNA_id",
        "Sequence": "crrna_seq_idt_original",
        "µg": "ug",
    }

    crrna_plate_specs_df = normalize_plate_df(crrna_plate_specs_df, columns, rename_map)

    # Clean crRNA sequences
    crrna_plate_specs_df["crrna_seq_short"] = (
        crrna_plate_specs_df["crrna_seq_idt_original"]
        .str.replace(
            "/AltR1/rUrArA rUrUrU rCrUrA rCrUrA rArGrU rGrUrA rGrArU ", "", regex=False
        )
        .str.replace(" /AltR2/", "", regex=False)
        .str.replace("r", "", regex=False)
        .str.replace(" ", "", regex=False)
    )
    crrna_plate_specs_df["crrna_seq_idt"] = (
        crrna_plate_specs_df["crrna_seq_idt_original"]
        .str.replace("/AltR1/", "", regex=False)
        .str.replace(" /AltR2/", "", regex=False)
        .str.replace("r", "", regex=False)
        .str.replace(" ", "", regex=False)
    )

    crrna_df_merge = pd.merge(
        crrna_plate_specs_df,
        crrna_df,
        how="left",
        on="crRNA_id",  # ,    suffixes=('', '_idt')  # Left DataFrame keeps original names, right gets '_idt'
    )

    # Add prefixes and fixed crRNA for act
    crrna_df_merge["BGC_number"] = crrna_df_merge["BGC_number"].apply(
        lambda x: str(int(x)) if pd.notna(x) else x
    )
    crrna_df_merge.loc[
        crrna_df_merge["crRNA_id"].str.startswith("CLCact"), "BGC_number"
    ] = "act"
    crrna_df_merge.loc[
        crrna_df_merge["crRNA_id"].str.startswith("CLCactcrRNAU"), "crRNA_prefix"
    ] = "U"
    crrna_df_merge.loc[
        crrna_df_merge["crRNA_id"].str.startswith("CLCactcrRNAD"), "crRNA_prefix"
    ] = "D"
    crrna_df_merge.loc[
        crrna_df_merge["crRNA_id"].str.startswith("CLCactcrRNAU"), "crRNA"
    ] = "GAATATGGGGCCACCCCCCAC"
    crrna_df_merge.loc[
        crrna_df_merge["crRNA_id"].str.startswith("CLCactcrRNAD"), "crRNA"
    ] = "GCCTTTGCTTGCCTGGGCCAA"

    crrna_df_merge["RNA_crrna"] = (
        crrna_df_merge["crRNA"].str.replace("T", "U").str.replace("t", "u")
    )

    valid, error = validate_plate(
        metadata_df=crrna_df,
        plate_specs_df=crrna_plate_specs_df,
        merged_df=crrna_df_merge,
        original_id_col="crRNA_id",  # crRNA_id
        spec_id_col="crRNA_id",
        original_well_col="Well Position",
        spec_well_col="well_crrna_idt",
        original_seq_col="RNA_crrna",
        spec_seq_col="crrna_seq_short",
        reference_label="crRNA",
    )

    if valid:
        return (
            crrna_df_merge[
                [
                    "well_crrna_idt",
                    "crRNA_id",
                    "crrna_seq_idt",
                    "crRNA",
                    "BGC_number",
                    "strain_name",
                    "crRNA_prefix",
                    "crRNA_strand",
                    "crRNA_loc",
                    "ug",
                ]
            ],
            "",
        )

    else:
        return pd.DataFrame(), error


def cleaning_receivers(receivers_df, primers_plate_specs_df):
    columns = ["Well Position", "Sequence Name", "Sequence", "Final Volume µL "]
    rename_map = {
        "Well Position": "well_primer_idt",
        "Sequence Name": "primer_id",
        "Sequence": "primer_seq_idt",
        "Final Volume µL ": "ul_primers",
    }

    primers_plate_specs_df = normalize_plate_df(
        primers_plate_specs_df, columns, rename_map
    )

    # Clean receiver sequences
    primers_plate_specs_df["primer_seq_idt"] = primers_plate_specs_df[
        "primer_seq_idt"
    ].str.replace(" ", "", regex=False)

    # Merge df
    receivers_df_merge = pd.merge(
        primers_plate_specs_df,
        receivers_df,
        how="left",
        right_on="receiver_primer_id",
        left_on="primer_id",
    )

    # Add prefixes and fixed receivers for act
    receivers_df_merge["BGC_number"] = receivers_df_merge["BGC_number"].apply(
        lambda x: str(int(x)) if pd.notna(x) else x
    )
    receivers_df_merge.loc[
        receivers_df_merge["primer_id"].str.startswith("CLCact4"), "receiver_primer_id"
    ] = receivers_df_merge["primer_id"]
    receivers_df_merge.loc[
        receivers_df_merge["primer_id"].str.startswith("CLCact4"), "BGC_number"
    ] = "act"
    receivers_df_merge.loc[
        receivers_df_merge["primer_id"].str.startswith("CLCact48"), "crRNA_prefix"
    ] = "U"
    receivers_df_merge.loc[
        receivers_df_merge["primer_id"].str.startswith("CLCact45"), "crRNA_prefix"
    ] = "D"

    receivers_df_merge = receivers_df_merge.dropna(subset=["receiver_primer_id"])

    valid, error = validate_plate(
        metadata_df=receivers_df,
        plate_specs_df=primers_plate_specs_df,
        merged_df=receivers_df_merge,
        original_id_col="receiver_primer_id",  # crRNA_id
        spec_id_col="primer_id",
        original_well_col="Well Position",
        spec_well_col="well_primer_idt",
        original_seq_col="receiver_primer_seq",
        spec_seq_col="primer_seq_idt",
        reference_label="Receiver",
    )

    if valid:
        return (
            receivers_df_merge[
                [
                    "well_primer_idt",
                    "primer_seq_idt",
                    "ul_primers",
                    "BGC_number",
                    "crRNA_prefix",
                    "receiver_primer_id",
                ]
            ],
            primers_plate_specs_df,
            "",
        )

    else:
        return pd.DataFrame(), pd.DataFrame(), error


def cleaning_screening(
    screening_df, primers_plate_specs_df_clean, only_make_idt_excel_for_C_primers=True
):

    screening_df["BGC_number"] = screening_df["locus tag"].str.extract(r"CLC(\d{3})")

    # keep only C screening primers
    if only_make_idt_excel_for_C_primers:
        screening_df = screening_df[screening_df["locus tag"].str.endswith("C")]

    screening_df["BGC_number"] = pd.to_numeric(screening_df["BGC_number"])
    screening_df = screening_df.set_index("BGC_number", drop=False)

    f_screening_df = screening_df[
        [
            "BGC_number",
            "locus tag",
            "f_primer_name",
            "f_primer_sequences(5-3)",
            "f_well_position",
        ]
    ].copy()
    r_screening_df = screening_df[
        [
            "BGC_number",
            "locus tag",
            "r_primer_name",
            "r_primer_sequences(5-3)",
            "r_well_position",
        ]
    ].copy()

    f_screening_df["sufix"] = "F"
    r_screening_df["sufix"] = "R"

    new_column_names = [
        "BGC_number",
        "locus tag",
        "primer_name",
        "primer_sequences",
        "well_position",
        "sufix",
    ]

    f_screening_df.columns = new_column_names
    r_screening_df.columns = new_column_names

    screening_df_concat = pd.concat([f_screening_df, r_screening_df], ignore_index=True)

    screening_df_merge = pd.merge(
        primers_plate_specs_df_clean,
        screening_df_concat,
        how="left",
        left_on="primer_id",
        right_on="primer_name",
        # ,    suffixes=('', '_idt')  # Left DataFrame keeps original names, right gets '_idt'
    )
    screening_df_merge["BGC_number"] = screening_df_merge["BGC_number"].apply(
        lambda x: str(int(x)) if pd.notna(x) else x
    )
    screening_df_merge.loc[
        screening_df_merge["primer_id"].str.startswith("CLCactC"), "primer_name"
    ] = screening_df_merge["primer_id"]
    screening_df_merge.loc[
        screening_df_merge["primer_id"].str.startswith("CLCactC"), "BGC_number"
    ] = "act"
    screening_df_merge.loc[
        screening_df_merge["primer_id"].str.startswith("CLCactCF"), "sufix"
    ] = "F"
    screening_df_merge.loc[
        screening_df_merge["primer_id"].str.startswith("CLCactCR"), "sufix"
    ] = "R"

    screening_df_merge = screening_df_merge.dropna(subset=["primer_name"])

    valid, error = validate_plate(
        metadata_df=screening_df_concat,
        plate_specs_df=primers_plate_specs_df_clean,
        merged_df=screening_df_merge,
        original_id_col="primer_name",  # crRNA_id
        spec_id_col="primer_id",
        original_well_col="well_position",
        spec_well_col="well_primer_idt",
        original_seq_col="primer_sequences",
        spec_seq_col="primer_seq_idt",
        reference_label="Screening",
    )

    if valid:
        return screening_df_merge, ""

    else:
        return pd.DataFrame(), error


def load_and_clean_data(destination_dict):
    only_make_idt_excel_for_C_primers = True
    errors_load_and_clean_data = []
    # load all the data and clean it
    crrna_df, error_load_crrna_df = read_and_basic_qc(
        # filepath="/downloaded_files/113_195_crRNA_metadata_2025_02_27_188_189_act_dummy.csv",
        filepath=destination_dict["crRNA_metadata"],
        min_cols=[
            "BGC_number",
            "strain_name",
            "crRNA_prefix",
            "crRNA_strand",
            "crRNA_loc",
            "crRNA_id",
            "crRNA",
            "Well Position",
            "Order ID",
        ],
    )
    if error_load_crrna_df:
        errors_load_and_clean_data.append(error_load_crrna_df)

    rec_df, error_load_rec_df = read_and_basic_qc(
        # filepath="/downloaded_files/113_195_receiver_primers_metadata_2025_02_27_188_189_act_dummy.csv",
        filepath=destination_dict["receiver_primers_metadata"],
        min_cols=[
            "BGC_number",
            "receiver_primer_id",
            "crRNA_prefix",
            "receiver_primer_seq",
            "Well Position",
        ],
    )
    if error_load_rec_df:
        errors_load_and_clean_data.append(error_load_rec_df)

    src_df, error_load_src_df = read_and_basic_qc(
        # filepath="/downloaded_files/113_195_screening_primers_metadata_2025_02_27_188_189_act_dummy.csv",
        filepath=destination_dict["screening_primers_metadata"],
        min_cols=[
            "BGC_num",
            "locus tag",
            "f_primer_name",
            "f_primer_sequences(5-3)",
            "f_well_position",
            "r_primer_name",
            "r_primer_sequences(5-3)",
            "r_well_position",
        ],
    )
    if error_load_src_df:
        errors_load_and_clean_data.append(error_load_src_df)

    crrna_plate_specs_df, error_load_crrna_plate_specs_df = read_and_basic_qc(
        # filepath="/downloaded_files/Plate_Specs_CLC_PlateA4_crRNA_188_189_act_dummy.csv",
        filepath=destination_dict["crRNA_plate_specs"],
        min_cols=["Well Position", "Sequence Name", "Sequence", "µg"],
    )
    if error_load_crrna_plate_specs_df:
        errors_load_and_clean_data.append(error_load_crrna_plate_specs_df)

    primers_plate_specs_df, error_load_primers_plate_specs_df = read_and_basic_qc(
        # filepath="/downloaded_files/Plate_Specs_CLC_PlateB4_receivers_screening_primers_188_189_act_dummy.csv",
        filepath=destination_dict["primers_plate_specs"],
        min_cols=["Well Position", "Sequence Name", "Sequence", "Final Volume µL "],
    )
    if error_load_primers_plate_specs_df:
        errors_load_and_clean_data.append(error_load_primers_plate_specs_df)

    genomes_df, error_load_genomes_df = read_and_basic_qc(
        # filepath="/downloaded_files/genomes_188_189_act_dummy.csv",
        filepath=destination_dict["strain_names_mapping"],
        min_cols=["benchling_name", "selection_name"],
    )
    if error_load_genomes_df:
        errors_load_and_clean_data.append(error_load_genomes_df)

    mapping_df, error_load_mapping_df = read_and_basic_qc(
        # filepath="/downloaded_files/113_195_384_96_mapping_2025_02_27_188_189_act_dummy.csv",
        filepath=destination_dict["plate_location_mapping"],
        min_cols=["BGC_number", "96_well_formatted"],
    )
    if error_load_mapping_df:
        errors_load_and_clean_data.append(error_load_mapping_df)

    if errors_load_and_clean_data:
        return (
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            errors_load_and_clean_data,
        )
        # log it and return it

    crrna_df_merge, error_crrna = cleaning_crrna(crrna_df, crrna_plate_specs_df)
    receivers_df_merge, primers_plate_specs_df, error_receivers = cleaning_receivers(
        rec_df, primers_plate_specs_df
    )
    screening_df_merge, error_screening = cleaning_screening(
        src_df, primers_plate_specs_df
    )

    error_cleaning = []
    if error_crrna:
        error_cleaning.append(error_crrna)

    if error_receivers:
        error_cleaning.append(error_receivers)

    if error_screening:
        error_cleaning.append(error_screening)

    if error_cleaning:
        return (
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            error_cleaning,
        )

    mapping_df = mapping_df.dropna(subset=["BGC_number"])
    mapping_df = mapping_df.drop_duplicates(subset=["BGC_number"], keep="first")
    mapping_df = mapping_df.set_index("BGC_number", drop=False)

    return (
        crrna_df_merge,
        receivers_df_merge,
        screening_df_merge,
        genomes_df,
        mapping_df,
        None,
    )


# ================================== generate entities and save API IDs ==================================


def bulk_register_entities(
    app,
    entity_type,
    df,
    name_column,
    output_column,
    folder_id,
    schema_id,
    entity_builder_fn,
):
    """
    Generic helper to bulk register DNA or custom entities in Benchling.
    """
    list_func = {
        "dna": app.benchling.dna_sequences.list,
        "custom": app.benchling.custom_entities.list,
    }[entity_type]

    create_func = {
        "dna": app.benchling.dna_sequences.bulk_create,
        "custom": app.benchling.custom_entities.bulk_create,
    }[entity_type]

    pre_existing = list_func(folder_id=folder_id, schema_id=schema_id)
    name_to_id = {item.name: item.id for batch in pre_existing for item in batch}

    df[output_column] = None
    bulk_entities = []

    for idx, row in df.iterrows():
        entity_name = row[name_column]

        if entity_name in name_to_id:
            print(f"{entity_name} already exists.")
            logger.info(f"{entity_name} already exists.")
            df.at[idx, output_column] = name_to_id[entity_name]
        else:
            entity = entity_builder_fn(row)
            bulk_entities.append(entity)

    if bulk_entities:
        task = create_func(bulk_entities)
        response = task.wait_for_response()

        if entity_type == "dna":
            results = response.dna_sequences
        elif entity_type == "custom":
            results = response.custom_entities
        else:
            raise ValueError(f"Unsupported entity_type: {entity_type}")

        id_map = {ent.name: ent.id for ent in results}
        df.loc[df[name_column].isin(id_map), output_column] = df[name_column].map(
            id_map
        )

    return df


def register_crrna(app, df, folder_id, schema_id, registry):
    def build_entity(row):
        if row["crRNA_id"].startswith("CLCact"):
            return DnaSequenceBulkCreate(
                folder_id=folder_id,
                name=row["crRNA_id"],
                is_circular=False,
                bases=row["crRNA"],
                schema_id=schema_id,
                **registry_kwargs(registry),
                # registry_id= registry, #biosustain_registry,
                # naming_strategy=NamingStrategy.NEW_IDS
            )
        else:
            return DnaSequenceBulkCreate(
                folder_id=folder_id,
                name=row["crRNA_id"],
                is_circular=False,
                bases=row["crRNA"],
                schema_id=schema_id,
                **registry_kwargs(registry),
                fields=fields(
                    {
                        "Target strand": {"value": str(row["crRNA_strand"])},
                        "Target position": {"value": int(row["crRNA_loc"])},
                    }
                ),
            )

    return bulk_register_entities(
        app, "dna", df, "crRNA_id", "crRNA_b_id", folder_id, schema_id, build_entity
    )


def register_receivers(app, df, folder_id, schema_id, registry):
    def build_entity(row):
        return DnaSequenceBulkCreate(
            folder_id=folder_id,
            name=row["receiver_primer_id"],
            is_circular=False,
            bases=row["primer_seq_idt"],
            schema_id=schema_id,
            **registry_kwargs(registry),
        )

    return bulk_register_entities(
        app=app,
        entity_type="dna",
        df=df,
        name_column="receiver_primer_id",
        output_column="receiver_primer_b_id",
        folder_id=folder_id,
        schema_id=schema_id,
        entity_builder_fn=build_entity,
    )


# def clc_bac_clc_receivers_kwargs():
#     """
#     Returns the extra kwargs for DnaSequenceBulkCreate that should
#     be included only when TESTING.
#     """
#     if TESTING:
#         return {
#             "bases":       "",        # test-only
#             "is_circular": False,     # test-only
#         }
#     else:
#         return {}
def register_clc_receivers(app, df, folder_id, schema_id, registry):
    # Step 1: Preprocess DataFrame to calculate clc_receiver_name
    def calc_receiver_name(row):
        prefix = "pBE48-" if row["crRNA_prefix"] == "U" else "pBE45-"
        return prefix + str(row["receiver_primer_id"])

    df["clc_receiver_name"] = df.apply(calc_receiver_name, axis=1)

    if TESTING:

        def build_entity(row):
            if row["crRNA_prefix"] == "U":
                backbone_primer = b_api_ids.backbone_primer48
                backbone_plasmid = b_api_ids.backbone_plasmid48
            else:
                backbone_primer = b_api_ids.backbone_primer45
                backbone_plasmid = b_api_ids.backbone_plasmid45

            return DnaSequenceBulkCreate(
                folder_id=folder_id,
                name=row["clc_receiver_name"],
                fields=fields(
                    {
                        "Backbone": {"value": backbone_plasmid},
                        "Backbone primer": {"value": backbone_primer},
                        "Homology primer": {"value": row["receiver_primer_b_id"]},
                    }
                ),
                schema_id=schema_id,
                bases="",  # not always necessary
                is_circular=False,  # also might be unnecessary later
                # **clc_bac_clc_receivers_kwargs(),
                **registry_kwargs(registry),
            )

        return bulk_register_entities(
            app=app,
            entity_type="dna",  # custome
            df=df,
            name_column="clc_receiver_name",
            output_column="clc_receiver_b_id",
            folder_id=folder_id,
            schema_id=schema_id,
            entity_builder_fn=build_entity,
        )

    else:

        def build_entity(row):
            if row["crRNA_prefix"] == "U":
                backbone_primer = b_api_ids.backbone_primer48
                backbone_plasmid = b_api_ids.backbone_plasmid48
            else:
                backbone_primer = b_api_ids.backbone_primer45
                backbone_plasmid = b_api_ids.backbone_plasmid45

            return CustomEntityBulkCreate(
                folder_id=folder_id,
                name=row["clc_receiver_name"],
                fields=fields(
                    {
                        "Backbone": {"value": backbone_plasmid},
                        "Backbone primer": {"value": backbone_primer},
                        "Homology primer": {"value": row["receiver_primer_b_id"]},
                    }
                ),
                schema_id=schema_id,
                **registry_kwargs(registry),
            )

        return bulk_register_entities(
            app=app,
            entity_type="custom",
            df=df,
            name_column="clc_receiver_name",
            output_column="clc_receiver_b_id",
            folder_id=folder_id,
            schema_id=schema_id,
            entity_builder_fn=build_entity,
        )


def register_screening(app, df, folder_id, schema_id, registry):
    def build_entity(row):
        return DnaSequenceBulkCreate(
            folder_id=folder_id,
            name=row["primer_id"],
            is_circular=False,
            bases=row["primer_seq_idt"],
            schema_id=schema_id,
            **registry_kwargs(registry),
        )

    return bulk_register_entities(
        app=app,
        entity_type="dna",
        df=df,
        name_column="primer_id",
        output_column="primer_b_id",
        folder_id=folder_id,
        schema_id=schema_id,
        entity_builder_fn=build_entity,
    )


def find_genomes(app, df, folder_clc_id, folder_nbc_id, schema_id):
    pre_recorded_strains_clc = app.benchling.custom_entities.list(
        folder_id=folder_clc_id, schema_id=schema_id
    )
    pre_recorded_strains_nbc = app.benchling.custom_entities.list(
        folder_id=folder_nbc_id, schema_id=schema_id
    )

    pre_recorded_strains_dict = {}
    for g in pre_recorded_strains_clc:
        for info in g:

            pre_recorded_strains_dict[info.name] = info.id

    for g in pre_recorded_strains_nbc:
        for info in g:

            pre_recorded_strains_dict[info.name] = info.id

    df["genome_b_id"] = None
    for idx, row in df.iterrows():
        if row["benchling_name"] in pre_recorded_strains_dict:
            df.at[idx, "genome_b_id"] = pre_recorded_strains_dict[row["benchling_name"]]

        else:
            return (
                pd.DataFrame(),
                f"Check if the genome {row['benchling_name']} can be found in Benchling and if it is correctly named in the genomes file.",
            )

    return df, None


def register_dna_fragments(app, df, folder_id, schema_id, registry):
    df["dna_fragment"] = df["benchling_name"] + " gDNA"

    def build_entity(row):
        return DnaSequenceBulkCreate(
            folder_id=folder_id,
            name=row["dna_fragment"],
            is_circular=False,
            bases="",
            schema_id=schema_id,
            fields=fields({"Template strains": {"value": [row["genome_b_id"]]}}),
            **registry_kwargs(registry),
        )

    return bulk_register_entities(
        app=app,
        entity_type="dna",
        df=df,
        name_column="dna_fragment",
        output_column="dna_fragment_b_id",
        folder_id=folder_id,
        schema_id=schema_id,
        entity_builder_fn=build_entity,
    )


def register_clc_bac(
    app,
    crrna_df_merge,
    receivers_df_merge,
    screening_df_merge,
    genome_df,
    mapping_df,
    min_clust,  # TODO this should change to a list/set of all the values so we can skip if not available
    max_clust,
    folder_id,
    schema_id,
    registry,
):

    clc_bac_rows = []

    for i in range(int(min_clust), int(max_clust) + 1):
        bac_name = "CLC" + str(i).zfill(3)

        grna_U = crrna_df_merge.loc[
            (crrna_df_merge["BGC_number"] == str(i).zfill(3))
            & (crrna_df_merge["crRNA_prefix"] == "U"),
            "crRNA_b_id",
        ].values[0]
        grna_D = crrna_df_merge.loc[
            (crrna_df_merge["BGC_number"] == str(i).zfill(3))
            & (crrna_df_merge["crRNA_prefix"] == "D"),
            "crRNA_b_id",
        ].values[0]

        # genome_df : if there is benchling_name  there is also selection_name
        strain_name = crrna_df_merge.loc[
            (crrna_df_merge["BGC_number"] == str(i).zfill(3))
            & (crrna_df_merge["crRNA_prefix"] == "D"),
            "strain_name",
        ].values[0]
        strain = genome_df.loc[
            genome_df["selection_name"] == strain_name, "genome_b_id"
        ].values[0]
        dna_fragment = genome_df.loc[
            (genome_df["selection_name"] == strain_name), "dna_fragment_b_id"
        ].values[0]

        rec_primer_U_48 = receivers_df_merge.loc[
            (receivers_df_merge["BGC_number"] == str(i).zfill(3))
            & (receivers_df_merge["crRNA_prefix"] == "U"),
            "receiver_primer_b_id",
        ].values[0]
        rec_primer_D_45 = receivers_df_merge.loc[
            (receivers_df_merge["BGC_number"] == str(i).zfill(3))
            & (receivers_df_merge["crRNA_prefix"] == "D"),
            "receiver_primer_b_id",
        ].values[0]
        clc_rec_primer_U_48 = receivers_df_merge.loc[
            (receivers_df_merge["BGC_number"] == str(i).zfill(3))
            & (receivers_df_merge["crRNA_prefix"] == "U"),
            "clc_receiver_b_id",
        ].values[0]
        clc_rec_primer_D_45 = receivers_df_merge.loc[
            (receivers_df_merge["BGC_number"] == str(i).zfill(3))
            & (receivers_df_merge["crRNA_prefix"] == "D"),
            "clc_receiver_b_id",
        ].values[0]

        scr_primer_f = screening_df_merge.loc[
            (screening_df_merge["BGC_number"] == str(i).zfill(3))
            & (screening_df_merge["sufix"] == "F"),
            "primer_b_id",
        ].values[0]
        scr_primer_r = screening_df_merge.loc[
            (screening_df_merge["BGC_number"] == str(i).zfill(3))
            & (screening_df_merge["sufix"] == "R"),
            "primer_b_id",
        ].values[0]

        well = mapping_df.at[i, "96_well_formatted"]

        clc_bac_rows.append(
            {
                "bac_name": bac_name,
                "BGC_number": str(i).zfill(3),
                "grna_U": grna_U,
                "grna_D": grna_D,
                "strain_name": strain,
                "dna_fragment": dna_fragment,
                "rec_primer_U_48": rec_primer_U_48,
                "rec_primer_D_45": rec_primer_D_45,
                "clc_rec_primer_U_48": clc_rec_primer_U_48,
                "clc_rec_primer_D_45": clc_rec_primer_D_45,
                "scr_primer_f": scr_primer_f,
                "scr_primer_r": scr_primer_r,
                "well_96": well,
            }
        )

    # actinorhodin:
    act_num = crrna_df_merge.loc[
        (crrna_df_merge["BGC_number"] == "act")
        & (crrna_df_merge["crRNA_prefix"] == "U"),
        "crRNA_id",
    ].values[0]
    bac_name = f"CLCact_{act_num[-3:]}"

    grna_U = crrna_df_merge.loc[
        (crrna_df_merge["BGC_number"] == "act")
        & (crrna_df_merge["crRNA_prefix"] == "U"),
        "crRNA_b_id",
    ].values[0]
    grna_D = crrna_df_merge.loc[
        (crrna_df_merge["BGC_number"] == "act")
        & (crrna_df_merge["crRNA_prefix"] == "D"),
        "crRNA_b_id",
    ].values[0]

    strain_name = "Streptomyces coelicolor M145"
    strain = genome_df.loc[
        genome_df["selection_name"] == strain_name, "genome_b_id"
    ].values[0]
    dna_fragment = genome_df.loc[
        (genome_df["selection_name"] == strain_name), "dna_fragment_b_id"
    ].values[0]

    rec_primer_U_48 = receivers_df_merge.loc[
        (receivers_df_merge["BGC_number"] == "act")
        & (receivers_df_merge["crRNA_prefix"] == "U"),
        "receiver_primer_b_id",
    ].values[0]
    rec_primer_D_45 = receivers_df_merge.loc[
        (receivers_df_merge["BGC_number"] == "act")
        & (receivers_df_merge["crRNA_prefix"] == "D"),
        "receiver_primer_b_id",
    ].values[0]
    clc_rec_primer_U_48 = receivers_df_merge.loc[
        (receivers_df_merge["BGC_number"] == "act")
        & (receivers_df_merge["crRNA_prefix"] == "U"),
        "clc_receiver_b_id",
    ].values[0]
    clc_rec_primer_D_45 = receivers_df_merge.loc[
        (receivers_df_merge["BGC_number"] == "act")
        & (receivers_df_merge["crRNA_prefix"] == "D"),
        "clc_receiver_b_id",
    ].values[0]

    scr_primer_f = screening_df_merge.loc[
        (screening_df_merge["BGC_number"] == "act")
        & (screening_df_merge["sufix"] == "F"),
        "primer_b_id",
    ].values[0]
    scr_primer_r = screening_df_merge.loc[
        (screening_df_merge["BGC_number"] == "act")
        & (screening_df_merge["sufix"] == "R"),
        "primer_b_id",
    ].values[0]

    well = "G12"  # mapping_df.at[i,'96_well_formatted']

    clc_bac_rows.append(
        {
            "bac_name": bac_name,
            "BGC_number": "act",
            "grna_U": grna_U,
            "grna_D": grna_D,
            "strain_name": strain,
            "dna_fragment": dna_fragment,
            "rec_primer_U_48": rec_primer_U_48,
            "rec_primer_D_45": rec_primer_D_45,
            "clc_rec_primer_U_48": clc_rec_primer_U_48,
            "clc_rec_primer_D_45": clc_rec_primer_D_45,
            "scr_primer_f": scr_primer_f,
            "scr_primer_r": scr_primer_r,
            "well_96": well,
        }
    )

    clc_bac_df = pd.DataFrame(clc_bac_rows)
    if TESTING:

        def build_entity(row):
            return DnaSequenceBulkCreate(
                folder_id=folder_id,
                name=row["bac_name"],
                schema_id=schema_id,
                fields=fields(
                    {
                        "Well96": {"value": row["well_96"]},  # row[""]
                        "gRNA - Up": {"value": row["grna_U"]},
                        "gRNA - Down": {"value": row["grna_D"]},
                        "Receiver Primer pBE45": {"value": row["rec_primer_D_45"]},
                        "Receiver Primer pBE48": {"value": row["rec_primer_U_48"]},
                        "Receiver Assembly pBE45": {
                            "value": row["clc_rec_primer_D_45"]
                        },
                        "Receiver Assembly pBE48": {
                            "value": row["clc_rec_primer_U_48"]
                        },
                        "Screening Primer CF": {"value": row["scr_primer_f"]},
                        "Screening Primer CR": {"value": row["scr_primer_r"]},
                        "Strain": {"value": row["strain_name"]},
                    }
                ),
                # **clc_bac_clc_receivers_kwargs(),
                bases="",  # not always necessary
                is_circular=False,  # also might be unnecessary later
                **registry_kwargs(registry),
            )

        return bulk_register_entities(
            app=app,
            entity_type="dna",
            df=clc_bac_df,
            name_column="bac_name",
            output_column="bac_b_id",
            folder_id=folder_id,
            schema_id=schema_id,
            entity_builder_fn=build_entity,
        )

    else:

        def build_entity(row):
            return CustomEntityBulkCreate(
                folder_id=folder_id,
                name=row["bac_name"],
                schema_id=schema_id,
                fields=fields(
                    {
                        "Well96": {"value": row["well_96"]},  # row[""]
                        "gRNA - Up": {"value": row["grna_U"]},
                        "gRNA - Down": {"value": row["grna_D"]},
                        "Receiver Primer pBE45": {"value": row["rec_primer_D_45"]},
                        "Receiver Primer pBE48": {"value": row["rec_primer_U_48"]},
                        "Receiver Assembly pBE45": {
                            "value": row["clc_rec_primer_D_45"]
                        },
                        "Receiver Assembly pBE48": {
                            "value": row["clc_rec_primer_U_48"]
                        },
                        "Screening Primer CF": {"value": row["scr_primer_f"]},
                        "Screening Primer CR": {"value": row["scr_primer_r"]},
                        "Strain": {"value": row["strain_name"]},
                    }
                ),
                **registry_kwargs(registry),
            )

        return bulk_register_entities(
            app=app,
            entity_type="custom",
            df=clc_bac_df,
            name_column="bac_name",
            output_column="bac_b_id",
            folder_id=folder_id,
            schema_id=schema_id,
            entity_builder_fn=build_entity,
        )


def create_and_register_entities(
    app: App,
    crrna_df_merge,
    receivers_df_merge,
    screening_df_merge,
    genomes_df,
    mapping_df,
):

    config = app.config_store.config_by_path

    crrna_df_merge = register_crrna(
        app=app,
        df=crrna_df_merge,
        folder_id=config(["crRNA storage folder"]).required().value_str(),
        schema_id=config(["gRNA schema"]).required().value_str(),
        registry=config(["Registry schema"]).required().value_str(),
    )

    receivers_df_merge = register_receivers(
        app=app,
        df=receivers_df_merge,
        folder_id=config(["Primers storage folder"]).required().value_str(),
        schema_id=config(["Primer schema"]).required().value_str(),
        registry=config(["Registry schema"]).required().value_str(),
    )

    receivers_df_merge = register_clc_receivers(
        app=app,
        df=receivers_df_merge,
        folder_id=config(["Receivers assemblies storage folder"])
        .required()
        .value_str(),
        schema_id=config(["CLC Receiver schema"]).required().value_str(),
        registry=config(["Registry schema"]).required().value_str(),
    )

    screening_df_merge = register_screening(
        app=app,
        df=screening_df_merge,
        folder_id=config(["Primers storage folder"]).required().value_str(),
        schema_id=config(["Primer schema"]).required().value_str(),
        registry=config(["Registry schema"]).required().value_str(),
    )

    genome_df, error_genomes_name = find_genomes(
        app=app,
        df=genomes_df,
        folder_clc_id=config(["CLC strains storage folder"])
        .required()
        .value_str(),  # strains_folder
        folder_nbc_id=config(["NBC strains storage folder"])
        .required()
        .value_str(),  # nbc_strains_folder
        schema_id=config(["Strain schema"]).required().value_str(),
    )
    if error_genomes_name:
        return (
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            error_genomes_name,
        )

    genome_df = register_dna_fragments(
        app=app,
        df=genome_df,
        folder_id=config(["DNA fragments storage folder"])
        .required()
        .value_str(),  # genomes_folder
        schema_id=config(["DNA fragment schema"]).required().value_str(),
        registry=config(["Registry schema"]).required().value_str(),
    )

    min_clust = pd.to_numeric(crrna_df_merge["BGC_number"], errors="coerce").min()
    max_clust = pd.to_numeric(crrna_df_merge["BGC_number"], errors="coerce").max()

    crrna_df_merge["BGC_number"] = crrna_df_merge["BGC_number"].astype(str)
    receivers_df_merge["BGC_number"] = receivers_df_merge["BGC_number"].astype(str)
    screening_df_merge["BGC_number"] = screening_df_merge["BGC_number"].astype(str)

    clc_bac_df = register_clc_bac(
        app=app,
        crrna_df_merge=crrna_df_merge,
        receivers_df_merge=receivers_df_merge,
        screening_df_merge=screening_df_merge,
        genome_df=genome_df,
        mapping_df=mapping_df,
        min_clust=min_clust,
        max_clust=max_clust,
        folder_id=config(["BACs storage folder"])
        .required()
        .value_str(),  # b_api_ids.my_folder_id,  # genomes_folder
        schema_id=config(["CLC BAC schema"])
        .required()
        .value_str(),  # b_api_ids.clc_bac_schema_id,
        registry=config(["Registry schema"]).required().value_str(),
    )

    return clc_bac_df, crrna_df_merge, receivers_df_merge, screening_df_merge, None
