%md
# TRX classification extract — by raw FSA

Same output as the `ter_onlvl_ibc_no` version (ADIDO 3868, `TRX_<var>`): earned on-level
premium, earned exposure, claim counts and capped claim amounts summed by
jurisdiction x company x accident year x variable. The only analytical change is the
grouping variable: `ter_onlvl_ibc_no` -> raw FSA.

**Read the source-table note in the README before running.** The table name in the original
code does not match any table the TRX pipeline writes; this notebook resolves it instead of
guessing.
from pyspark.sql import functions as F
from functools import reduce
as_of_date_suffix = dbutils.widgets.get("as_of_date_suffix")
province = dbutils.widgets.get("province")
%run ./00_fsa_common
# ---------------------------------------------------------------------
# Grouping variable
# ---------------------------------------------------------------------
# WAS: VARS = ["ter_onlvl_ibc_no"]
VARS = [FSA_VAR]

#################### Interaction Variables ###############
# dri_type_cd_x_number_au_mh_x_veh_dri_onp_nb
# dri_type_cd_x_rat_km_work_nb
# dri_type_cd_x_rat_km_business_nb
# dri_type_cd_x_rat_km_annual_nb
# "ter_onlvl_ibc_no"

# ---------------------------------------------------------------------
# Columns used in filters + aggregations (kept explicit for reliability)
# ---------------------------------------------------------------------
premium_cols = [
    "Prm_Ern_BI_OnLvl_End_Am",
    "Prm_Ern_PD_OnLvl_End_Am",
    "Prm_Ern_DC_OnLvl_End_Am",
    "Prm_Ern_AB_OnLvl_End_Am",
    "Prm_Ern_AP_COL_OnLvl_End_Am",
    "Prm_Ern_AP_SP_CMP_OnLvl_End_Am"
]

expo_cols = [
    "Xpo_Ern_BI_Nb",
    "Xpo_Ern_PD_Nb",
    "Xpo_Ern_DC_Nb",
    "Xpo_Ern_AB_Nb",
    "Xpo_Ern_AP_COL_Nb",
    "Xpo_Ern_AP_SP_CMP_Nb"
]

clm_nb_cols = [
    "clm_chap_tpl_nb",
    "clm_chap_bi_nb",
    "clm_chap_dc_nb",
    "clm_chap_pd_nb",
    "clm_chap_ab_nb",
    "clm_chap_col_nb",
    "clm_chap_cmp_nb"
]

clm_am_cols = [
    "clm_chap_tpl_cap500k_am",
    "clm_chap_bi_cap500k_am",
    "clm_chap_pd_cap500k_am",
    "clm_chap_dc_am",
    "clm_chap_ab_cap500k_am",
    "clm_chap_col_am",
    "clm_chap_cmp_am"
]

base_group_cols = ["pol_jurisdiction_cd", "pol_uwcompany_cd", "pol_acc_yr_dt"]
measure_cols = premium_cols + expo_cols + clm_nb_cols + clm_am_cols
%md
## Read data

The original code read `trx_{prov}_ppa_prep_onlvl_{suffix}`. The pipeline writes
`trx_{prov}_ppa_prep_{suffix}` (prepped), `trx_{prov}_ppa_prep_{suffix}_onlvl` (scored) and
`trx_ap_ppa_prep_onlvl_{suffix}` (scored + endorsement split + earned premium). Only the
last one carries `Prm_Ern_*_OnLvl_End_Am` and `Xpo_Ern_*_Nb`, because the earning step in
`06_ap_ppa_trx_pipeline.py` runs after the four provinces are unioned back together.

`resolve_table` tries each name and takes the first that exists AND has every column this
notebook needs, so it is correct whichever table your workspace actually holds.
required_cols = base_group_cols + measure_cols + ["dri_type_cd", FSA_SOURCE_COL]

df_trx, trx_table = resolve_table(
    candidates = [
        f"t_ap_ppa_pricing.trx_{province.lower()}_ppa_prep_onlvl_{as_of_date_suffix}",
        f"t_ap_ppa_pricing.trx_{province.lower()}_ppa_prep_{as_of_date_suffix}_onlvl",
        f"t_ap_ppa_pricing.trx_ap_ppa_prep_onlvl_{as_of_date_suffix}",
    ],
    required_cols = required_cols,
    context = "TRX",
)

# The AP-wide table holds all four provinces. The original read a province-specific name,
# so filter to keep the row set identical either way. pol_jurisdiction_cd stays in the
# group-by as a provenance check: more than one value in the output means this filter did
# not do what you think.
df_trx = df_trx.filter(F.col("pol_jurisdiction_cd") == F.lit(province.upper()))
%run /Workspace/Shared/t_ap_ppa_pricing/Functions/AP_PPA_Classification
# ---------------------------------------------------------------------
# Build filter condition: dri_type_cd != "OccasionalPrincipal"
# and all premium cols not null
# ---------------------------------------------------------------------
not_null_all_premiums = reduce(
    lambda acc, c: acc & F.col(c).isNotNull(),
    premium_cols,
    F.lit(True)
)

# ---------------------------------------------------------------------
# Prep data (filter, mutate, join, coalesce)
# ---------------------------------------------------------------------


df_trx_prep = (df_trx
    .filter(
        (F.col("dri_type_cd") != F.lit("OccasionalPrincipal")) &
        not_null_all_premiums
    )
    .withColumn(
        "pol_uwcompany_cd",
        F.when(F.col("pol_uwcompany_cd") == F.lit("snic"), F.lit("SN"))
         .when(F.col("pol_uwcompany_cd") == F.lit("prim"), F.lit("PIC"))
         .otherwise(F.lit("TDHA"))
    )
)

df_trx_prep = ClassificationPrep(df_trx_prep)

# ClassificationPrep does not touch the FSA columns, so the derivation goes after it and
# the banding of every other variable is unchanged.
df_trx_prep = add_fsa(df_trx_prep)

require_columns(df_trx_prep, base_group_cols + measure_cols + VARS, "TRX post-prep")
fsa_coverage_check(df_trx_prep, "TRX")
%run /Workspace/Shared/t_ap_ppa_pricing/Functions/Utils
# ---------------------------------------------------------------------
# Loop over VARS (your R code has only one var, but keeping loop)
# ---------------------------------------------------------------------
for v in VARS:
    group_cols = base_group_cols + [v]

    # select only required columns (like dplyr::select)
    df_sel = df_trx_prep.select(*(group_cols + measure_cols))

    # sum everything except group cols (like summarise(across(everything(), sum)))
    # F.sum ignores nulls, matching R's na.rm = TRUE.
    agg_exprs = [F.sum(F.col(c)).alias(c) for c in measure_cols]

    df_out = (df_sel
        .groupBy(*group_cols)
        .agg(*agg_exprs)
    )
    df_out.cache()

    adido_out(table = df_out, ticket = 3868, filename = 'Trx_on_ppa_Ay_onlvl_wRels', freeForm = f"TRX_{v}", fileformat='parquet', folder_out = f't_ap_ppa_pricing/data/{province.lower()}/classification/')

thin_cell_report(df_out, "Xpo_Ern_BI_Nb", "TRX", floor = 100)
df_out.display()
%md
## Reconciliation

Same rows, different key — so every measure must tie to the `ter_onlvl_ibc_no` run at the
original grain. A break means rows were lost, most likely to a null FSA. Run this before
using the exhibit.
recon = (df_out
    .groupBy(*base_group_cols)
    .agg(*[F.sum(F.col(c)).alias(c) for c in measure_cols]))
recon.display()
# Guard: one province in, one province out.
provs = [r["pol_jurisdiction_cd"] for r in df_out.select("pol_jurisdiction_cd").distinct().collect()]
assert provs == [province.upper()], f"expected only {province.upper()}, got {provs}"
print(f"OK - source table {trx_table}, single jurisdiction {provs[0]}")
