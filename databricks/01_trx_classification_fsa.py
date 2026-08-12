%md
# TRX classification extract — by raw FSA

Same output as the `ter_onlvl_ibc_no` version (ADIDO 3868, `TRX_<var>`): earned on-level
premium, earned exposure, claim counts and capped claim amounts summed by
jurisdiction x company x accident year x variable. The only change is the grouping
variable: `ter_onlvl_ibc_no` -> raw FSA.
from pyspark.sql import functions as F
from functools import reduce
as_of_date_suffix = dbutils.widgets.get("as_of_date_suffix")
province = dbutils.widgets.get("province")
# ---------------------------------------------------------------------
# Read data
# ---------------------------------------------------------------------
df_trx = spark.sql(f"""
        select *
        from t_ap_ppa_pricing.trx_{province.lower()}_ppa_prep_onlvl_{as_of_date_suffix}
        """)
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
fsa_coverage_check(df_trx_prep, "TRX")
%run /Workspace/Shared/t_ap_ppa_pricing/Functions/Utils
# ---------------------------------------------------------------------
# Loop over VARS (your R code has only one var, but keeping loop)
# ---------------------------------------------------------------------
base_group_cols = ["pol_jurisdiction_cd", "pol_uwcompany_cd", "pol_acc_yr_dt"]

measure_cols = premium_cols + expo_cols + clm_nb_cols + clm_am_cols

for v in VARS:
    group_cols = base_group_cols + [v]

    # select only required columns (like dplyr::select)
    df_sel = df_trx_prep.select(*(group_cols + measure_cols))

    # sum everything except group cols (like summarise(across(everything(), sum)))
    agg_exprs = [F.sum(F.col(c)).alias(c) for c in measure_cols]

    df_out = (df_sel
        .groupBy(*group_cols)
        .agg(*agg_exprs)
    )
    adido_out(table = df_out, ticket = 3868, filename = 'Trx_on_ppa_Ay_onlvl_wRels', freeForm = f"TRX_{v}", fileformat='parquet', folder_out = f't_ap_ppa_pricing/data/{province.lower()}/classification/')

df_out.display()
%md
## Reconciliation

The FSA cut must tie to the territory cut on every measure — same filters, same rows, only
the grouping key differs. Run this against the `ter_onlvl_ibc_no` output before using the
exhibit; a break means rows were dropped by a null FSA, not a real difference.
# Totals should match the ter_onlvl_ibc_no run cell-for-cell at the jurisdiction/company/AY level.
recon = (df_out
    .groupBy(*base_group_cols)
    .agg(*[F.sum(F.col(c)).alias(c) for c in measure_cols]))
recon.display()
